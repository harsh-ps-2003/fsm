package fsm

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"
	"github.com/superfly/fsm/gen/fsm/v1/fsmv1connect"

	"github.com/hashicorp/go-memdb"
	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const (
	fsmTable          = "fsm"
	idIndex           = "id"
	runIndex          = "run"
	runPrefixIndex    = runIndex + "_prefix"
	parentIndex       = "parent"
	parentPrefixIndex = parentIndex + "_prefix"

	tracerName = "fsm"
)

var (
	fsmSchema = &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			fsmTable: {
				Name: fsmTable,
				Indexes: map[string]*memdb.IndexSchema{
					idIndex: {
						Name:   idIndex,
						Unique: true,
						Indexer: ulidIndexer{
							fieldFn: func(rs runState) ulid.ULID {
								return rs.StartVersion
							},
						},
					},
					runIndex: {
						Name:    runIndex,
						Unique:  false,
						Indexer: &memdb.StringFieldIndex{Field: "ID"},
					},
					parentIndex: {
						Name:         parentIndex,
						AllowMissing: true,
						Unique:       false,
						Indexer: ulidIndexer{
							fieldFn: func(rs runState) ulid.ULID {
								return rs.Parent
							},
						},
					},
				},
			},
		},
	}
)

type Manager struct {
	logger logrus.FieldLogger

	tracer trace.Tracer

	wg sync.WaitGroup

	db *memdb.MemDB

	store *store

	fsms map[fsmKey]*fsm

	queues map[string]*queuedRunner

	done chan struct{}

	mu      sync.RWMutex
	running map[ulid.ULID]context.CancelCauseFunc
}

type fsmKey struct {
	name string

	action string
}

type Config struct {
	Logger logrus.FieldLogger

	// DBPath is the directory to use for persisting FSM state.
	DBPath string

	// Qeues defines which queues are available for FSMs to use. The key is the queue name and the
	// value is the maximum number of FSMs that can run concurrently.
	Queues map[string]int
}

// New creates a new FSM manager to register and run FSMs.
func New(cfg Config) (*Manager, error) {
	memDB, err := memdb.NewMemDB(fsmSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create memdb, %w", err)
	}

	if cfg.Logger == nil {
		cfg.Logger = logrus.New()
	}

	if cfg.DBPath == "" {
		return nil, errors.New("db path is required")
	}

	if err := os.MkdirAll(cfg.DBPath, 0600); err != nil {
		return nil, fmt.Errorf("failed to setup DB path: %w", err)
	}

	tracer := otel.GetTracerProvider().Tracer(tracerName,
		trace.WithInstrumentationVersion("0.1.0"),
		trace.WithSchemaURL(semconv.SchemaURL),
	)

	store, err := newStore(cfg.Logger.WithField("sys", "fsm-store"), tracer, cfg.DBPath, memDB)
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})

	man := &Manager{
		logger:  cfg.Logger.WithField("sys", "fsm"),
		tracer:  tracer,
		store:   store,
		db:      memDB,
		fsms:    map[fsmKey]*fsm{},
		queues:  make(map[string]*queuedRunner, len(cfg.Queues)),
		done:    done,
		running: map[ulid.ULID]context.CancelCauseFunc{},
	}

	for name, size := range cfg.Queues {
		q := &queuedRunner{
			name:   name,
			size:   size,
			queue:  make(chan queueItem),
			queued: make([]func(), 0, size),
		}
		man.queues[name] = q
		go q.run(done, cfg.Logger.WithField("queue", name))
	}

	mux := http.NewServeMux()
	mux.Handle(fsmv1connect.NewFSMServiceHandler(&adminServer{
		m: man,
	}))

	server := &http.Server{
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	socket := filepath.Join(cfg.DBPath, "fsm.sock")
	os.Remove(socket)
	unixListener, err := net.Listen("unix", socket)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on unix socket %s, %w", socket, err)
	}

	go server.Serve(unixListener)

	go func() {
		defer os.Remove(socket)
		<-man.done
		if err := unixListener.Close(); err != nil {
			man.logger.WithError(err).Error("failed to close unix listener")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			man.logger.WithError(err).Error("failed to shutdown http server")
		}
	}()

	return man, nil
}

// Shutdown sends a stop signal to all FSMs and blocks until they have all stopped.
func (m *Manager) Shutdown(timeout time.Duration) {
	m.logger.WithField("shutdown_timeout", timeout).Info("shutting down")

	m.mu.RLock()
	for id, cancel := range m.running {
		m.logger.WithField("fsm_id", id.String()).Info("shutting down fsm")
		cancel(nil)
	}
	m.mu.RUnlock()

	close(m.done)

	wait := make(chan struct{})
	go func() {
		defer close(wait)
		m.wg.Wait()
	}()

	select {
	case <-wait:
		m.logger.Info("all FSMs have shutdown")
	case <-time.After(timeout):
		m.logger.Warn("timed out waiting for FSMs to shutdown")
	}

	if err := m.store.Close(); err != nil {
		m.logger.WithError(err).Error("failed to close store")
	}

	m.logger.Info("shutdown complete")
}

type ActiveKey struct {
	Action  string
	Version ulid.ULID
}

type ActiveSet map[ActiveKey]fsmv1.RunState

// Active returns a map of active runs for the given id. The map keys are the run type and the
// values are the run version which can be used to wait for the run to complete.
func (m *Manager) Active(ctx context.Context, id string) (ActiveSet, error) {
	txn := m.db.Txn(false)
	defer txn.Abort()

	active := map[ActiveKey]fsmv1.RunState{}

	it, err := txn.Get(fsmTable, runPrefixIndex, id)
	if err != nil {
		return nil, err
	}

	for next := it.Next(); next != nil; next = it.Next() {
		rs := next.(runState)
		if rs.State == fsmv1.RunState_RUN_STATE_COMPLETE {
			continue
		}
		active[ActiveKey{Action: rs.Action, Version: rs.StartVersion}] = rs.State
	}

	return active, nil
}

// Children returns a list of FSMs that are associated with the given parent.
func (m *Manager) Children(ctx context.Context, parent ulid.ULID) ([]ulid.ULID, error) {
	return m.store.Children(ctx, parent)
}

// ActiveChildren returns a list of FSMs that were started from the given parent and are still
// active.
func (m *Manager) ActiveChildren(ctx context.Context, parent ulid.ULID) ([]Run, error) {
	txn := m.db.Txn(false)
	defer txn.Abort()

	it, err := txn.Get(fsmTable, parentPrefixIndex, parent)
	if err != nil {
		return nil, err
	}

	children := []Run{}
	for next := it.Next(); next != nil; next = it.Next() {
		rs := next.(runState)
		if rs.StartVersion.Compare(ulid.ULID{}) == 0 {
			continue
		}
		children = append(children, rs.Run)
	}

	return children, nil
}

// Cancel sends a cancel signal to the FSM should it exist. It does not block until the FSM has
// completed so callers should use Wait to ensure the FSM has stopped, if needed.
func (m *Manager) Cancel(ctx context.Context, version ulid.ULID, cause string) error {
	m.mu.RLock()
	f, ok := m.running[version]
	m.mu.RUnlock()
	if !ok {
		return ErrFsmNotFound
	}

	f(errors.New(cause))
	return nil
}

// Wait blocks until the run with the given version completes.
func (m *Manager) Wait(ctx context.Context, version ulid.ULID) error {
	var (
		v      = version.String()
		logger = m.logger.WithField("start_version", v)
	)

	logger.Info("waiting for FSM to finish")
	defer logger.Info("done waiting for FSM to finish")
	for {
		txn := m.db.Txn(false)
		ws := memdb.NewWatchSet()
		defer txn.Abort()

		ch, item, err := txn.FirstWatch(fsmTable, idIndex, v)
		switch {
		case err != nil:
			logger.WithError(err).Error("failed to wait for FSM")
			return err
		case item == nil:
			// Lookup from the store in case the FSM has already completed.
			he, err := m.store.History(ctx, version)
			switch {
			case errors.Is(err, ErrFsmNotFound):
				return nil
			case err != nil:
				return err
			case he.GetLastEvent().GetError() != "":
				return &haltError{err: errors.New(he.GetLastEvent().GetError())}
			default:
				return nil
			}
		}

		state, ok := item.(runState)
		switch {
		case !ok:
			return fmt.Errorf("unexpected type %T", item)
		case state.State == fsmv1.RunState_RUN_STATE_COMPLETE:
			return state.Error.Err
		default:
			ws.Add(ch)
		}

		err = ws.WatchCtx(ctx)
		switch {
		case errors.Is(err, context.Canceled):
			return err
		case err != nil:
			logger.WithError(err).Error("failed to wait for FSM")
			return err
		}

		roTxn := m.db.Txn(false)
		defer roTxn.Abort()

		item, err = roTxn.First(fsmTable, idIndex, v)
		switch {
		case err != nil:
			return err
		case item == nil:
			// Lookup from the store in case the FSM has already completed.
			he, err := m.store.History(ctx, version)
			switch {
			case errors.Is(err, ErrFsmNotFound):
				return nil
			case err != nil:
				return err
			case he.GetLastEvent().GetError() != "":
				return &haltError{err: errors.New(he.GetLastEvent().GetError())}
			default:
				return nil
			}
		default:
			state, ok := item.(runState)
			switch {
			case !ok:
				return fmt.Errorf("unexpected type %T", item)
			case state.State == fsmv1.RunState_RUN_STATE_PENDING, state.State == fsmv1.RunState_RUN_STATE_RUNNING:
				logger.Info("FSM still running")
			case state.State == fsmv1.RunState_RUN_STATE_COMPLETE:
				return state.Error.Err
			}
		}
	}
}

// WaitByID blocks until the run with the given ID completes.
func (m *Manager) WaitByID(ctx context.Context, id string) error {
	var (
		logger = m.logger.WithField("fsm_run_id", id)
	)

	logger.Info("waiting for FSM to finish")
	defer logger.Info("done waiting for FSM to finish")
	for {
		txn := m.db.Txn(false)
		defer txn.Abort()

		var version ulid.ULID
		itemByID, err := txn.First(fsmTable, runIndex, id)
		switch {
		case err != nil:
		case itemByID == nil:
		default:
			rs := itemByID.(runState)
			if rs.State == fsmv1.RunState_RUN_STATE_COMPLETE {
				return rs.Error.Err
			}
			version = rs.StartVersion
			logger = logger.WithField("start_version", version.String())
		}

		ch, item, err := txn.FirstWatch(fsmTable, idIndex, version.String())
		switch {
		case err != nil:
			logger.WithError(err).Error("failed to wait for FSM")
			return err
		case item == nil:
			// Lookup from the store in case the FSM has already completed.
			he, err := m.store.History(ctx, version)
			switch {
			case errors.Is(err, ErrFsmNotFound):
				return nil
			case err != nil:
				return err
			case he.GetLastEvent().GetError() != "":
				return &haltError{err: errors.New(he.GetLastEvent().GetError())}
			default:
				return nil
			}
		}

		ws := memdb.NewWatchSet()
		state, ok := item.(runState)
		switch {
		case !ok:
			return fmt.Errorf("unexpected type %T", item)
		case state.State == fsmv1.RunState_RUN_STATE_COMPLETE:
			return state.Error.Err
		default:
			ws.Add(ch)
		}

		err = ws.WatchCtx(ctx)
		switch {
		case errors.Is(err, context.Canceled):
			return err
		case err != nil:
			logger.WithError(err).Error("failed to wait for FSM")
			return err
		}

		roTxn := m.db.Txn(false)
		defer roTxn.Abort()

		item, err = roTxn.First(fsmTable, idIndex, version.String())
		switch {
		case err != nil:
			return err
		case item == nil:
			// Lookup from the store in case the FSM has already completed.
			he, err := m.store.History(ctx, version)
			switch {
			case errors.Is(err, ErrFsmNotFound):
				return nil
			case err != nil:
				return err
			case he.GetLastEvent().GetError() != "":
				return &haltError{err: errors.New(he.GetLastEvent().GetError())}
			default:
				return nil
			}
		default:
			state, ok := item.(runState)
			switch {
			case !ok:
				return fmt.Errorf("unexpected type %T", item)
			case state.State == fsmv1.RunState_RUN_STATE_PENDING, state.State == fsmv1.RunState_RUN_STATE_RUNNING:
				logger.Info("FSM still running")
			case state.State == fsmv1.RunState_RUN_STATE_COMPLETE:
				return state.Error.Err
			}
		}
	}
}
