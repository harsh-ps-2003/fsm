// Package fsm contains the Manager, which is the main entry point for using the FSM library.
// The Manager handles FSM registration, execution, lifecycle management, and provides
// administrative APIs for monitoring and controlling FSMs.
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
	// Table and index names for the in-memory database.
	fsmTable          = "fsm"
	idIndex           = "id"           // Index by StartVersion (ULID)
	runIndex          = "run"          // Index by Run.ID (string)
	runPrefixIndex    = runIndex + "_prefix"
	parentIndex       = "parent"       // Index by Parent ULID
	parentPrefixIndex = parentIndex + "_prefix"

	tracerName = "fsm" // Name for OpenTelemetry tracer
)

var (
	// fsmSchema defines the schema for the in-memory database (memdb).
	// This provides fast lookups for active FSMs and supports watching for state changes.
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

// Manager is the central coordinator for all FSM operations. It:
//   - Registers FSM definitions
//   - Starts and manages FSM executions
//   - Provides APIs for querying active FSMs
//   - Handles graceful shutdown
//   - Exposes an admin API over HTTP/2 (via Unix socket)
//
// A single Manager instance should be created at application startup and reused
// throughout the application lifetime.
type Manager struct {
	// logger is used for structured logging of Manager operations.
	logger logrus.FieldLogger

	// tracer is used for distributed tracing of FSM operations.
	tracer trace.Tracer

	// wg tracks running FSM goroutines for graceful shutdown.
	wg sync.WaitGroup

	// db is the in-memory database for fast FSM state lookups.
	// It mirrors some state from the persistent store for performance.
	db *memdb.MemDB

	// store is the persistent storage layer for FSM state.
	store *store

	// fsms maps FSM keys (typeName + action) to their registered definitions.
	fsms map[fsmKey]*fsm

	// queues maps queue names to their queue runners for rate limiting.
	queues map[string]*queuedRunner

	// done is closed when the Manager is shutting down.
	done chan struct{}

	// mu protects the running map from concurrent access.
	mu      sync.RWMutex
	
	// running maps FSM StartVersions to their cancellation functions.
	// This allows cancelling FSMs externally via Manager.Cancel().
	running map[ulid.ULID]context.CancelCauseFunc
}

// fsmKey uniquely identifies an FSM definition by type name and action.
type fsmKey struct {
	// name is the request type name (e.g., "DeployRequest").
	name string

	// action is the action name (e.g., "deploy").
	action string
}

// Config configures a new Manager instance.
type Config struct {
	// Logger is the logger to use. If nil, a new logrus.Logger is created.
	Logger logrus.FieldLogger

	// DBPath is the directory path where FSM state will be persisted.
	// This directory will be created if it doesn't exist.
	DBPath string

	// Queues defines named queues for rate limiting. The map key is the queue name,
	// and the value is the maximum number of FSMs that can run concurrently in that queue.
	// FSMs assigned to a queue will wait if the queue is at capacity.
	Queues map[string]int
}

// New creates a new Manager instance with the provided configuration.
// It:
//   1. Creates the in-memory database
//   2. Initializes the persistent store
//   3. Starts queue runners (if any queues are configured)
//   4. Starts the admin HTTP server (listening on a Unix socket)
//
// The Manager is ready to use after this function returns successfully.
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

// Shutdown gracefully shuts down the Manager by:
//   1. Cancelling all running FSMs
//   2. Waiting for all FSMs to finish (up to timeout)
//   3. Closing the persistent store
//
// This should be called during application shutdown to ensure clean termination.
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

// ActiveKey uniquely identifies an active FSM by action and version.
type ActiveKey struct {
	// Action is the action name (e.g., "deploy").
	Action string
	
	// Version is the StartVersion (ULID) of the FSM run.
	Version ulid.ULID
}

// ActiveSet maps ActiveKeys to their current RunState (pending/running/complete).
type ActiveSet map[ActiveKey]fsmv1.RunState

// Active returns all active FSM runs for the given resource ID.
// Active runs are those that are not yet complete. The returned map can be used
// to track which actions are currently running for a resource and wait for completion.
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

// Cancel sends a cancellation signal to the FSM with the given version.
// It does not block; the FSM will stop asynchronously. Use Wait() if you need
// to ensure the FSM has fully stopped before proceeding.
//
// The cause string is included in the cancellation error for debugging.
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

// Wait blocks until the FSM with the given version completes (successfully or with error).
// It uses the in-memory database with watch sets for efficient waiting without polling.
// If the FSM has already completed, it checks the history store.
//
// Returns nil on success, or an error if the FSM failed or was cancelled.
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

// WaitByID blocks until all FSMs with the given resource ID complete.
// This is useful when you want to wait for any action on a resource to finish,
// rather than waiting for a specific FSM version.
//
// Returns nil on success, or an error if any FSM failed or was cancelled.
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
