// Package fsm contains runner implementations for scheduling FSM execution.
// Runners control when and how FSMs execute (immediately, delayed, queued, etc.).
package fsm

import (
	"context"
	"errors"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
)

// runner is an interface for executing FSM functions with different scheduling strategies.
// Implementations include:
//   - defaultRunner: Execute immediately
//   - delayedRunner: Execute after a delay
//   - queuedRunner: Execute when queue capacity is available
//   - runAfter: Execute after another FSM completes
type runner interface {
	// Run schedules the execution of fn. It closes ack when the FSM has been
	// accepted for execution (not necessarily started). This allows callers
	// to know the FSM is scheduled without blocking.
	Run(ctx context.Context, logger logrus.FieldLogger, ack chan struct{}, fn func())
}

type runnerFn func(ctx context.Context, logger logrus.FieldLogger, fn func())

func (r runnerFn) Run(ctx context.Context, logger logrus.FieldLogger, ack chan struct{}, fn func()) {
	close(ack)
	r(ctx, logger, fn)
}

// runnerFromOpts selects the appropriate runner based on start options.
// It checks options in order of precedence:
//   1. Delayed start (until time)
//   2. Run after dependency (runAfter ULID)
//   3. Queue assignment
//   4. Default (immediate execution)
func runnerFromOpts(opts *startOptions, m *Manager) runner {
	switch {
	case !opts.until.IsZero():
		return delayedRunner(opts.until)
	case opts.runAfter.Compare(ulid.ULID{}) != 0:
		return runAfter(m, opts.runAfter)
	case opts.queue != "":
		q, ok := m.queues[opts.queue]
		if !ok {
			m.logger.WithField("queue", opts.queue).Warn("queue not found, using default runner")
			return defaultRunner()
		}
		return q
	default:
		return defaultRunner()
	}
}

func defaultRunner() runner {
	return runnerFn(func(ctx context.Context, logger logrus.FieldLogger, fn func()) {
		fn()
	})
}

func delayedRunner(delayUntil time.Time) runner {
	return runnerFn(func(ctx context.Context, logger logrus.FieldLogger, fn func()) {
		delay := delayUntil.Sub(time.Now())
		logger.WithField("delay", delay).Info("delaying start")
		t := time.NewTimer(delay)
		select {
		case <-t.C:
			fn()
		case <-ctx.Done():
			t.Stop()
		}
	})
}

type waiter interface {
	Wait(context.Context, ulid.ULID) error
}

func runAfter(w waiter, after ulid.ULID) runner {
	return runnerFn(func(ctx context.Context, logger logrus.FieldLogger, fn func()) {
		err := w.Wait(ctx, after)
		switch {
		case errors.Is(err, context.Canceled):
			logger.Info("context canceled, fsm shutting down")
			return
		case errors.Is(err, ErrFsmNotFound):
			logger.WithField("run_after_version", after.String()).Warn("FSM not found, immediately starting")
		case err != nil:
			logger.WithError(err).Error("failed to wait for FSM to complete, immediately starting")
		}
		fn()
	})
}

// queuedRunner implements rate limiting by limiting concurrent FSM executions.
// When the queue is at capacity, new FSMs wait until capacity is available.
type queuedRunner struct {
	// name is the queue name (for logging).
	name string

	// inflight is the current number of FSMs executing.
	// size is the maximum number of concurrent executions.
	inflight, size int

	// queue is the channel for submitting FSM functions to execute.
	queue chan queueItem

	// queued is a slice of FSM functions waiting for capacity.
	queued []func()
}

type queueItem struct {
	fn func()

	ack chan struct{}
}

func (r *queuedRunner) withFields() logrus.Fields {
	return logrus.Fields{
		"inflight": r.inflight,
		"queued":   len(r.queued),
	}
}

func (r *queuedRunner) Run(ctx context.Context, logger logrus.FieldLogger, ack chan struct{}, fn func()) {
	item := queueItem{
		fn: func() {
			logger.Info("running queued function")
			fn()
		},
		ack: ack,
	}
	r.queue <- item
	<-item.ack
}

func (r *queuedRunner) run(quit <-chan struct{}, logger logrus.FieldLogger) {
	logger = logger.WithFields(logrus.Fields{"queue": r.name, "size": r.size})
	logger.Info("started")

	done := make(chan struct{}, r.size)
	for {
		select {
		case <-quit:
			logger.Info("exiting")
			return
		case <-done:
			r.inflight--
			logger.WithFields(r.withFields()).Info("done")
			switch len(r.queued) {
			case 0:
				continue
			default:
				f := r.queued[0]
				r.queued = r.queued[1:]
				r.inflight++
				logger.WithFields(r.withFields()).Info("executing")
				go func() {
					f()
					done <- struct{}{}
				}()
			}
		case item := <-r.queue:
			switch {
			case r.inflight >= r.size:
				r.queued = append(r.queued, item.fn)
				logger.WithFields(r.withFields()).Info("queued")
			default:
				r.inflight++
				logger.WithFields(r.withFields()).Info("executing")
				go func() {
					item.fn()
					done <- struct{}{}
				}()
			}
			close(item.ack)
		}
	}
}
