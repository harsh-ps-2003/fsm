// Package fsm provides a finite state machine (FSM) library for orchestrating
// multi-step workflows with persistence, retry logic, and observability.
//
// The FSM library allows you to define state machines that execute a series of
// transitions (steps) in order. Each transition can:
//   - Execute custom business logic
//   - Retry on failure with exponential backoff
//   - Persist state to disk for recovery
//   - Resume execution after restarts
//   - Support parent-child relationships
//   - Use queues for rate limiting
//   - Emit metrics and traces
//
// Key concepts:
//   - Action: A named workflow (e.g., "deploy", "migrate")
//   - State: A named step in the workflow (e.g., "fetch-image", "unpack-image")
//   - Transition: The function that executes when entering a state
//   - Run: An instance of an FSM execution with a unique version (ULID)
//   - Request/Response: Strongly-typed input/output for each transition
//
// Example usage:
//
//	type DeployRequest struct { Image string }
//	type DeployResponse struct { ContainerID string }
//
//	manager := fsm.New(fsm.Config{DBPath: "/tmp/fsm"})
//	start, resume, _ := fsm.Register[DeployRequest, DeployResponse](manager, "deploy").
//		Start("fetch", fetchImage).
//		To("unpack", unpackImage).
//		End("activate", activateContainer).
//		Build(context.Background())
//
//	version, _ := start(ctx, "deploy-123", &DeployRequest{Image: "nginx"})
package fsm

//go:generate rm -rf gen
//go:generate go run github.com/bufbuild/buf/cmd/buf@v1.28.1 generate

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/benbjohnson/immutable"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var (
	// actionCounterVec is a Prometheus counter that tracks the number of FSM action
	// completions. It's labeled by:
	//   - action: The action name (e.g., "deploy")
	//   - resource: The resource alias (e.g., "deploy_request")
	//   - status: The completion status ("ok", "abort", "unrecoverable", etc.)
	//   - kind: The error kind if applicable (empty for success)
	actionCounterVec = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "fsm_action_count",
			Help: "A count of action completions.",
		},
		[]string{"action", "resource", "status", "kind"},
	)

	// actionDurationVec is a Prometheus histogram that tracks the duration of FSM
	// actions in seconds. It uses the same labels as actionCounterVec and includes
	// buckets from 0.5s to 1200s (20 minutes) to capture both fast and slow operations.
	actionDurationVec = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "fsm_action_duration_seconds",
			Help:    "Time spent performing an action.",
			Buckets: []float64{.5, 1, 2.5, 5, 10, 30, 60, 150, 300, 600, 1200},
		},
		[]string{"action", "resource", "status", "kind"},
	)
)

// Request represents the input to a transition function. It contains:
//   - Msg: The typed request message (R) specific to this FSM
//   - W: The response wrapper that will be populated as transitions execute
//   - logger: A structured logger with FSM context (run ID, version, etc.)
//   - run: Metadata about the current FSM execution
//
// R is the request type (input) and W is the response type (output) for the FSM.
// These are strongly typed using Go generics to ensure type safety throughout
// the FSM execution.
type Request[R, W any] struct {
	// Msg is the typed request message passed to each transition function.
	// This is the primary input data for the FSM.
	Msg *R

	// W is the response wrapper that accumulates the output from transitions.
	// Each transition can update the response, which persists across transitions.
	W Response[W]

	// logger is a structured logger that includes FSM metadata (run ID, version,
	// current state, etc.) for observability.
	logger logrus.FieldLogger

	// run contains metadata about the current FSM execution, including:
	// - StartVersion: Unique ULID for this FSM run
	// - CurrentState: The name of the current transition being executed
	// - ID: The user-provided identifier for this resource
	// - Action: The action name this FSM is executing
	run Run
}

// Any returns the untyped request message. This is used internally to
// work with requests in a type-erased way (via AnyRequest interface).
func (r *Request[_, _]) Any() any {
	if r == nil {
		return nil
	}
	return r.Msg
}

// Log returns the structured logger for this request. The logger includes
// context about the FSM run (ID, version, current state, etc.).
func (r *Request[_, _]) Log() logrus.FieldLogger {
	return r.logger
}

// Run returns the metadata about the current FSM execution.
func (r *Request[_, _]) Run() Run {
	return r.run
}

// withLogger sets the logger for this request. This is called internally
// by the FSM executor to attach run-specific context.
func (r *Request[_, _]) withLogger(logger logrus.FieldLogger) {
	r.logger = logger
}

// withTransition updates the request with the current transition information.
// This is called before each transition executes to update:
//   - The logger with transition name and version
//   - The run metadata with the current transition state
func (r *Request[_, _]) withTransition(name string, version ulid.ULID) {
	r.logger = r.logger.WithFields(logrus.Fields{
		"transition":         name,
		"transition_version": version,
	})
	r.run.TransitionVersion = version
	r.run.CurrentState = name
}

// withError records an error that occurred during FSM execution. This error
// will cause subsequent transitions to be skipped (via the skipper interceptor).
func (r *Request[_, _]) withError(err RunErr) {
	r.run.fsmErr = err
}

// NewRequest creates a new request to be used for starting a FSM.
// It wraps the typed request message (msg) and response (w) in a Request struct.
// The response pointer can be nil initially; it will be populated as transitions execute.
func NewRequest[R, W any](msg *R, w *W) *Request[R, W] {
	return &Request[R, W]{
		Msg: msg,
		W:   *NewResponse[W](w),
	}
}

// AnyRequest is a type-erased interface for working with requests generically.
// This allows the FSM executor to work with requests without knowing their
// specific types. The interface provides access to:
//   - The untyped message (Any)
//   - The logger (Log)
//   - The run metadata (Run)
//   - Internal methods for updating request state (withLogger, withTransition, withError)
type AnyRequest interface {
	Any() any

	Log() logrus.FieldLogger

	Run() Run

	withLogger(logrus.FieldLogger)

	withTransition(string, ulid.ULID)

	withError(RunErr)
}

// MockRequest creates a request with custom logger and run metadata.
// This is primarily used for testing to inject test-specific loggers and
// run state. Note: this should probably be deprecated once better test
// helpers for executing a transition are introduced.
func MockRequest[R, W any](req *Request[R, W], logger logrus.FieldLogger, run Run) *Request[R, W] {
	return &Request[R, W]{
		Msg:    req.Msg,
		W:      req.W,
		logger: logger,
		run:    run,
	}
}

// Response wraps the typed response message (W) that accumulates output
// from transitions. Each transition can return a Response to update the
// accumulated state, which persists across all transitions in the FSM.
type Response[W any] struct {
	// Msg is the typed response message that accumulates output from transitions.
	// Each transition can update this, and the final value is persisted when
	// the FSM completes.
	Msg *W
}

func (r *Response[_]) Any() any {
	if r == nil {
		return nil
	}
	return r.Msg
}

func (r *Response[_]) internalOnly() {}

func NewResponse[W any](msg *W) *Response[W] {
	return &Response[W]{
		Msg: msg,
	}
}

type AnyResponse interface {
	Any() any

	internalOnly()
}

// RunErr captures an error that occurred during FSM execution along with
// the state where it occurred. This allows the FSM to skip subsequent
// transitions and preserve error context for debugging.
type RunErr struct {
	// Err is the underlying error that occurred.
	Err error

	// State is the name of the transition where the error occurred.
	State string
}

// Run contains the metadata associated with an active FSM execution.
// This information is used for observability, persistence, and control flow.
type Run struct {
	// StartVersion is a unique ULID assigned when the FSM starts. This serves
	// as the primary identifier for the FSM execution and is used for:
	//   - Persisting state to disk
	//   - Resuming after restarts
	//   - Parent-child relationships
	//   - Waiting for completion
	StartVersion ulid.ULID

	// TransitionVersion is a unique ULID assigned to each transition execution.
	// This allows tracking individual transition attempts, which is useful for
	// debugging and observability.
	TransitionVersion ulid.ULID

	// ID is the user-provided identifier for the resource being processed.
	// This is typically a resource ID (e.g., "deploy-123", "image-abc").
	// Multiple FSM runs can share the same ID if they represent different
	// actions on the same resource.
	ID string

	// Action is the name of the action this FSM is executing (e.g., "deploy", "migrate").
	// Combined with TypeName, this uniquely identifies the FSM definition.
	Action string

	// CurrentState is the name of the transition currently being executed.
	// This updates as the FSM progresses through its state machine.
	CurrentState string

	// ResourceName is the alias/name of the resource type (e.g., "deploy_request").
	// This is derived from the request type name and is used for metrics and logging.
	ResourceName string

	// TypeName is the fully qualified type name of the request type (e.g., "DeployRequest").
	// Combined with Action, this uniquely identifies the FSM definition.
	TypeName string

	// Queue is the name of the queue this FSM is using (if any). Queues allow
	// rate limiting by limiting concurrent executions. Empty string means no queue.
	Queue string

	// Parent is the StartVersion of a parent FSM if this FSM is a child.
	// Parent-child relationships allow creating hierarchies of FSMs, which is
	// useful for orchestrating complex workflows.
	Parent ulid.ULID

	// fsmErr is the error and originating state that caused the FSM to stop
	// executing transitions. If set, subsequent transitions will be skipped.
	fsmErr RunErr
}

// fsm represents a registered finite state machine definition. It contains:
//   - The action name and type information
//   - Codecs for serializing/deserializing requests and responses
//   - The ordered list of transitions (states)
//   - Initializers that run before the first transition
//   - Registered transition implementations
//
// The fsm struct is created during registration and remains immutable
// during execution. Multiple runs can execute concurrently using the same
// fsm definition.
type fsm struct {
	// action is the name of the action this FSM handles (e.g., "deploy").
	action string

	// typeName is the fully qualified type name of the request type.
	// alias is a snake_case version of typeName used for metrics/logging.
	typeName, alias string

	// rCodec is used to marshal/unmarshal request messages (R) for persistence.
	// wCodec is used to marshal/unmarshal response messages (W) for persistence.
	// These are automatically determined based on the type (JSON for structs, proto for proto.Message).
	rCodec, wCodec Codec

	// startState is the name of the first transition in the FSM.
	// endState is the name of the last transition in the FSM.
	startState, endState string

	// queue is the default queue name for this FSM (can be overridden per-start).
	queue string

	// parent is the default parent ULID for this FSM (can be overridden per-start).
	parent ulid.ULID

	// initializers are functions that run before the first transition executes.
	// They can modify the context and request, useful for setup/validation.
	initializers []InitializerFunc

	// transitions is an immutable list of transition names in execution order.
	// This preserves the sequence: startState -> ... -> endState.
	transitions *immutable.List[string]

	// registeredTransitions maps transition keys to their implementations.
	// The key combines action, typeName, and transition name to uniquely
	// identify a transition. This allows fast lookup during execution.
	registeredTransitions map[transitionKey]*transition
}

func (f *fsm) transitionSlice() []string {
	names := make([]string, 0, f.transitions.Len())
	itr := f.transitions.Iterator()
	for !itr.Done() {
		_, value := itr.Next()
		names = append(names, value)
	}
	return names
}

// transitionKey uniquely identifies a transition within an FSM definition.
// The combination of action, typeName, and name ensures that transitions
// from different FSMs don't collide.
type transitionKey struct {
	// action is the action name (e.g., "deploy").
	action string

	// typeName is the request type name (e.g., "DeployRequest").
	typeName string

	// name is the transition name (e.g., "fetch-image").
	name string
}

// transition represents a single state transition in the FSM. It contains:
//   - name: The state name
//   - impl: The function that executes when entering this state
//
// The implementation function is wrapped with interceptors (retry, cancellation,
// etc.) during registration, so the impl may be a chain of wrapped functions.
type transition struct {
	// name is the state name for this transition.
	name string

	// impl is the transition function that executes when entering this state.
	// It's wrapped with interceptors (retry, cancellation, etc.) during registration.
	impl TransitionFunc
}

// TransitionFunc is the signature for a transition implementation function.
// It receives:
//   - ctx: The context (can be cancelled to stop the FSM)
//   - request: The typed request with current state and run metadata
//
// It returns:
//   - response: The typed response (can be nil if no output)
//   - error: An error if the transition failed (nil on success)
//
// Errors are handled by interceptors:
//   - Retryable errors are retried with exponential backoff
//   - AbortError stops the FSM without retry
//   - UnrecoverableError stops the FSM permanently
//   - HandoffError transfers control to another FSM
type TransitionFunc func(context.Context, AnyRequest) (AnyResponse, error)

// Attributable is an interface that can be implemented by a request type to
// include additional OpenTelemetry span attributes. This is useful for adding
// custom observability data to traces.
type Attributable interface {
	Attributes() []attribute.KeyValue
}

// newTransition creates a transition from a strongly-typed transition function.
// It:
//  1. Wraps the typed function in a type-erased TransitionFunc
//  2. Applies interceptors in reverse order (last interceptor wraps the function first)
//  3. Returns a transition that can be executed by the FSM
//
// The interceptors are applied in reverse order so that:
//   - The first interceptor in the list is the outermost wrapper
//   - The last interceptor in the list is the innermost wrapper (closest to the actual function)
//
// For example, with interceptors [A, B, C], the execution order is:
//
//	A -> B -> C -> transitionFn
func newTransition[R, W any](name string, transitionFn func(context.Context, *Request[R, W]) (*Response[W], error), cfg TransitionConfig[R, W]) *transition {
	// Wrap the strongly-typed implementation so we can apply interceptors.
	// This converts the typed function to the type-erased TransitionFunc interface.
	untyped := TransitionFunc(func(ctx context.Context, request AnyRequest) (AnyResponse, error) {
		// Check if context was cancelled before proceeding.
		if context.Cause(ctx) == context.Canceled {
			return nil, ctx.Err()
		}

		// Type assert to the expected request type.
		typed, ok := request.(*Request[R, W])
		if !ok {
			return nil, fmt.Errorf("unexpected handler request type %T", request)
		}

		// Call the typed transition function.
		res, err := transitionFn(ctx, typed)

		// Update the request's response wrapper if a response was returned.
		if res != nil {
			typed.W = *res
		}

		return res, err
	})

	// Apply interceptors in reverse order so the first interceptor wraps the outermost layer.
	// This creates a chain: interceptor[0] -> interceptor[1] -> ... -> transitionFn
	if interceptors := cfg.interceptors; interceptors != nil {
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			untyped = interceptor(untyped)
		}
	}

	return &transition{
		name: name,
		impl: untyped,
	}
}

// InitializerFunc is a function that runs before the first transition executes.
// It receives the context and request, and returns a (possibly modified) context.
// Initializers are useful for:
//   - Setting up tracing/spans
//   - Validating the request
//   - Injecting dependencies into the context
//   - Initializing external resources
type InitializerFunc func(context.Context, AnyRequest) context.Context

// newInitializer wraps a strongly-typed initializer function in a type-erased InitializerFunc.
// This allows the FSM executor to call initializers without knowing their specific types.
func newInitializer[R, W any](initFn func(context.Context, *Request[R, W]) context.Context) InitializerFunc {
	return InitializerFunc(func(ctx context.Context, request AnyRequest) context.Context {
		// Type assert to the expected request type.
		typed, ok := request.(*Request[R, W])
		if !ok {
			// If type assertion fails, return the context unchanged.
			return ctx
		}
		// Call the typed initializer function.
		return initFn(ctx, typed)
	})
}

// FinalizerFunc is a function that runs after the FSM completes (successfully or with error).
// It receives:
//   - ctx: The context
//   - request: The request with final state
//   - err: The error that occurred (if any, otherwise RunErr.Err is nil)
//
// Finalizers are useful for:
//   - Cleanup operations
//   - Notifications
//   - Logging final state
//   - Updating external systems
type FinalizerFunc func(context.Context, AnyRequest, RunErr)

// newFinalizer wraps a strongly-typed finalizer function in a type-erased FinalizerFunc.
// This allows the FSM executor to call finalizers without knowing their specific types.
func newFinalizer[R, W any](finalFn func(context.Context, *Request[R, W], RunErr)) FinalizerFunc {
	return FinalizerFunc(func(ctx context.Context, request AnyRequest, err RunErr) {
		// Type assert to the expected request type.
		typedReq, ok := request.(*Request[R, W])
		if !ok {
			// If type assertion fails, silently skip the finalizer.
			return
		}
		// Call the typed finalizer function.
		finalFn(ctx, typedReq, err)
	})
}

// runState represents the state of an FSM run in the in-memory database.
// It combines the Run metadata with the execution state (pending/running/complete)
// and any error that occurred.
type runState struct {
	// Run contains the FSM execution metadata.
	Run

	// State indicates the current execution state:
	//   - RUN_STATE_PENDING: Waiting to start or resume
	//   - RUN_STATE_RUNNING: Currently executing transitions
	//   - RUN_STATE_COMPLETE: Finished (successfully or with error)
	State fsmv1.RunState

	// Error captures any error that occurred during execution.
	Error RunErr
}

// resume returns a function that resumes all active FSMs of the given type.
// This is called during Manager startup to recover FSMs that were interrupted
// by a process restart. The function:
//  1. Queries the persistent store for active FSMs
//  2. Deserializes the request and response from disk
//  3. Prunes completed transitions from the transition list
//  4. Restores context (retry count, restart flag, trace context)
//  5. Resumes execution from where it left off
func resume[R, W any](m *Manager, f *fsm) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		// clearRun removes a run from the in-memory database if resume fails.
		// This prevents leaving orphaned state if we can't properly resume.
		clearRun := func(run Run) {
			txn := m.db.Txn(true)
			defer txn.Abort()

			rs := runState{
				Run:   run,
				State: fsmv1.RunState_RUN_STATE_PENDING,
			}
			if err := txn.Delete(fsmTable, rs); err != nil {
				m.logger.WithError(err).Error("failed to update fsm state store")
			}
			txn.Commit()
			return
		}

		// Query the persistent store for all active FSMs of this type.
		// This returns FSMs that were interrupted before completion.
		resources, err := m.store.Active(ctx, f)
		if err != nil {
			return err
		}

		// Resume each active FSM.
		for _, resource := range resources {
			r := Run{
				ID:           resource.active.GetResourceId(),
				StartVersion: resource.version,
				Action:       f.action,
				ResourceName: f.alias,
				TypeName:     f.typeName,
				Queue:        f.queue,
				Parent:       f.parent,
				fsmErr:       resource.fsmError,
			}

			// Deserialize the request from the persisted state.
			var req R
			if err := f.rCodec.Unmarshal(resource.active.Resource, &req); err != nil {
				m.logger.WithError(err).Error("failed to unmarshal resource, unable to resume")
				defer clearRun(r)
				return err
			}

			// Deserialize the response if it exists (some transitions may have completed).
			var w W
			if resource.response != nil {
				if err := f.wCodec.Unmarshal(resource.response, &w); err != nil {
					m.logger.WithField("response_bytes", string(resource.response)).WithError(err).Error("failed to unmarshal response, unable to resume")
					defer clearRun(r)
					return err
				}
			}

			// Prune completed transitions from the list. We only need to execute
			// transitions that haven't completed yet.
			m.logger.WithField("completed", resource.completedTransitions).Debug("pruning completed transitions")

			remainingTransitions := immutable.NewList[*transition]()
			for _, name := range resource.active.Transitions {
				// Skip transitions that have already completed successfully.
				if !slices.Contains(resource.completedTransitions, name) {
					// Look up the transition implementation.
					transition, ok := f.registeredTransitions[transitionKey{
						action:   f.action,
						typeName: f.typeName,
						name:     name,
					}]
					if !ok {
						// If the transition doesn't exist (e.g., code changed), create a no-op
						// transition that will skip execution but still mark it as complete.
						m.logger.Warn("transition did not exist")
						transition = newTransition(name, noOp[R, W], TransitionConfig[R, W]{
							interceptors: []TransitionInterceptorFunc{
								skipper(),
								canceller(m.store, f.wCodec),
							},
						})
					}
					remainingTransitions = remainingTransitions.Append(transition)
				}
			}

			// Restore the start options from the persisted state.
			var startOpt startOptions
			if delayUntil := resource.active.GetOptions().GetDelayUntil(); delayUntil > 0 {
				startOpt.until = time.Unix(delayUntil, 0)
			}

			if runAfter := resource.active.GetOptions().GetRunAfter(); runAfter != nil {
				if err := startOpt.runAfter.UnmarshalText(runAfter); err != nil {
					m.logger.WithError(err).Error("failed to unmarshal run_after")
				}
			}

			// Restore queue and parent from persisted options.
			f.queue = resource.active.GetOptions().GetQueue()

			if parentBytes := resource.active.GetOptions().GetParent(); parentBytes != nil {
				if err := startOpt.parent.UnmarshalText(parentBytes); err != nil {
					m.logger.WithError(err).Error("failed to unmarshal parent")
				}

				if startOpt.parent.Compare(ulid.ULID{}) != 0 {
					f.parent = startOpt.parent
				}
			}

			// Restore context state: retry count and restart flag.
			// The retry count is preserved so exponential backoff continues correctly.
			ctx := withRetry(ctx, resource.retryCount)
			ctx = withRestart(ctx, true) // Mark this as a restart so transitions know

			// Restore distributed tracing context from the persisted state.
			// This allows tracing to continue across process restarts.
			ctx = (propagation.TraceContext{}).Extract(ctx, propagation.MapCarrier(resource.active.TraceContext))

			// Determine the runner based on the restored options (delayed, queued, etc.).
			runner := runnerFromOpts(&startOpt, m)

			// Create a new request with the deserialized data and run metadata.
			request := NewRequest[R, W](&req, &w)
			request.run = r

			// Resume execution with the remaining transitions.
			run(ctx, request, m, runner, &runInstance{initializers: f.initializers, transitions: remainingTransitions})
		}
		return nil
	}
}

// StartOptionsFn is a function that modifies start options when starting an FSM.
// Multiple options can be combined to configure scheduling, queuing, and relationships.
type StartOptionsFn func(*startOptions)

// startOptions configures how an FSM starts executing. These options are persisted
// so they survive process restarts and can be restored during resume.
type startOptions struct {
	// until specifies a time to delay the start of the FSM. If set, the FSM
	// will wait until this time before beginning execution.
	until time.Time

	// runAfter specifies a parent FSM version to wait for. The FSM will not
	// start until the parent FSM completes. This enables dependencies between FSMs.
	runAfter ulid.ULID

	// queue specifies a queue name to use for rate limiting. If the queue is full,
	// the FSM will wait until capacity is available. Empty string means no queue.
	queue string

	// parent specifies the StartVersion of a parent FSM. This creates a parent-child
	// relationship, useful for tracking and orchestrating FSM hierarchies.
	parent ulid.ULID
}

// WithDelayedStart delays the start of the FSM until the provided time.
// The FSM will be persisted but won't begin executing until the specified time.
// This is useful for scheduling future work.
func WithDelayedStart(until time.Time) StartOptionsFn {
	return func(opts *startOptions) {
		opts.until = until
	}
}

// WithRunAfter delays the start of the FSM until the FSM with the given version
// has completed. This creates a dependency relationship: this FSM waits for the
// parent FSM to finish before starting. Useful for sequencing workflows.
func WithRunAfter(version ulid.ULID) StartOptionsFn {
	return func(opts *startOptions) {
		opts.runAfter = version
	}
}

// WithQueue assigns the FSM to a named queue for rate limiting. If the queue
// has capacity, the FSM starts immediately. Otherwise, it waits until capacity
// is available. Queues are configured when creating the Manager.
func WithQueue(queue string) StartOptionsFn {
	return func(opts *startOptions) {
		opts.queue = queue
	}
}

// WithParent sets the parent FSM version for this FSM. This creates a parent-child
// relationship that allows tracking FSM hierarchies and dependencies.
func WithParent(parent ulid.ULID) StartOptionsFn {
	return func(opts *startOptions) {
		opts.parent = parent
	}
}

// start returns a function that starts a new FSM execution. The returned function:
//  1. Generates a unique run version (ULID)
//  2. Serializes the request for persistence
//  3. Persists a START event to disk
//  4. Begins executing transitions
//
// The id parameter uniquely identifies the resource being processed. Multiple
// FSMs can share the same id if they represent different actions on the same resource.
// The returned ULID (runVersion) uniquely identifies this specific FSM execution.
func start[R, W any](m *Manager, f *fsm) func(ctx context.Context, id string, request *Request[R, W], opts ...StartOptionsFn) (ulid.ULID, error) {
	return func(ctx context.Context, id string, request *Request[R, W], opts ...StartOptionsFn) (ulid.ULID, error) {
		// Apply start options (delayed start, queue, parent, etc.).
		var startOpt startOptions
		for _, opt := range opts {
			opt(&startOpt)
		}

		// Create a logger with FSM context.
		logger := m.logger.WithFields(logrus.Fields{
			"run_id":    id,
			"run_type":  f.typeName,
			"run_alias": f.alias,
		})

		// Serialize the request for persistence. This allows resuming after restarts.
		resource, err := f.rCodec.Marshal(request.Msg)
		if err != nil {
			logger.WithError(err).Error("failed to marshal request")
			return ulid.ULID{}, fmt.Errorf("failed to marshal request: %w", err)
		}

		// Generate a unique version for this FSM run. This ULID serves as:
		//   - The primary identifier for this execution
		//   - A timestamp (embedded in ULID)
		//   - A way to track parent-child relationships
		runVersion := ulid.Make()

		// Mark this as a new start (not a restart).
		ctx = withRestart(ctx, false)

		// Update FSM with queue and parent from options.
		f.queue = startOpt.queue
		if startOpt.parent.Compare(ulid.ULID{}) != 0 {
			f.parent = startOpt.parent
		}

		// Determine the runner based on options (immediate, delayed, queued, etc.).
		r := runnerFromOpts(&startOpt, m)

		// Initialize the run metadata.
		request.run = Run{
			ID:           id,
			StartVersion: runVersion,
			Action:       f.action,
			ResourceName: f.alias,
			TypeName:     f.typeName,
			Queue:        f.queue,
			Parent:       f.parent,
		}

		// Build the list of transitions in execution order.
		transitions := immutable.NewList[*transition]()
		iter := f.transitions.Iterator()
		for !iter.Done() {
			_, value := iter.Next()
			transitions = transitions.Append(f.registeredTransitions[transitionKey{
				action:   f.action,
				typeName: f.typeName,
				name:     value,
			}])
		}

		// Persist a START event to disk. This marks the FSM as active and allows
		// resuming after restarts. The event includes:
		//   - The serialized request
		//   - The list of transitions to execute
		//   - Start options (delay, queue, parent, etc.)
		_, err = m.store.Append(ctx,
			request.run,
			&fsmv1.StateEvent{
				Type:         fsmv1.EventType_EVENT_TYPE_START,
				Id:           id,
				ResourceType: f.typeName,
				Action:       f.action,
				State:        f.startState,
			},
			startOpt.queue,
			withStartOption(resource, f.transitionSlice()),
			withDelayUntil(startOpt.until),
			withRunAfter(startOpt.runAfter),
			withParent(startOpt.parent),
		)
		if err != nil {
			m.logger.WithError(err).Error("failed to append start event")
			return ulid.ULID{}, err
		}

		// Begin executing the FSM. This runs asynchronously (via the runner).
		run(ctx, request, m, r, &runInstance{initializers: f.initializers, transitions: transitions})

		// Return the run version so callers can track/wait for completion.
		return runVersion, nil
	}
}

// runInstance holds the execution configuration for a single FSM run.
// It contains the initializers and transitions that will be executed.
type runInstance struct {
	// initializers are functions that run before the first transition.
	initializers []InitializerFunc

	// transitions is the ordered list of transitions to execute.
	// This may be a subset of all transitions if resuming from a restart.
	transitions *immutable.List[*transition]
}

// run executes an FSM instance. It:
//  1. Sets up distributed tracing
//  2. Runs initializers
//  3. Executes transitions in order
//  4. Handles errors and cancellation
//  5. Emits metrics
//
// The function runs asynchronously via the runner. The runner handles
// scheduling (immediate, delayed, queued, etc.).
func run(ctx context.Context, request AnyRequest, m *Manager, r runner, ri *runInstance) {
	// Create a new context that is not cancelable from the parent context.
	// This allows the FSM to control its own lifecycle independently.
	// The FSM can still be cancelled via Manager.Cancel() which uses a
	// separate cancellation mechanism.
	ctx = context.WithoutCancel(ctx)

	var (
		run        = request.Run()
		runVersion = run.StartVersion
		id         = run.ID
		action     = run.Action
		alias      = run.ResourceName
		typeName   = run.TypeName
		parent     = run.Parent
	)

	// Prepare OpenTelemetry span attributes for distributed tracing.
	startAttrs := []attribute.KeyValue{
		attribute.String("fsm.action", action),
		attribute.String("fsm.alias", alias),
		attribute.String("fsm.type", typeName),
		attribute.String("fsm.version", runVersion.String()),
		attribute.Int("fsm.sdk_version", 2),
	}
	// If the request implements Attributable, include custom attributes.
	if attr, ok := request.Any().(Attributable); ok {
		startAttrs = append(startAttrs, attr.Attributes()...)
	}

	startOpts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(startAttrs...),
	}

	// If the FSM does not have a parent, create a new root span and link it
	// to the caller's span. This allows tracing to continue from the caller
	// into the FSM execution while maintaining separate trace hierarchies.
	if parent.Compare(ulid.ULID{}) == 0 {
		startOpts = append(startOpts,
			trace.WithNewRoot(),
			trace.WithLinks(trace.LinkFromContext(ctx)),
		)
	}

	// Start a new trace span for this FSM execution.
	ctx, span := m.tracer.Start(ctx, fmt.Sprintf("%s.%s", alias, action), startOpts...)

	logger := m.logger.WithFields(logrus.Fields{
		"run_id":      id,
		"run_type":    typeName,
		"run_alias":   alias,
		"run_version": runVersion.String(),
	})

	// runFn is the actual execution function that runs the FSM transitions.
	// It's executed by the runner, which handles scheduling (immediate, delayed, queued).
	runFn := func() {
		// Create a cancellable context for this FSM run. The cancel function
		// is stored in the Manager so it can be called via Manager.Cancel().
		ctx, cancel := context.WithCancelCause(ctx)

		// Register this FSM as running so it can be cancelled externally.
		m.mu.Lock()
		m.running[runVersion] = cancel
		m.mu.Unlock()

		// Cleanup: end trace span, unregister from running map, cancel context.
		defer func() {
			span.End()
			m.mu.Lock()
			delete(m.running, runVersion)
			m.mu.Unlock()
			cancel(nil)
		}()

		logger.Info("starting fsm")

		// Prepare Prometheus metrics with labels for this action/resource.
		localActionCounterVec := actionCounterVec.MustCurryWith(prometheus.Labels{
			"action":   action,
			"resource": alias,
		})

		// Use the ULID timestamp as the action start time for accurate duration tracking.
		actionStartTime := ulid.Time(runVersion.Time())
		localActionDurationVec := actionDurationVec.MustCurryWith(prometheus.Labels{
			"action":   action,
			"resource": alias,
		})

		// Attach logger to request and run initializers.
		request.withLogger(logger)
		for _, init := range ri.initializers {
			ctx = init(ctx, request)
		}

		// Execute transitions in order.
		var err error
		iter := ri.transitions.Iterator()
		for !iter.Done() {
			_, transition := iter.Next()
			transitionName := transition.name

			// Generate a unique version for this transition execution.
			transitionVersion := ulid.Make()
			logger = logger.WithFields(logrus.Fields{
				"transition":         transitionName,
				"transition_version": transitionVersion,
			})
			request.withTransition(transitionName, transitionVersion)

			// Check if context was cancelled before executing the transition.
			select {
			case <-ctx.Done():
				switch ctxErr := context.Cause(ctx); {
				case errors.Is(ctxErr, context.Canceled):
					logger.Info("context canceled, fsm shutting down")
					return
				default:
					// Continue running through transitions even if context is done
					// (allows cleanup transitions to run).
				}
			default:
			}

			logger.Info("running transition")

			// Execute the transition in a goroutine so we can detect cancellation
			// while it's running. This allows timely cancellation of long-running transitions.
			errc := make(chan error)
			defer close(errc)
			go func() {
				_, implErr := transition.impl(ctx, request)
				errc <- implErr
			}()

			// Wait for either cancellation or completion.
			select {
			case <-ctx.Done():
				// Context was cancelled. Still wait for the transition to finish
				// so we can capture any error it returns.
				if context.Cause(ctx) == context.Canceled {
					logger.Debug("context canceled, fsm shutting down")
				}
				err = context.Cause(ctx)
				chanErr := <-errc
				if chanErr != nil {
					err = chanErr
				}
			case err = <-errc:
				// Transition completed (successfully or with error).
			}

			// Handle different error types:
			//   - AbortError: Stop without retry
			//   - UnrecoverableError: Stop permanently (system or user error)
			//   - HandoffError: Transfer to another FSM
			//   - Other errors: Record but continue (handled by retry interceptor)
			var (
				ae *AbortError
				ue *UnrecoverableError
				he *HandoffError
			)
			switch {
			case err == nil:
				// Success, continue to next transition.
				continue
			case errors.As(err, &ae):
				// Abort: stop execution without retry.
				localActionCounterVec.WithLabelValues("abort", "").Inc()
				localActionDurationVec.WithLabelValues("abort", "").Observe(time.Since(actionStartTime).Seconds())
				span.SetAttributes(attribute.String("fsm.error_kind", "abort"))
			case errors.As(err, &ue):
				// Unrecoverable: stop permanently.
				kind := ue.Kind.String()
				localActionCounterVec.WithLabelValues("unrecoverable", kind).Inc()
				localActionDurationVec.WithLabelValues("unrecoverable", "").Observe(time.Since(actionStartTime).Seconds())
				span.SetAttributes(attribute.String("fsm.error_kind", kind))
				logger.WithError(err).Error("reached unrecoverable error, canceling FSM")
			case errors.As(err, &he):
				// Handoff: transfer to another FSM.
				localActionCounterVec.WithLabelValues("fsm_handoff_error", "").Inc()
				localActionDurationVec.WithLabelValues("fsm_handoff_error", "").Observe(time.Since(actionStartTime).Seconds())
				span.SetAttributes(attribute.String("fsm.error_kind", "handoff"))
			}

			// Record the error in the request so subsequent transitions are skipped.
			request.withError(RunErr{
				Err:   err,
				State: transitionName,
			})
		}

		// If we completed without error, record success metrics.
		if err == nil {
			localActionCounterVec.WithLabelValues("ok", "").Inc()
			localActionDurationVec.WithLabelValues("ok", "").Observe(time.Since(actionStartTime).Seconds())
		}
	}

	// Execute the FSM asynchronously via the runner. The runner handles:
	//   - Immediate execution (defaultRunner)
	//   - Delayed execution (delayedRunner)
	//   - Queued execution (queuedRunner)
	//   - Waiting for dependencies (runAfter)
	//
	// The ack channel is closed when the runner has accepted the FSM for execution.
	// This allows the caller to know the FSM has been scheduled (but not necessarily started).
	m.wg.Add(1)
	ack := make(chan struct{})
	go func() {
		defer m.wg.Done()
		r.Run(ctx, logger, ack, runFn)
	}()

	// Wait for acknowledgement that the FSM has been scheduled.
	<-ack
	return
}

// noOp is a no-op transition function used when a transition doesn't exist
// during resume (e.g., code changed). It skips execution but still marks
// the transition as complete so the FSM can continue.
func noOp[R, W any](ctx context.Context, req *Request[R, W]) (*Response[W], error) {
	return nil, nil
}
