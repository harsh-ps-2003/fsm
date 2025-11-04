// Package fsm contains the builder API for registering FSMs.
// This file provides a fluent builder pattern for defining FSM state machines
// with their transitions, initializers, interceptors, and finalizers.
package fsm

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/benbjohnson/immutable"
	"github.com/iancoleman/strcase"
	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// fsmStart is the builder state after calling Register. It supports:
//   - Start(): Define the initial transition
type fsmStart[R, W any] struct {
	transitionStep[R, W]
}

// fsmTransition is the builder state after calling Start or To. It supports:
//   - To(): Add another transition
//   - End(): Complete the FSM definition
type fsmTransition[R, W any] struct {
	transitionStep[R, W]
}

// fsmEnd is the builder state after calling End. It supports:
//   - Build(): Finalize the FSM and return start/resume functions
type fsmEnd[R, W any] struct {
	transitionStep[R, W]
}

// transitionStep is the shared state for all builder stages.
type transitionStep[R, W any] struct {
	// m is the Manager this FSM will be registered with.
	m *Manager

	// f is the FSM definition being built.
	f *fsm

	// cfg accumulates transition configuration (initializers, interceptors, finalizers).
	cfg *TransitionConfig[R, W]

	// buildError accumulates any errors encountered during building.
	buildError error
}

// nameable is an interface that request types can implement to provide
// a custom resource name for metrics and logging. If not implemented,
// the type name is converted to snake_case automatically.
type nameable interface {
	Name() string
}

// Register creates a new FSM builder for the given action and request/response types.
// The action name uniquely identifies this workflow (e.g., "deploy", "migrate").
// R is the request type (input) and W is the response type (output).
//
// Example:
//   start, resume, _ := fsm.Register[DeployRequest, DeployResponse](manager, "deploy").
//       Start("fetch", fetchImage).
//       To("unpack", unpackImage).
//       End("activate", activateContainer).
//       Build(ctx)
func Register[R, W any](m *Manager, action string) *fsmStart[R, W] {
	var (
		r     R
		w     W
		name  = getType(r)
		alias = strcase.ToSnake(name)
	)

	if nameable, ok := NewRequest(&r, &w).Any().(nameable); ok {
		alias = nameable.Name()
	}

	fs := &fsmStart[R, W]{
		transitionStep: transitionStep[R, W]{
			m: m,
			f: &fsm{
				action:                action,
				typeName:              name,
				alias:                 alias,
				transitions:           immutable.NewList[string](),
				registeredTransitions: map[transitionKey]*transition{},
			},
			cfg: &TransitionConfig[R, W]{
				initializers: []Initializer[R, W]{},
				interceptors: []TransitionInterceptorFunc{},
			},
		},
	}

	rc, err := determineCodec(m.logger, r)
	if err != nil {
		fs.buildError = fmt.Errorf("unable to determine codec for Request type: %w", err)
	}
	fs.f.rCodec = rc

	wc, err := determineCodec(m.logger, w)
	if err != nil {
		fs.buildError = errors.Join(fs.buildError, fmt.Errorf("unable to determine codec for Response type: %w", err))
	}
	fs.f.wCodec = wc

	return fs
}

func getType(myvar any) string {
	t := reflect.TypeOf(myvar)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

// TransitionConfig accumulates configuration for transitions during FSM building.
// Different configuration types can only be set at specific builder stages.
type TransitionConfig[R, W any] struct {
	// initializers are functions that run before the first transition.
	// They can only be configured when calling Start().
	initializers []Initializer[R, W]

	// interceptors wrap transition functions with cross-cutting concerns
	// (retry, cancellation, logging, etc.). They can be configured when
	// calling Start() or To().
	interceptors []TransitionInterceptorFunc

	// finalizers are functions that run after the FSM completes.
	// They can only be configured when calling End().
	finalizers []Finalizer[R, W]
}

type Option[R, W any] interface {
	apply(*TransitionConfig[R, W]) *TransitionConfig[R, W]
}

type StartOption[R, W any] interface {
	Option[R, W]
	applyStart(*TransitionConfig[R, W]) *TransitionConfig[R, W]
}

type EndOption[R, W any] interface {
	applyEnd(*TransitionConfig[R, W]) *TransitionConfig[R, W]
}

type initializerOption[R, W any] []Initializer[R, W]

func (o initializerOption[R, W]) apply(cfg *TransitionConfig[R, W]) *TransitionConfig[R, W] {
	cfg.initializers = append(cfg.initializers, o...)
	return cfg
}

func (o initializerOption[R, W]) applyStart(cfg *TransitionConfig[R, W]) *TransitionConfig[R, W] {
	return o.apply(cfg)
}

// Initializer can be used to modify the context.Context provided to transitions as well as modify
// the Request before the transition is executed.
type Initializer[R, W any] func(context.Context, *Request[R, W]) context.Context

// WithInitializers adds the provided initializers to the list of initializers to be executed before
// the first transition is executed.
func WithInitializers[R, W any](i ...Initializer[R, W]) StartOption[R, W] {
	return initializerOption[R, W](i)
}

type TransitionInterceptorFunc func(TransitionFunc) TransitionFunc

type interceptorOption[R, W any] []TransitionInterceptorFunc

func (o interceptorOption[R, W]) apply(cfg *TransitionConfig[R, W]) *TransitionConfig[R, W] {
	cfg.interceptors = append(cfg.interceptors, o...)
	return cfg
}

func (o interceptorOption[R, W]) applyStart(cfg *TransitionConfig[R, W]) *TransitionConfig[R, W] {
	return o.apply(cfg)
}

// WithInterceptors adds the provided TransitionInterceptorFunc to the list of interceptors to
// be executed with the transition and should take care to call the next function in the chain.
func WithInterceptors[R, W any](i ...TransitionInterceptorFunc) StartOption[R, W] {
	return interceptorOption[R, W](i)
}

type finalizerOption[R, W any] []Finalizer[R, W]

func (o finalizerOption[R, W]) applyEnd(cfg *TransitionConfig[R, W]) *TransitionConfig[R, W] {
	cfg.finalizers = append(cfg.finalizers, o...)
	return cfg
}

type Finalizer[R, W any] func(context.Context, *Request[R, W], RunErr)

// WithFinalizers adds the provided Finalizers to the list of finalizers to be executed when the FSM
// has completed.
func WithFinalizers[R, W any](f ...Finalizer[R, W]) EndOption[R, W] {
	return finalizerOption[R, W](f)
}

type Transition[R, W any] func(context.Context, *Request[R, W]) (*Response[W], error)

// Start defines the initial transition (state) of the FSM and applies any options.
// This must be called first after Register(). The name is the state name, and
// transition is the function that executes when entering this state.
//
// Start options can include initializers and interceptors that apply to the entire FSM.
func (s *fsmStart[R, W]) Start(name string, transition Transition[R, W], startOpts ...StartOption[R, W]) *fsmTransition[R, W] {
	s.f.startState = name

	opts := make([]Option[R, W], 0, len(startOpts)+1)
	opts = append(opts, WithInitializers[R, W](setStarted[R, W](s.m.db)))
	for _, o := range startOpts {
		opts = append(opts, o)
	}

	return (&fsmTransition[R, W]{s.transitionStep}).To(name, transition, opts...)
}

// To adds another transition to the FSM. Transitions execute in the order they
// are added. The name is the state name, and transition is the function that
// executes when entering this state.
//
// Options can include interceptors that apply only to this transition.
func (s *fsmTransition[R, W]) To(name string, transition Transition[R, W], opts ...Option[R, W]) *fsmTransition[R, W] {
	tk := transitionKey{
		action:   s.f.action,
		typeName: s.f.typeName,
		name:     name,
	}
	if _, ok := s.f.registeredTransitions[tk]; ok {
		s.m.logger.WithField("transition", name).Error("transition already registered")
		s.buildError = errors.Join(s.buildError, fmt.Errorf("transition %s already registered", name))
		return &fsmTransition[R, W]{s.transitionStep}
	}

	s.cfg.interceptors = []TransitionInterceptorFunc{
		skipper(),
		canceller(s.m.store, s.f.wCodec),
		retry(s.m.tracer, s.m.store),
	}

	for _, o := range opts {
		o.apply(s.cfg)
	}

	s.f.initializers = make([]InitializerFunc, 0, len(s.cfg.initializers))
	for _, i := range s.cfg.initializers {
		s.f.initializers = append(s.f.initializers, newInitializer(i))
	}

	s.f.registeredTransitions[tk] = newTransition(name, transition, *s.cfg)
	s.f.transitions = s.f.transitions.Append(name)

	return &fsmTransition[R, W]{s.transitionStep}
}

// End defines the final transition (state) of the FSM and completes the builder.
// This must be called last. The name is the final state name, and a finisher
// function is automatically created that runs finalizers and marks the FSM complete.
//
// End options can include finalizers that run when the FSM completes.
func (s *fsmTransition[R, W]) End(name string, opts ...EndOption[R, W]) *fsmEnd[R, W] {
	fk := fsmKey{
		name:   s.f.typeName,
		action: s.f.action,
	}

	if _, ok := s.m.fsms[fk]; ok {
		s.m.logger.WithField("fsm", s.f.typeName).Error("fsm already registered")
		s.buildError = errors.Join(s.buildError, fmt.Errorf("fsm %s:%s already registered", s.f.typeName, s.f.action))
		return &fsmEnd[R, W]{s.transitionStep}
	}

	tk := transitionKey{
		action:   s.f.action,
		typeName: s.f.typeName,
		name:     name,
	}
	if _, ok := s.f.registeredTransitions[tk]; ok {
		s.m.logger.WithField("transition", name).Error("transition already registered")
		s.buildError = errors.Join(s.buildError, fmt.Errorf("transition %s already registered", name))
		return &fsmEnd[R, W]{s.transitionStep}
	}

	cfg := TransitionConfig[R, W]{
		interceptors: []TransitionInterceptorFunc{
			retry(s.m.tracer, s.m.store),
		},
	}
	for _, opt := range opts {
		opt.applyEnd(&cfg)
	}

	finalizers := make([]FinalizerFunc, 0, len(cfg.finalizers))
	for _, f := range cfg.finalizers {
		finalizers = append(finalizers, newFinalizer(f))
	}

	s.f.registeredTransitions[tk] = newTransition(name, finisher[R, W](s.m, finalizers), cfg)
	s.f.transitions = s.f.transitions.Append(name)
	s.f.endState = name

	s.m.fsms[fk] = s.f

	return &fsmEnd[R, W]{s.transitionStep}
}

// Start is the function type returned by Build() for starting new FSM executions.
// It takes a context, resource ID, request, and optional start options, and returns
// the FSM's StartVersion (ULID) which can be used to track/wait for completion.
type Start[R, W any] func(ctx context.Context, id string, req *Request[R, W], opts ...StartOptionsFn) (ulid.ULID, error)

// Resume is the function type returned by Build() for resuming interrupted FSMs.
// It should be called during application startup to recover FSMs that were interrupted
// by a process restart.
type Resume func(context.Context) error

// Build finalizes the FSM definition and returns:
//   1. A Start function for starting new FSM executions
//   2. A Resume function for resuming interrupted FSMs
//   3. An error if the FSM definition is invalid
//
// After Build() is called, the FSM is registered with the Manager and ready to use.
func (s *fsmEnd[R, W]) Build(ctx context.Context) (Start[R, W], Resume, error) {
	if s.buildError != nil {
		return nil, nil, s.buildError
	}

	wrappedResume := func(ctx context.Context) error {
		if err := resume[R, W](s.m, s.f)(ctx); err != nil {
			return fmt.Errorf("failed to resume active FSMs: %w", err)
		}
		return nil
	}

	return start[R, W](s.m, s.f), wrappedResume, nil
}

// determineCodec automatically selects an appropriate codec for serializing/deserializing
// the given type. It checks:
//   1. If the type implements Codec, use it directly
//   2. If the type implements proto.Message, use protoBinaryCodec
//   3. Otherwise, try jsonCodec (requires JSON marshal/unmarshal support)
//
// This allows the FSM to work with various message types without explicit codec configuration.
func determineCodec(logger logrus.FieldLogger, req any) (Codec, error) {
	if codec, ok := req.(Codec); ok {
		logger.Info("using provided codec")
		return codec, nil
	}

	if _, ok := req.(proto.Message); ok {
		logger.Info("using proto codec")
		return &protoBinaryCodec{}, nil
	}

	codec := &jsonCodec{}
	b, err := codec.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("no codec provided and could not use json codec for %T: %w", req, err)
	}

	var req2 any
	if err := codec.Unmarshal(b, &req2); err != nil {
		return nil, fmt.Errorf("no codec provided and could not use json codec for %T: %w", req, err)
	}
	logger.Info("using json codec")

	return &jsonCodec{}, nil
}
