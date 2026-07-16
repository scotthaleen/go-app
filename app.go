package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	// DefaultStartTimeout is the default timeout for the entire startup phase.
	DefaultStartTimeout = 60 * time.Second
	// DefaultStopTimeout is the default timeout for internally managed shutdown.
	DefaultStopTimeout = 60 * time.Second
)

// App coordinates component startup and shutdown.
type App struct {
	mu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	startupGroups [][]*Component
	initialized   []*Component
	startWG       sync.WaitGroup

	startTimeout   time.Duration
	stopTimeout    time.Duration
	logger         *slog.Logger
	signalHandling bool
	startCalled    bool
	closed         bool
}

var (
	// ErrAlreadyStarted is returned when Start is called more than once.
	ErrAlreadyStarted = errors.New("app: already started")
	// ErrClosed is returned when Start is called after the app has closed.
	ErrClosed = errors.New("app: closed")
)

// RequestShutdownFunc requests cancellation of the app runtime context.
type RequestShutdownFunc func()

// RuntimeContext carries app-lifetime cancellation and registry values for
// runtime goroutines started by components.
type RuntimeContext struct {
	context.Context
}

// Option configures an App.
type Option func(*App)

// StartupItem describes a component's lifecycle and registry behavior within a
// startup group.
type StartupItem struct {
	provider Provider
	apply    func(*App)
}

// New creates an App derived from ctx.
func New(ctx context.Context, opts ...Option) *App {
	ctx, cancel := context.WithCancel(ctx)
	a := &App{
		ctx:            ctx,
		cancel:         cancel,
		startTimeout:   DefaultStartTimeout,
		stopTimeout:    DefaultStopTimeout,
		logger:         slog.Default(),
		signalHandling: true,
	}
	a.ctx = Register[RequestShutdownFunc](a.ctx, RequestShutdownFunc(cancel))
	for _, opt := range opts {
		opt(a)
	}
	a.ctx = Register[RuntimeContext](a.ctx, RuntimeContext{Context: a.ctx})
	return a
}

// WithDependency registers val in the app context by its exact type T.
func WithDependency[T any](val T) Option {
	return func(a *App) {
		a.ctx = Register(a.ctx, val)
	}
}

// Managed creates a lifecycle-only startup item.
func Managed(p Provider) StartupItem {
	return StartupItem{provider: p}
}

// Registered creates a startup item that also registers val as an app-scoped
// dependency by its exact type T.
func Registered[T Provider](val T) StartupItem {
	return StartupItem{
		provider: val,
		apply: func(a *App) {
			a.ctx = Register(a.ctx, val)
		},
	}
}

// WithSequentialStartup starts each item in order, waiting for OnStart to return
// before starting the next item.
func WithSequentialStartup(items ...StartupItem) Option {
	return func(a *App) {
		for _, item := range items {
			a.addStartupGroup(item)
		}
	}
}

// WithConcurrentStartup starts all items together and waits for every OnStart
// callback to return before starting the next group.
func WithConcurrentStartup(items ...StartupItem) Option {
	return func(a *App) {
		a.addStartupGroup(items...)
	}
}

// WithComponent adds p as a sequential lifecycle-only component.
func WithComponent(p Provider) Option {
	return func(a *App) {
		WithSequentialStartup(Managed(p))(a)
	}
}

// WithRegisteredComponent adds val as a sequential component and app-scoped
// dependency.
func WithRegisteredComponent[T Provider](val T) Option {
	return func(a *App) {
		WithSequentialStartup(Registered(val))(a)
	}
}

func (a *App) addStartupGroup(items ...StartupItem) {
	if len(items) == 0 {
		return
	}
	group := make([]*Component, 0, len(items))
	for _, item := range items {
		if item.apply != nil {
			item.apply(a)
		}
		group = append(group, item.provider.Component())
	}
	a.startupGroups = append(a.startupGroups, group)
}

// WithStartTimeout sets the timeout for the entire startup phase. A zero or
// negative timeout disables the configured deadline.
func WithStartTimeout(timeout time.Duration) Option {
	return func(a *App) {
		a.startTimeout = timeout
	}
}

// WithStopTimeout sets the timeout for internally managed whole-app shutdown in
// Run, RunOnce, and startup rollback. Direct Stop and Close calls use their
// supplied contexts. A zero or negative timeout disables the configured deadline.
func WithStopTimeout(timeout time.Duration) Option {
	return func(a *App) {
		a.stopTimeout = timeout
	}
}

// WithLogger sets the logger used for lifecycle diagnostics. A nil logger is
// ignored.
func WithLogger(logger *slog.Logger) Option {
	return func(a *App) {
		if logger != nil {
			a.logger = logger
		}
	}
}

// WithSignalHandling controls whether Run requests shutdown on SIGINT or
// SIGTERM. Signal handling is enabled by default.
func WithSignalHandling(enabled bool) Option {
	return func(a *App) {
		a.signalHandling = enabled
	}
}

// Context returns the app runtime context and registry.
func (a *App) Context() context.Context { return a.ctx }

// Done returns a channel closed when app shutdown is requested.
func (a *App) Done() <-chan struct{} { return a.ctx.Done() }

// RequestShutdown cancels the app runtime context. It does not stop components.
func (a *App) RequestShutdown() { a.cancel() }

// Start runs each component's OnStart callback. ctx controls the entire startup
// phase, not each component independently. Start must not be called concurrently
// with other lifecycle methods.
func (a *App) Start(ctx context.Context) error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return ErrClosed
	}
	if a.startCalled {
		a.mu.Unlock()
		return ErrAlreadyStarted
	}
	a.startCalled = true
	a.mu.Unlock()

	startCtx := a.ctx
	if ctx != nil {
		var cancel context.CancelFunc
		startCtx, cancel = context.WithCancel(a.ctx)
		linkedStartCtx := startCtx
		go func() {
			select {
			case <-ctx.Done():
				cancel()
			case <-linkedStartCtx.Done():
			}
		}()
		defer cancel()
	}
	if a.startTimeout > 0 {
		var cancel context.CancelFunc
		startCtx, cancel = context.WithTimeout(startCtx, a.startTimeout)
		defer cancel()
	}

	for _, group := range a.startupGroups {
		initialized, pending, err := a.startGroup(startCtx, group)
		a.addInitialized(initialized)
		if err != nil {
			a.cancel()
			a.markClosed()
			stopCtx := a.shutdownContext()
			defer stopCtx.cancel()
			stopErr := a.stopInitialized(stopCtx.ctx)
			waitErr := a.waitStartCallbacks(stopCtx.ctx)
			lateStopErr := a.stopPendingInitialized(stopCtx.ctx, pending)
			return errors.Join(err, stopErr, waitErr, lateStopErr)
		}
	}
	return nil
}

// Stop stops initialized components in reverse startup order. A component is
// initialized when its OnStart callback returns nil.
//
// Stop does not request app shutdown or cancel RuntimeContext. Use Close for a
// full shutdown path, or call RequestShutdown before Stop when runtime
// cancellation is required. Stop must not be called concurrently with other
// lifecycle methods.
func (a *App) Stop(ctx context.Context) error {
	a.markClosed()
	return a.stopInitialized(a.stopContext(ctx))
}

func (a *App) markClosed() {
	a.mu.Lock()
	a.closed = true
	a.mu.Unlock()
}

func (a *App) addInitialized(components []*Component) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.initialized = append(a.initialized, components...)
}

// Close requests application shutdown, stops initialized components, and waits
// for component OnStart goroutines to return. Close must not be called
// concurrently with other lifecycle methods.
//
// Close is the preferred cleanup helper for short-lived applications and CLI
// commands that call Start directly instead of Run. The provided context is the
// shutdown context used for component Stop calls; it should usually be derived
// from context.WithoutCancel or context.Background, not from an already-canceled
// request context.
func (a *App) Close(ctx context.Context) error {
	a.RequestShutdown()
	a.markClosed()
	stopCtx := a.stopContext(ctx)
	stopErr := a.stopInitialized(stopCtx)
	waitErr := a.waitStartCallbacks(stopCtx)
	return errors.Join(stopErr, waitErr)
}

// RunOnce starts the app, runs fn with the app context, then closes the app.
//
// RunOnce is for short-lived commands and jobs that need lifecycle-managed
// dependencies but should not block until cancellation like Run. ctx controls
// startup only; fn receives the app context. If fn returns an error and shutdown
// also fails, both errors are joined. RunOnce must not be called concurrently
// with other lifecycle methods.
func (a *App) RunOnce(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		fn = func(context.Context) error { return nil }
	}
	if err := a.Start(ctx); err != nil {
		return err
	}
	runErr := fn(a.Context())
	shutdown := a.shutdownContext()
	defer shutdown.cancel()
	closeErr := a.Close(shutdown.ctx)
	return errors.Join(runErr, closeErr)
}

// Run starts the app, waits for app cancellation, then stops its components. Run
// must not be called concurrently with other lifecycle methods.
func (a *App) Run() error {
	var stopSignals context.CancelFunc
	if a.signalHandling {
		signalCtx, stop := signal.NotifyContext(a.ctx, os.Interrupt, syscall.SIGTERM)
		stopSignals = stop
		defer stopSignals()
		go func() {
			<-signalCtx.Done()
			a.logger.Debug("app shutdown requested")
			a.cancel()
		}()
	}

	if err := a.Start(a.ctx); err != nil {
		return err
	}
	<-a.ctx.Done()

	shutdown := a.shutdownContext()
	defer shutdown.cancel()
	err := a.Stop(shutdown.ctx)
	waitErr := a.waitStartCallbacks(shutdown.ctx)
	return errors.Join(err, waitErr)
}

func (a *App) waitStartCallbacks(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		a.startWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for component start callbacks: %w", ctx.Err())
	}
}

type componentStartResult struct {
	initialized bool
	err         error
	pending     <-chan componentStartResult
}

type indexedStartResult struct {
	index int
	componentStartResult
}

type pendingComponentStart struct {
	component *Component
	result    <-chan componentStartResult
}

func (a *App) startGroup(ctx context.Context, group []*Component) ([]*Component, []pendingComponentStart, error) {
	if len(group) == 0 {
		return nil, nil, nil
	}
	groupCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan indexedStartResult, len(group))
	for i, c := range group {
		go func(i int, c *Component) {
			resultCh <- indexedStartResult{index: i, componentStartResult: a.startComponent(groupCtx, c)}
		}(i, c)
	}

	initialized := make([]bool, len(group))
	pendingResults := make([]<-chan componentStartResult, len(group))
	var errs []error
	for range group {
		result := <-resultCh
		initialized[result.index] = result.initialized
		pendingResults[result.index] = result.pending
		if result.err != nil {
			errs = append(errs, result.err)
			cancel()
		}
	}

	initializedComponents := make([]*Component, 0, len(group))
	for i, ok := range initialized {
		if ok {
			initializedComponents = append(initializedComponents, group[i])
		}
	}
	pending := make([]pendingComponentStart, 0, len(group))
	for i, result := range pendingResults {
		if result != nil {
			pending = append(pending, pendingComponentStart{component: group[i], result: result})
		}
	}
	return initializedComponents, pending, errors.Join(errs...)
}

func (a *App) startComponent(ctx context.Context, c *Component) componentStartResult {
	a.logger.Debug("starting component", "component", c.Name())
	startResultCh := make(chan componentStartResult, 1)
	a.startWG.Add(1)
	go func() {
		defer a.startWG.Done()
		if err := c.OnStart(ctx); err != nil {
			startResultCh <- componentStartResult{err: fmt.Errorf("start %s: %w", c.Name(), err)}
			return
		}
		startResultCh <- componentStartResult{initialized: true}
	}()

	select {
	case result := <-startResultCh:
		if result.err != nil {
			return result
		}
		if err := ctx.Err(); err != nil {
			result.err = fmt.Errorf("start %s: %w", c.Name(), err)
			return result
		}
		a.logger.Debug("component initialized", "component", c.Name())
		return result
	case <-ctx.Done():
		return canceledComponentStart(ctx, c, componentStartResult{}, startResultCh)
	}
}

func canceledComponentStart(ctx context.Context, c *Component, result componentStartResult, startResultCh <-chan componentStartResult) componentStartResult {
	select {
	case startResult := <-startResultCh:
		if startResult.err != nil {
			return startResult
		}
		result.initialized = startResult.initialized
	default:
		result.pending = startResultCh
	}
	result.err = fmt.Errorf("start %s: %w", c.Name(), ctx.Err())
	return result
}

func (a *App) stopInitialized(ctx context.Context) error {
	a.mu.Lock()
	initialized := append([]*Component(nil), a.initialized...)
	a.mu.Unlock()

	remaining, err := a.stopComponents(ctx, initialized)
	a.mu.Lock()
	a.initialized = remaining
	a.mu.Unlock()
	return err
}

func (a *App) stopPendingInitialized(ctx context.Context, pending []pendingComponentStart) error {
	initialized := make([]*Component, 0, len(pending))
	for _, start := range pending {
		select {
		case result := <-start.result:
			if result.initialized {
				initialized = append(initialized, start.component)
			}
		default:
		}
	}

	remaining, err := a.stopComponents(ctx, initialized)
	a.addInitialized(remaining)
	return err
}

func (a *App) stopComponents(ctx context.Context, components []*Component) ([]*Component, error) {
	var errs []error
	stopped := make([]bool, len(components))
	for i := len(components) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("shutdown before stopping all components: %w", err))
			break
		}
		c := components[i]
		a.logger.Debug("stopping component", "component", c.Name())
		if err := c.OnStop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("stop %s: %w", c.Name(), err))
			continue
		}
		stopped[i] = true
		a.logger.Debug("component stopped", "component", c.Name())
	}

	remaining := make([]*Component, 0, len(components))
	for i, c := range components {
		if !stopped[i] {
			remaining = append(remaining, c)
		}
	}
	return remaining, errors.Join(errs...)
}

type stopContext struct {
	control context.Context
	values  context.Context
}

func (c stopContext) Deadline() (time.Time, bool) { return c.control.Deadline() }

func (c stopContext) Done() <-chan struct{} { return c.control.Done() }

func (c stopContext) Err() error { return c.control.Err() }

func (c stopContext) Value(key any) any {
	if value := c.control.Value(key); value != nil {
		return value
	}
	return c.values.Value(key)
}

func (a *App) stopContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return stopContext{control: ctx, values: context.WithoutCancel(a.ctx)}
}

type shutdownContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (a *App) shutdownContext() shutdownContext {
	base := context.WithoutCancel(a.ctx)
	if a.stopTimeout <= 0 {
		ctx, cancel := context.WithCancel(base)
		return shutdownContext{ctx: ctx, cancel: cancel}
	}
	ctx, cancel := context.WithTimeout(base, a.stopTimeout)
	return shutdownContext{ctx: ctx, cancel: cancel}
}
