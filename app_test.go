package app

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type testProvider struct {
	component *Component
}

func (p testProvider) Component() *Component { return p.component }

type readyTestProvider struct {
	component *Component
	ready     chan struct{}
}

func (p *readyTestProvider) Component() *Component { return p.component }

func (p *readyTestProvider) Ready() <-chan struct{} { return p.ready }

func TestReadinessWaitIsConsumerOwned(t *testing.T) {
	databaseInitialized := make(chan struct{})
	database := &readyTestProvider{ready: make(chan struct{})}
	database.component = NewComponent(
		WithName("database"),
		WithOnStart(func(context.Context) error {
			close(databaseInitialized)
			return nil
		}),
	)

	dependentConsumer := func(name string) (testProvider, chan struct{}) {
		initialized := make(chan struct{})
		return testProvider{component: NewComponent(
			WithName(name),
			WithOnStart(func(ctx context.Context) error {
				database := MustGet[*readyTestProvider](ctx)
				select {
				case <-database.Ready():
					close(initialized)
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		)}, initialized
	}
	apiA, apiAInitialized := dependentConsumer("api-a")
	apiB, apiBInitialized := dependentConsumer("api-b")
	metricsInitialized := make(chan struct{})
	metrics := testProvider{component: NewComponent(
		WithName("metrics"),
		WithOnStart(func(context.Context) error {
			close(metricsInitialized)
			return nil
		}),
	)}

	a := New(
		context.Background(),
		WithSignalHandling(false),
		WithSequentialStartup(Registered(database)),
		WithConcurrentStartup(Managed(apiA), Managed(apiB), Managed(metrics)),
	)
	startErr := startApp(a)

	select {
	case <-databaseInitialized:
	case <-time.After(time.Second):
		t.Fatal("database did not initialize")
	}
	select {
	case <-metricsInitialized:
	case <-time.After(time.Second):
		t.Fatal("unrelated consumer waited for database readiness")
	}
	select {
	case <-apiAInitialized:
		t.Fatal("dependent consumer initialized before database readiness")
	default:
	}
	select {
	case <-apiBInitialized:
		t.Fatal("second dependent consumer initialized before database readiness")
	default:
	}
	select {
	case err := <-startErr:
		t.Fatalf("Start returned while dependent consumer was waiting: %v", err)
	default:
	}

	close(database.ready)
	select {
	case <-apiAInitialized:
	case <-time.After(time.Second):
		t.Fatal("dependent consumer did not observe database readiness")
	}
	select {
	case <-apiBInitialized:
	case <-time.After(time.Second):
		t.Fatal("second dependent consumer did not observe shared database readiness")
	}
	select {
	case err := <-startErr:
		if err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return")
	}
	if err := a.Close(context.Background()); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

func TestStartWaitsForOnStartReturn(t *testing.T) {
	release := make(chan struct{})
	provider := testProvider{component: NewComponent(
		WithOnStart(func(ctx context.Context) error {
			<-release
			return nil
		}),
	)}
	a := New(context.Background(), WithSignalHandling(false), WithComponent(provider))
	startErr := startApp(a)

	select {
	case err := <-startErr:
		t.Fatalf("Start returned before OnStart completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-startErr:
		if err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return after OnStart completed")
	}
}

func TestStopStopsComponentsInReverseStartOrder(t *testing.T) {
	var (
		mu    sync.Mutex
		order []string
	)
	provider := func(name string) testProvider {
		return testProvider{component: NewComponent(
			WithName(name),
			WithOnStart(func(ctx context.Context) error { return nil }),
			WithOnStop(func(ctx context.Context) error {
				mu.Lock()
				defer mu.Unlock()
				order = append(order, name)
				return nil
			}),
		)}
	}
	a := New(context.Background(), WithSignalHandling(false), WithComponent(provider("first")), WithComponent(provider("second")), WithComponent(provider("third")))

	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if err := a.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}

	want := []string{"third", "second", "first"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("stop order = %v, want %v", order, want)
	}
}

func TestSequentialStartupStartsComponentsInOrder(t *testing.T) {
	var order []string
	provider := func(name string) testProvider {
		return testProvider{component: NewComponent(
			WithName(name),
			WithOnStart(func(ctx context.Context) error {
				order = append(order, name)
				return nil
			}),
		)}
	}
	a := New(context.Background(), WithSignalHandling(false), WithSequentialStartup(Managed(provider("first")), Managed(provider("second")), Managed(provider("third"))))

	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	want := []string{"first", "second", "third"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("start order = %v, want %v", order, want)
	}
}

func TestConcurrentStartupStartsComponentsTogether(t *testing.T) {
	firstRelease := make(chan struct{})
	secondStarted := make(chan struct{})
	first := testProvider{component: NewComponent(
		WithName("first"),
		WithOnStart(func(ctx context.Context) error {
			<-firstRelease
			return nil
		}),
	)}
	second := testProvider{component: NewComponent(
		WithName("second"),
		WithOnStart(func(ctx context.Context) error {
			close(secondStarted)
			return nil
		}),
	)}
	a := New(context.Background(), WithSignalHandling(false), WithConcurrentStartup(Managed(first), Managed(second)))
	startErr := startApp(a)

	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second component did not start while first component was still starting")
	}

	select {
	case err := <-startErr:
		t.Fatalf("Start returned before first component completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(firstRelease)
	select {
	case err := <-startErr:
		if err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return")
	}
}

func TestStartupFailureStopsOnlyInitializedComponents(t *testing.T) {
	expectedErr := errors.New("boom")
	var stopped []string
	provider := func(name string, startErr error) testProvider {
		return testProvider{component: NewComponent(
			WithName(name),
			WithOnStart(func(ctx context.Context) error { return startErr }),
			WithOnStop(func(ctx context.Context) error {
				stopped = append(stopped, name)
				return nil
			}),
		)}
	}
	a := New(context.Background(), WithSignalHandling(false), WithComponent(provider("first", nil)), WithComponent(provider("second", expectedErr)), WithComponent(provider("third", nil)))

	err := a.Start(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("Start error = %v, want %v", err, expectedErr)
	}
	if !reflect.DeepEqual(stopped, []string{"first"}) {
		t.Fatalf("stopped = %v, want [first]", stopped)
	}
	select {
	case <-a.Done():
	case <-time.After(time.Second):
		t.Fatal("app context was not canceled")
	}
	a.startWG.Wait()
}

func TestConcurrentStartupFailureStopsInitializedSibling(t *testing.T) {
	initialized := make(chan struct{})
	stopCalled := false

	initializedSibling := testProvider{component: NewComponent(
		WithName("initialized"),
		WithOnStart(func(context.Context) error {
			close(initialized)
			return nil
		}),
		WithOnStop(func(context.Context) error {
			stopCalled = true
			return nil
		}),
	)}
	failing := testProvider{component: NewComponent(
		WithName("failing"),
		WithOnStart(func(context.Context) error {
			<-initialized
			return errors.New("boom")
		}),
	)}
	a := New(context.Background(), WithSignalHandling(false), WithConcurrentStartup(Managed(initializedSibling), Managed(failing)))

	if err := a.Start(context.Background()); err == nil {
		t.Fatal("Start returned nil error")
	}
	if !stopCalled {
		t.Fatal("initialized component was not stopped")
	}
}

func TestStartupRollbackStopsComponentInitializedAfterCancellation(t *testing.T) {
	releaseLateStart := make(chan struct{})
	priorStopped := false
	prior := testProvider{component: NewComponent(
		WithName("prior"),
		WithOnStart(func(context.Context) error { return nil }),
		WithOnStop(func(context.Context) error {
			priorStopped = true
			close(releaseLateStart)
			return nil
		}),
	)}

	lateStartEntered := make(chan struct{})
	lateStopCount := 0
	late := testProvider{component: NewComponent(
		WithName("late"),
		WithOnStart(func(ctx context.Context) error {
			close(lateStartEntered)
			<-ctx.Done()
			<-releaseLateStart
			return nil
		}),
		WithOnStop(func(context.Context) error {
			lateStopCount++
			return nil
		}),
	)}
	failing := testProvider{component: NewComponent(
		WithName("failing"),
		WithOnStart(func(context.Context) error {
			<-lateStartEntered
			return errors.New("boom")
		}),
	)}
	a := New(
		context.Background(),
		WithSignalHandling(false),
		WithSequentialStartup(Managed(prior)),
		WithConcurrentStartup(Managed(late), Managed(failing)),
	)

	if err := a.Start(context.Background()); err == nil {
		t.Fatal("Start returned nil error")
	}
	if !priorStopped {
		t.Fatal("prior component was not stopped")
	}
	if lateStopCount != 1 {
		t.Fatalf("late component stop count = %d, want 1", lateStopCount)
	}
}

func TestStartupFailureClosesApp(t *testing.T) {
	expectedErr := errors.New("boom")
	provider := testProvider{component: NewComponent(
		WithOnStart(func(ctx context.Context) error { return expectedErr }),
	)}
	a := New(context.Background(), WithSignalHandling(false), WithComponent(provider))

	err := a.Start(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("Start error = %v, want %v", err, expectedErr)
	}
	if err := a.Start(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("second Start error = %v, want %v", err, ErrClosed)
	}
}

func TestStartupFailureWaitsForOutstandingStartCallbacksWithTimeout(t *testing.T) {
	ready := make(chan struct{})
	blocked := testProvider{component: NewComponent(
		WithName("blocked"),
		WithOnStart(func(ctx context.Context) error {
			close(ready)
			select {}
		}),
	)}
	failing := testProvider{component: NewComponent(
		WithName("failing"),
		WithOnStart(func(ctx context.Context) error {
			<-ready
			return errors.New("boom")
		}),
	)}
	a := New(context.Background(), WithSignalHandling(false), WithStartTimeout(10*time.Millisecond), WithStopTimeout(10*time.Millisecond), WithConcurrentStartup(Managed(blocked), Managed(failing)))

	err := a.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "wait for component start callbacks") || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start error = %v, want bounded wait deadline", err)
	}
}

func TestStopUsesSeparateShutdownContext(t *testing.T) {
	type key struct{}
	shutdownCtx := context.WithValue(context.Background(), key{}, "shutdown")
	appCtx, cancel := context.WithCancel(context.Background())
	cancel()

	provider := testProvider{component: NewComponent(
		WithOnStart(func(ctx context.Context) error { return nil }),
		WithOnStop(func(ctx context.Context) error {
			if got := ctx.Value(key{}); got != "shutdown" {
				t.Fatalf("shutdown context value = %v", got)
			}
			if err := ctx.Err(); err != nil {
				t.Fatalf("shutdown context was canceled: %v", err)
			}
			return nil
		}),
	)}
	a := New(appCtx, WithSignalHandling(false), WithComponent(provider))
	a.initialized = append(a.initialized, provider.component)

	if err := a.Stop(shutdownCtx); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}

func TestStartTimeoutWhenConsumerWaitsForReadiness(t *testing.T) {
	ready := make(chan struct{})
	stopCalled := false
	provider := &readyTestProvider{ready: ready}
	provider.component = NewComponent(
		WithName("provider"),
		WithOnStart(func(ctx context.Context) error { return nil }),
		WithOnStop(func(context.Context) error {
			stopCalled = true
			return nil
		}),
	)
	consumer := testProvider{component: NewComponent(
		WithName("consumer"),
		WithOnStart(func(ctx context.Context) error {
			provider := MustGet[*readyTestProvider](ctx)
			select {
			case <-provider.Ready():
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
	)}
	a := New(
		context.Background(),
		WithSignalHandling(false),
		WithStartTimeout(10*time.Millisecond),
		WithSequentialStartup(Registered(provider), Managed(consumer)),
	)

	err := a.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("Start error = %v, want deadline exceeded", err)
	}
	if !stopCalled {
		t.Fatal("initialized component was not stopped")
	}
}

func TestStartTimeoutCancelsOnStartContext(t *testing.T) {
	ctxCanceled := make(chan struct{})
	provider := testProvider{component: NewComponent(
		WithName("blocked"),
		WithOnStart(func(ctx context.Context) error {
			<-ctx.Done()
			close(ctxCanceled)
			return nil
		}),
	)}
	a := New(context.Background(), WithSignalHandling(false), WithStartTimeout(10*time.Millisecond), WithComponent(provider))

	err := a.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("Start error = %v, want deadline exceeded", err)
	}
	select {
	case <-ctxCanceled:
	case <-time.After(time.Second):
		t.Fatal("OnStart context was not canceled")
	}
}

func TestStartReturnsErrorWhenCalledTwice(t *testing.T) {
	a := New(context.Background(), WithSignalHandling(false), WithComponent(NewComponent()))

	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if err := a.Start(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Start error = %v, want %v", err, ErrAlreadyStarted)
	}
}

func TestStartAfterCloseReturnsClosed(t *testing.T) {
	a := New(context.Background(), WithSignalHandling(false), WithComponent(NewComponent()))

	if err := a.Close(context.Background()); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if err := a.Start(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start after Close error = %v, want %v", err, ErrClosed)
	}
}

func TestStopHonorsCanceledShutdownContext(t *testing.T) {
	stopCalled := false
	provider := testProvider{component: NewComponent(
		WithOnStop(func(ctx context.Context) error {
			stopCalled = true
			return nil
		}),
	)}
	a := New(context.Background(), WithSignalHandling(false), WithComponent(provider))
	a.initialized = append(a.initialized, provider.component)
	shutdownCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := a.Stop(shutdownCtx); err == nil {
		t.Fatal("Stop returned nil error for canceled context")
	}
	if stopCalled {
		t.Fatal("OnStop was called")
	}
}

func TestStopIsIdempotentForSuccessfullyStoppedComponents(t *testing.T) {
	stopCount := 0
	provider := testProvider{component: NewComponent(
		WithOnStart(func(ctx context.Context) error { return nil }),
		WithOnStop(func(ctx context.Context) error {
			stopCount++
			return nil
		}),
	)}
	a := New(context.Background(), WithSignalHandling(false), WithComponent(provider))

	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if err := a.Stop(context.Background()); err != nil {
		t.Fatalf("first Stop returned error: %v", err)
	}
	if err := a.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop returned error: %v", err)
	}
	if stopCount != 1 {
		t.Fatalf("stop count = %d, want 1", stopCount)
	}
}

func TestStopAggregatesMultipleErrors(t *testing.T) {
	errOne := errors.New("one")
	errTwo := errors.New("two")
	provider := func(err error) testProvider {
		return testProvider{component: NewComponent(
			WithOnStart(func(ctx context.Context) error { return nil }),
			WithOnStop(func(ctx context.Context) error { return err }),
		)}
	}
	a := New(context.Background(), WithSignalHandling(false), WithComponent(provider(errOne)), WithComponent(provider(errTwo)))
	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	err := a.Stop(context.Background())
	if !errors.Is(err, errOne) || !errors.Is(err, errTwo) {
		t.Fatalf("Stop error = %v, want both errors", err)
	}
}

func TestStartTimeoutWhenOnStartBlocks(t *testing.T) {
	secondStarted := false
	provider := testProvider{component: NewComponent(
		WithName("blocked"),
		WithOnStart(func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		}),
	)}
	second := testProvider{component: NewComponent(
		WithOnStart(func(ctx context.Context) error {
			secondStarted = true
			return nil
		}),
	)}
	a := New(context.Background(), WithSignalHandling(false), WithStartTimeout(10*time.Millisecond), WithComponent(provider), WithComponent(second))

	err := a.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("Start error = %v, want deadline exceeded", err)
	}
	if secondStarted {
		t.Fatal("second component started after first component blocked")
	}
}

func TestCloseCancelsAndStops(t *testing.T) {
	stopCalled := make(chan struct{})
	provider := testProvider{component: NewComponent(
		WithOnStart(func(ctx context.Context) error { return nil }),
		WithOnStop(func(ctx context.Context) error {
			close(stopCalled)
			return nil
		}),
	)}
	a := New(context.Background(), WithSignalHandling(false), WithComponent(provider))

	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if err := a.Close(context.Background()); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	select {
	case <-a.Done():
	default:
		t.Fatal("Close did not cancel the app")
	}
	select {
	case <-stopCalled:
	default:
		t.Fatal("Close did not stop the component")
	}
}

func TestCloseHonorsContextWhileWaitingForStartCallbacks(t *testing.T) {
	a := New(context.Background(), WithSignalHandling(false))
	a.startWG.Add(1)
	defer a.startWG.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := a.Close(shutdownCtx)
	if err == nil || !strings.Contains(err.Error(), "wait for component start callbacks") || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close error = %v, want start callback wait deadline", err)
	}
}

func TestCloseStopContextIncludesAppRegistry(t *testing.T) {
	cfg := &testConfig{Name: "demo"}
	provider := testProvider{component: NewComponent(
		WithOnStart(func(ctx context.Context) error { return nil }),
		WithOnStop(func(ctx context.Context) error {
			if got := MustGet[*testConfig](ctx); got != cfg {
				t.Fatalf("registered cfg = %p, want %p", got, cfg)
			}
			if err := ctx.Err(); err != nil {
				t.Fatalf("shutdown context was canceled: %v", err)
			}
			return nil
		}),
	)}
	a := New(context.Background(), WithSignalHandling(false), WithDependency(cfg), WithComponent(provider))

	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if err := a.Close(context.Background()); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

func TestRunOnceStartsRunsAndCloses(t *testing.T) {
	initialized := make(chan struct{})
	stopCalled := make(chan struct{})
	provider := testProvider{component: NewComponent(
		WithName("runtime"),
		WithOnStart(func(ctx context.Context) error {
			close(initialized)
			return nil
		}),
		WithOnStop(func(ctx context.Context) error {
			close(stopCalled)
			return nil
		}),
	)}
	a := New(context.Background(), WithSignalHandling(false), WithComponent(provider))

	called := false
	err := a.RunOnce(context.Background(), func(ctx context.Context) error {
		called = true
		select {
		case <-initialized:
		default:
			t.Fatal("RunOnce callback ran before component initialization")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if !called {
		t.Fatal("RunOnce callback was not called")
	}
	select {
	case <-stopCalled:
	default:
		t.Fatal("RunOnce did not stop the component")
	}
}

func TestRunOnceJoinsCallbackAndShutdownErrors(t *testing.T) {
	runErr := errors.New("run")
	stopErr := errors.New("stop")
	provider := testProvider{component: NewComponent(
		WithOnStart(func(ctx context.Context) error { return nil }),
		WithOnStop(func(ctx context.Context) error { return stopErr }),
	)}
	a := New(context.Background(), WithSignalHandling(false), WithComponent(provider))

	err := a.RunOnce(context.Background(), func(ctx context.Context) error { return runErr })
	if !errors.Is(err, runErr) || !errors.Is(err, stopErr) {
		t.Fatalf("RunOnce error = %v, want run and stop errors", err)
	}
}

func TestStopAfterRunOnceDoesNotStopTwice(t *testing.T) {
	stopCount := 0
	provider := testProvider{component: NewComponent(
		WithOnStart(func(ctx context.Context) error { return nil }),
		WithOnStop(func(ctx context.Context) error {
			stopCount++
			return nil
		}),
	)}
	a := New(context.Background(), WithSignalHandling(false), WithComponent(provider))

	if err := a.RunOnce(context.Background(), nil); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if err := a.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if stopCount != 1 {
		t.Fatalf("stop count = %d, want 1", stopCount)
	}
}

func TestWithDependencyAndRegisteredComponent(t *testing.T) {
	cfg := &testConfig{Name: "demo"}
	provider := testProvider{component: NewComponent()}
	a := New(context.Background(), WithSignalHandling(false), WithDependency(cfg), WithRegisteredComponent(provider))

	gotCfg := MustGet[*testConfig](a.Context())
	if gotCfg != cfg {
		t.Fatalf("registered cfg = %p, want %p", gotCfg, cfg)
	}
	gotProvider := MustGet[testProvider](a.Context())
	if gotProvider.component != provider.component {
		t.Fatal("registered component dependency mismatch")
	}
	if len(a.startupGroups) != 1 || len(a.startupGroups[0]) != 1 {
		t.Fatalf("startup groups = %#v, want one component group", a.startupGroups)
	}
}

func TestRegisteredStartupItemRegistersDependency(t *testing.T) {
	provider := testProvider{component: NewComponent()}
	a := New(context.Background(), WithSignalHandling(false), WithSequentialStartup(Registered(provider)))

	gotProvider := MustGet[testProvider](a.Context())
	if gotProvider.component != provider.component {
		t.Fatal("registered startup item dependency mismatch")
	}
	if len(a.startupGroups) != 1 || len(a.startupGroups[0]) != 1 {
		t.Fatalf("startup groups = %#v, want one component group", a.startupGroups)
	}
}

func TestRuntimeContextIncludesAppDependencies(t *testing.T) {
	cfg := &testConfig{Name: "demo"}
	a := New(context.Background(), WithSignalHandling(false), WithDependency(cfg))

	runtime := MustGet[RuntimeContext](a.Context())
	if got := MustGet[*testConfig](runtime); got != cfg {
		t.Fatalf("runtime cfg = %p, want %p", got, cfg)
	}
}

func TestComponentImplementsProvider(t *testing.T) {
	component := NewComponent(WithName("inline"))
	a := New(context.Background(), WithSignalHandling(false), WithComponent(component))

	if len(a.startupGroups) != 1 || len(a.startupGroups[0]) != 1 || a.startupGroups[0][0] != component {
		t.Fatalf("startup groups = %#v, want inline component", a.startupGroups)
	}
}

func startApp(a *App) <-chan error {
	errCh := make(chan error, 1)
	go func() { errCh <- a.Start(context.Background()) }()
	return errCh
}
