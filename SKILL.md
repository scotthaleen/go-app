---
name: go-app
description: Build and modify Go applications using github.com/scotthaleen/go-app. Use when a Go project uses go-app for lifecycle orchestration, typed dependency registration, startup groups, provider-owned readiness, or graceful shutdown.
---

# go-app Agent Skill

Portable guidance for any coding agent building or modifying a Go application
with `github.com/scotthaleen/go-app` for lifecycle orchestration.

## Pattern

1. Define app-owned config/settings as normal Go structs.
2. Construct concrete dependencies and components in the app layer.
3. Register dependencies with `app.WithDependency`.
4. Add lifecycle components with `app.WithSequentialStartup` for ordinary ordered startup.
5. Use `app.Registered(provider)` for lifecycle plus typed registry access.
6. Use `app.Managed(provider)` for lifecycle-only components.
7. Use `app.WithConcurrentStartup` only for independent components that can safely start together.
8. For short-lived commands, prefer `a.RunOnce(ctx, fn)`.
9. Put dependency lookups in component `Start(ctx)` or `Stop(ctx)`, then store dependencies in fields.
10. Use `app.RuntimeContext` for app-lifetime runtime goroutines; do not retain startup `ctx` for runtime loops.
11. Avoid `app.MustGet` in business logic, request handlers, store methods, and deep helpers.
12. Keep `Start(ctx)` focused on setup; start runtime goroutines or resources, then return.
13. Expose provider-owned readiness signals and wait on them only in consumers that require readiness.

## Component Styles

Setup component:

```go
func (r *Routes) Component() *app.Component {
	return app.NewComponent(
		app.WithName("routes"),
		app.WithOnStart(r.Start),
	)
}

func (r *Routes) Start(ctx context.Context) error {
	router := app.MustGet[*Router](ctx)
	r.router = router
	return nil
}
```

Runtime component:

```go
func (s *Server) Component() *app.Component {
	return app.NewComponent(
		app.WithName("server"),
		app.WithOnStart(s.Start),
		app.WithOnStop(s.Stop),
	)
}

func (s *Server) Start(ctx context.Context) error {
	runtime := app.MustGet[app.RuntimeContext](ctx)
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	go s.serve(runtime, listener)
	return nil
}
```

Lifecycle ownership depends on the `OnStart` result:

- `OnStart` returns nil: the app owns cleanup and calls `OnStop` during shutdown or rollback.
- `OnStart` returns an error: the component must clean up any partial acquisitions before returning.

```go
func (s *Server) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	if err := s.configure(listener); err != nil {
		_ = listener.Close()
		return err
	}
	s.listener = listener
	return nil
}
```

For asynchronous readiness, expose one shared signal from the provider. Consumers
that need it wait from their own `Start`; unrelated components do not wait:

```go
func (s *Server) Ready() <-chan struct{} { return s.ready }

func (c *Consumer) Start(ctx context.Context) error {
	server := app.MustGet[*Server](ctx)
	select {
	case <-server.Ready():
		c.server = server
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
```

Short-lived command:

```go
a := app.New(ctx,
	app.WithSignalHandling(false),
	app.WithSequentialStartup(app.Registered(store)),
)

return a.RunOnce(ctx, func(ctx context.Context) error {
	return runCommand(store.DB())
})
```

Manual lifecycle:

```go
func run(ctx context.Context) (err error) {
	if err := a.Start(ctx); err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err = errors.Join(err, a.Close(shutdownCtx))
	}()

	return runWork(a.Context())
}
```

Use `Close` for full shutdown. Direct `Stop` only stops components; it does not request shutdown or cancel `app.RuntimeContext`.

## Do Not

- Do not add automatic dependency ordering.
- Do not hide dependencies in package globals.
- Do not use the app registry as a service locator throughout runtime code.
- Do not add framework-specific integrations to core lifecycle code.
- Do not put business or request-processing loops directly in `Start(ctx)`.
- Do not call `Start` more than once on the same app instance.
- Do not call lifecycle methods concurrently on the same app instance.

## Verify

```sh
go test ./...
go vet ./...
```
