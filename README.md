# go-app

`go-app` is a small Go application lifecycle manager with a typed app-scoped registry.

It is for organizing application startup, readiness, shutdown, and app-layer dependency assembly without becoming a dependency injection framework.

## Install

```sh
go get github.com/scotthaleen/go-app
```

Import it as package `app`:

```go
import "github.com/scotthaleen/go-app"
```

## Development

This repo uses [Task](https://taskfile.dev/) for local commands and [gofumpt](https://github.com/mvdan/gofumpt) for stricter formatting.

```sh
task fmt
task test
task vet
task check
```

`task fmt` runs `gofumpt` through `go run`, so `task` is the only extra command expected locally.

## Agent Skill

`SKILL.md` is platform-agnostic guidance for coding agents using this library. It
teaches the lifecycle, registry, runtime-context, and provider-owned readiness
contracts without depending on a specific agent runner or configuration format.

## Goals

- App-scoped root context for cancellation and shutdown.
- Typed registry for app-level dependencies.
- Explicit component registration.
- Sequential startup in caller-defined order by default.
- Concurrent startup groups for independent components.
- Provider-owned readiness coordination between components.
- Reverse-order shutdown.
- Separate runtime and shutdown contexts.
- Signal handling for `SIGINT` and `SIGTERM` by default.

## Non-Goals

- No dependency graph solver.
- No reflection-based auto-wiring.
- No constructor graph execution.
- No global registry or default app.
- No named values or value groups.
- No built-in health, metrics, tracing, config, or settings backend.

Settings/configuration are application-defined dependencies. For example, populate a `Config` from Cobra, Viper, environment variables, or files, register it with `app.WithDependency(cfg)`, and let components read it during `Start(ctx)`.

Apps should expose health, metrics, and tracing through ordinary components. For example, an HTTP component can expose `/health` or `/metrics`, and a worker component can record domain metrics with the app's chosen telemetry stack.

## Basic Usage

```go
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"

	"github.com/scotthaleen/go-app"
)

type Config struct {
	Addr string
}

type Server struct {
	cfg    *Config
	server *http.Server
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Component() *app.Component {
	return app.NewComponent(
		app.WithName("server"),
		app.WithOnStart(s.Start),
		app.WithOnStop(s.Stop),
	)
}

func (s *Server) Start(ctx context.Context) error {
	s.cfg = app.MustGet[*Config](ctx)
	s.server = &http.Server{Handler: http.DefaultServeMux}

	listener, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}

	go func() {
		if err := s.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("server failed: %v", err)
		}
	}()
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func main() {
	cfg := &Config{Addr: ":8080"}
	server := NewServer()

	a := app.New(context.Background(),
		app.WithDependency(cfg),
		app.WithSequentialStartup(app.Registered(server)),
	)

	if err := a.Run(); err != nil {
		log.Fatal(err)
	}
}
```

This server does not retain the startup context; graceful runtime cleanup happens in `Stop`. Components that start goroutines needing app-lifetime cancellation should resolve `app.RuntimeContext` during `Start` and pass that context to the goroutine.

## Dependency Lookup Guidance

Use `app.Get` and `app.MustGet` at lifecycle boundaries:

- `Start(ctx)` methods.
- `Stop(ctx)` methods when cleanup needs app-scoped state.
- Top-level app assembly callbacks.

Avoid registry lookups in:

- Business/domain logic.
- Stores and query methods.
- HTTP handlers after construction.
- Deep helper functions.
- Reusable libraries.

The preferred pattern is to resolve dependencies in `Start(ctx)`, assign them to component fields, then use those fields during runtime.

Registry values are keyed by their exact Go type. Registering the same type more than once replaces the previous value; named dependencies and value groups are intentionally not supported. `app.Register` is the low-level context assembly primitive. Prefer `WithDependency`, `Registered`, `Get`, and `MustGet` for normal application wiring.

## Startup Shape

Most apps can use sequential startup. `WithSequentialStartup` starts each item one at a time in the order provided:

```go
a := app.New(context.Background(),
	app.WithDependency(cfg),
	app.WithSequentialStartup(
		app.Registered(db),
		app.Registered(server),
	),
)
```

Use `app.Registered(provider)` when the component should also be available through `app.Get` or `app.MustGet`. Use `app.Managed(provider)` for lifecycle-only components.

When independent components have expensive startup work, use `WithConcurrentStartup` to start them together. Startup continues after every `OnStart` callback in the group returns successfully:

```go
a := app.New(context.Background(),
	app.WithDependency(cfg),
	app.WithConcurrentStartup(
		app.Registered(db),
		app.Registered(nats),
		app.Registered(model),
	),
	app.WithSequentialStartup(
		app.Registered(api),
	),
)
```

This is a caller-owned coarse dependency shape, not a graph solver. If one component needs another component's stronger readiness contract, it should wait on that component's provider-owned readiness from its own `Start` method. Components that do not care about that stronger readiness can start without waiting.

## Component Contract

`OnStart` is always called in a goroutine. Startup progression and operational readiness are separate concepts:

- A component is initialized when `OnStart` returns nil. Startup may advance, and the app owns calling `OnStop` during shutdown or rollback.
- A provider is ready when it satisfies its own operational readiness contract. The app does not wait for provider readiness automatically.

`Start` callbacks should finish setup, start runtime goroutines or resources, then return. `Stop` callbacks own graceful shutdown for the resources started by `Start`.

Use a meaningful, unique `WithName` value for useful logs and errors. Unnamed components all use the fallback name `"component"`.

The `Start(ctx)` context is a startup control context. It carries app registry values and startup cancellation/deadline. Runtime goroutines that need app-lifetime cancellation should resolve `app.RuntimeContext` during startup and use that context instead of retaining the startup context.

When initialization itself provides the required guarantee, return only after setup succeeds:

```go
func (r *Routes) Component() *app.Component {
	return app.NewComponent(
		app.WithName("routes"),
		app.WithOnStart(r.Start),
	)
}

func (r *Routes) Start(ctx context.Context) error {
	router := app.MustGet[*Router](ctx)
	router.Handle("/health", r.health)
	return nil
}
```

When readiness is asynchronous, expose one shared provider-owned signal:

```go
type Database struct {
	ready chan struct{}
}

func NewDatabase() *Database {
	return &Database{ready: make(chan struct{})}
}

func (d *Database) Component() *app.Component {
	return app.NewComponent(
		app.WithName("database"),
		app.WithOnStart(d.Start),
		app.WithOnStop(d.Stop),
	)
}

func (d *Database) Ready() <-chan struct{} { return d.ready }

func (d *Database) Start(ctx context.Context) error {
	runtime := app.MustGet[app.RuntimeContext](ctx)
	go d.probeUntilReady(runtime)
	return nil
}

func (a *API) Start(ctx context.Context) error {
	database := app.MustGet[*Database](ctx)
	select {
	case <-database.Ready():
		a.database = database
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
```

Multiple consumers can wait on the same `Database.Ready()` channel. Put consumers that may initialize concurrently in the same startup group so unrelated consumers can proceed while dependent consumers wait. The startup timeout still bounds readiness waits performed inside consumer `OnStart` callbacks.

Avoid putting business loops, request processing, or ordinary runtime work directly in `Start`. If a component starts a server, worker, subscription, or ticker, put that runtime work in a goroutine or resource owned by the component and shut it down from `Stop`.

## Inline Components

Not every dependency needs a named struct with a formal component type. For small setup steps, checks, short-lived command dependencies, or one-off subscriptions, use `app.NewComponent` directly:

```go
var db *sql.DB

database := app.NewComponent(
	app.WithName("database"),
	app.WithOnStart(func(ctx context.Context) error {
		var err error
		db, err = sql.Open("sqlite", path)
		if err != nil {
			return err
		}
		return db.PingContext(ctx)
	}),
	app.WithOnStop(func(ctx context.Context) error {
		if db == nil {
			return nil
		}
		return db.Close()
	}),
)

a := app.New(context.Background(),
	app.WithSignalHandling(false),
	app.WithSequentialStartup(app.Managed(database)),
)
```

`*app.Component` implements `app.Provider`, so inline components can be passed directly to `app.Managed`. Use a formal `Provider` type when the dependency has state, methods, or will be reused. Use an inline component when the lifecycle boundary is the main abstraction.

## Short-Lived Commands

CLI commands often need the same lifecycle guarantees as services but should not block in `Run`. Use `RunOnce` to start the app, run command work, and close the app:

```go
ctx := cmd.Context()
a := app.New(ctx,
	app.WithSignalHandling(false),
	app.WithSequentialStartup(app.Registered(store)),
)

return a.RunOnce(ctx, func(ctx context.Context) error {
	return runCommand(store.DB())
})
```

This pattern is especially useful for rapid CLI feature work: add a dependency component, start the app at the command boundary, and keep business logic taking ordinary typed values such as `*sql.DB`. The context passed to `RunOnce` controls startup; the callback receives the app context. Construct the app from the command context, as above, when command cancellation should also cancel callback work. If you need custom shutdown control, call `Start` and `Close` directly. `Close` uses the supplied context for shutdown cancellation and deadlines while preserving access to app-registered dependencies for `OnStop` callbacks.

Components that need to request application shutdown, such as HTTP `/shutdown` handlers or TUI quit commands, can resolve `app.RequestShutdownFunc` during startup and call it later. Requesting shutdown cancels the app context; `Run` or `Close` performs component cleanup.

## Lifecycle

Startup:

- Components start in caller-defined startup order.
- Sequential startup items start one at a time.
- Concurrent startup items in the same group start together.
- Each component `OnStart` runs in a goroutine.
- `Start(ctx)` receives the startup control context, including startup timeout/cancellation.
- Startup groups advance after every `OnStart` callback in the group returns nil.
- Provider readiness is not a global barrier; consumers opt in by waiting on provider-owned readiness from their own `OnStart` callbacks.
- If startup fails, the app context is canceled and the app is closed.
- Every initialized component whose `OnStart` returned nil is stopped during rollback.
- If a canceled `OnStart` returns nil while rollback is waiting for callbacks, that component is also stopped before rollback returns.
- Outstanding `OnStart` callbacks are waited for with the shutdown context.

Shutdown:

- `RequestShutdown()` requests shutdown by canceling the app context; it does not stop components by itself.
- `Stop(ctx)` stops components only; it does not request shutdown or cancel `RuntimeContext`. Use `Close(ctx)` for full shutdown, or call `RequestShutdown()` before `Stop(ctx)`.
- Only initialized components whose `OnStart` returned nil are stopped.
- Components stop in reverse registration order. For concurrent groups, that means reverse group order and reverse item order within each group.
- Shutdown uses a context derived from `context.WithoutCancel(app.Context())`.
- Stop errors are joined with `errors.Join`.
- `Close(ctx)` requests shutdown, stops initialized components with `ctx`, and waits with `ctx` for component `OnStart` goroutines to return. Use it after direct `Start` calls in short-lived commands.
- `RunOnce(ctx, fn)` starts the app, runs `fn` with the app context, closes the app, and joins callback/shutdown errors.

Lifecycle methods must not be called concurrently on the same app. This includes `Start`, `Run`, `RunOnce`, `Stop`, and `Close`.

The configured startup timeout covers the entire application startup phase, not each component independently. The configured stop timeout covers the entire internally managed shutdown phase used by `Run`, `RunOnce`, and startup rollback. Direct `Stop(ctx)` and `Close(ctx)` calls are controlled by the context supplied by the caller.

## Options

- `app.WithDependency(value)` registers an app-scoped dependency.
- `app.Managed(provider)` creates a lifecycle-only startup item.
- `app.Registered(provider)` creates a startup item and registers the provider as an app-scoped dependency.
- `app.RuntimeContext` is registered automatically for app-lifetime runtime goroutines.
- `app.WithSequentialStartup(items...)` starts each item one at a time.
- `app.WithConcurrentStartup(items...)` starts all items together before continuing.
- `app.WithComponent(provider)` is shorthand for sequential lifecycle-only startup.
- `app.WithRegisteredComponent(provider)` is shorthand for sequential startup that also registers the provider as a dependency.
- `app.WithStartTimeout(duration)` changes the whole-app startup timeout; use `0` to disable.
- `app.WithStopTimeout(duration)` changes the internally managed whole-app shutdown timeout; use `0` to disable.
- `app.WithSignalHandling(false)` disables default signal handling.
- `app.WithLogger(logger)` sets the `log/slog` logger.

## Examples

- `examples/web` starts an HTTP server, loads `Config{Addr string}` from `ADDR`, and performs graceful shutdown.
- `examples/advanced-web` runs an HTTP API plus a background task manager and shows cascading shutdown through `/shutdown`.
- `examples/cli` shows a short-running command still using start/stop lifecycle boundaries.
- `examples/tui` shows an interactive shell that uses app cancellation for shutdown.

Run them with:

```sh
go run ./examples/web
go run ./examples/advanced-web
go run ./examples/cli Scott
go run ./examples/tui
```

Try the advanced web example with:

```sh
curl -X POST http://localhost:8081/tasks -d '{"name":"demo"}'
curl http://localhost:8081/tasks
curl -X POST http://localhost:8081/shutdown
```

The examples keep settings/config application-owned: each app defines a small `Config`, populates it at the application boundary, registers it with `app.WithDependency`, and resolves it during startup.
