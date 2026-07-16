// Package app provides a small application lifecycle manager with a typed
// app-scoped registry.
//
// Components are started in caller-defined startup order. Components whose
// OnStart returned nil are stopped in reverse registration order. Sequential
// startup starts one component at a time. Concurrent startup starts a
// caller-provided group together and stops that group in reverse item order.
// Dependency ordering is explicit and owned by the caller; this package does
// not build or solve a dependency graph.
//
// The typed registry is intended for app assembly and lifecycle boundaries.
// Resolve dependencies in component Start or Stop methods, assign them to
// fields, and keep ordinary runtime code using explicit fields and parameters.
// Values are keyed by their exact Go type; registering the same type again
// replaces the value returned by subsequent lookups.
//
// OnStart is always called in a goroutine. Startup groups advance when their
// OnStart callbacks return nil. That successful return transfers lifecycle
// ownership to the app, which calls OnStop during shutdown or rollback. Start
// callbacks should finish setup, start any runtime goroutines or resources, then
// return. Start receives a startup control context. Use RuntimeContext from the
// registry for app-lifetime runtime cancellation. If a canceled OnStart returns
// nil during the bounded rollback wait, the app still calls OnStop.
//
// Operational readiness is provider-owned rather than a global startup barrier.
// A provider may expose a shared Ready channel, and only consumers that need the
// stronger guarantee should wait on it from their own OnStart callbacks.
//
// Short-lived commands can call RunOnce, or call Start and Close directly when
// they need custom control. Close requests shutdown, stops components, and waits
// with the shutdown context for component OnStart goroutines to return.
// Direct Stop calls only stop components; they do not request shutdown or cancel
// RuntimeContext.
//
// Start, Run, RunOnce, Stop, and Close must not be called concurrently on the
// same App.
package app
