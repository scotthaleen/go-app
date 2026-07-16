package app

import "context"

// ComponentFunc is a component startup or shutdown callback.
type ComponentFunc func(context.Context) error

// Provider exposes a lifecycle component.
type Provider interface {
	Component() *Component
}

// Component defines startup and shutdown callbacks for one application resource.
type Component struct {
	name    string
	onStart ComponentFunc
	onStop  ComponentFunc
}

// ComponentOption configures a Component.
type ComponentOption func(*Component)

// NewComponent creates a lifecycle component.
//
// Start callbacks should finish setup, start any runtime goroutines or resources,
// then return. The Start context is a startup control context; runtime goroutines
// that need app-lifetime cancellation should use RuntimeContext from the app
// registry. Providers may expose their own shared readiness signal for consumers
// that need a stronger guarantee than initialization.
func NewComponent(opts ...ComponentOption) *Component {
	c := &Component{
		onStart: noop,
		onStop:  noop,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// WithName sets the component name used in logs and errors. Prefer a meaningful,
// unique name; unnamed components use "component".
func WithName(name string) ComponentOption {
	return func(c *Component) {
		c.name = name
	}
}

// WithOnStart sets the component startup callback.
func WithOnStart(fn ComponentFunc) ComponentOption {
	return func(c *Component) {
		if fn != nil {
			c.onStart = fn
		}
	}
}

// WithOnStop sets the component shutdown callback.
func WithOnStop(fn ComponentFunc) ComponentOption {
	return func(c *Component) {
		if fn != nil {
			c.onStop = fn
		}
	}
}

// Name returns the configured name or "component" when no name was provided.
func (c *Component) Name() string {
	if c.name == "" {
		return "component"
	}
	return c.name
}

// Component implements Provider.
func (c *Component) Component() *Component { return c }

// OnStart invokes the configured startup callback.
func (c *Component) OnStart(ctx context.Context) error {
	return c.onStart(ctx)
}

// OnStop invokes the configured shutdown callback.
func (c *Component) OnStop(ctx context.Context) error {
	return c.onStop(ctx)
}

func noop(context.Context) error { return nil }
