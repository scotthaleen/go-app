package app

import (
	"context"
	"fmt"
	"reflect"
)

type ctxKey[T any] struct{}

// Register stores val by its exact type T. It is a low-level helper for app
// assembly and lifecycle boundaries; most applications should use
// WithDependency or Registered. Registering T again shadows the previous value.
func Register[T any](ctx context.Context, val T) context.Context {
	return context.WithValue(ctx, ctxKey[T]{}, val)
}

// Get returns the value registered for the exact type T.
func Get[T any](ctx context.Context) (T, bool) {
	v, ok := ctx.Value(ctxKey[T]{}).(T)
	return v, ok
}

// MustGet returns the value registered for the exact type T and panics if no
// value is registered.
func MustGet[T any](ctx context.Context) T {
	v, ok := Get[T](ctx)
	if !ok {
		panic(fmt.Sprintf("app: no value of type %v in context", typeName[T]()))
	}
	return v
}

func typeName[T any]() reflect.Type {
	var zero T
	t := reflect.TypeOf(zero)
	if t != nil {
		return t
	}
	return reflect.TypeFor[T]()
}
