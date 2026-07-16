package app

import (
	"context"
	"testing"
)

func TestGetReportsMissingDependency(t *testing.T) {
	if got, ok := Get[*testConfig](context.Background()); ok || got != nil {
		t.Fatalf("Get returned (%v, %v), want (nil, false)", got, ok)
	}
}

func TestRegisterAndGetDependency(t *testing.T) {
	cfg := &testConfig{Name: "test"}
	ctx := Register(context.Background(), cfg)

	got, ok := Get[*testConfig](ctx)
	if !ok {
		t.Fatal("Get returned ok=false")
	}
	if got != cfg {
		t.Fatalf("Get returned %p, want %p", got, cfg)
	}
}

func TestMustGetPanicsForMissingDependency(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("MustGet did not panic")
		}
	}()

	_ = MustGet[*testConfig](context.Background())
}

type testConfig struct {
	Name string
}
