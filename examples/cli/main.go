package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/scotthaleen/go-app"
)

type Config struct {
	Name string
}

type Greeter struct {
	cfg *Config
}

func (g *Greeter) Component() *app.Component {
	return app.NewComponent(
		app.WithName("greeter"),
		app.WithOnStart(g.Start),
		app.WithOnStop(g.Stop),
	)
}

func (g *Greeter) Start(ctx context.Context) error {
	g.cfg = app.MustGet[*Config](ctx)
	return nil
}

func (g *Greeter) Stop(ctx context.Context) error {
	cfg := app.MustGet[*Config](ctx)
	fmt.Printf("cleanup complete for %s\n", cfg.Name)
	return nil
}

func (g *Greeter) Greet() {
	fmt.Printf("hello, %s\n", g.cfg.Name)
}

func main() {
	name := "world"
	if len(os.Args) > 1 {
		name = os.Args[1]
	}

	greeter := &Greeter{}
	a := app.New(
		context.Background(),
		app.WithSignalHandling(false),
		app.WithDependency(&Config{Name: name}),
		app.WithSequentialStartup(app.Registered(greeter)),
	)

	if err := a.RunOnce(context.Background(), func(ctx context.Context) error {
		greeter.Greet()
		return nil
	}); err != nil {
		log.Fatal(err)
	}
}
