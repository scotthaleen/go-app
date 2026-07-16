package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/scotthaleen/go-app"
)

type Config struct {
	Prompt string
}

type Shell struct {
	in  io.Reader
	out io.Writer
	cfg *Config
}

func NewShell(in io.Reader, out io.Writer) *Shell {
	return &Shell{in: in, out: out}
}

func (s *Shell) Component() *app.Component {
	return app.NewComponent(
		app.WithName("shell"),
		app.WithOnStart(s.Start),
		app.WithOnStop(s.Stop),
	)
}

func (s *Shell) Start(ctx context.Context) error {
	s.cfg = app.MustGet[*Config](ctx)
	runtime := app.MustGet[app.RuntimeContext](ctx)
	requestShutdown := app.MustGet[app.RequestShutdownFunc](ctx)
	go s.run(runtime, requestShutdown)
	return nil
}

func (s *Shell) run(ctx context.Context, requestShutdown app.RequestShutdownFunc) {
	lines := scanLines(s.in)

	fmt.Fprintln(s.out, "simple shell; type quit to exit")

	for {
		fmt.Fprint(s.out, s.cfg.Prompt)
		select {
		case <-ctx.Done():
			return
		case line, ok := <-lines:
			if !ok {
				requestShutdown()
				return
			}
			line = strings.TrimSpace(line)
			switch line {
			case "", "help":
				fmt.Fprintln(s.out, "commands: help, quit")
			case "quit", "exit":
				requestShutdown()
				return
			default:
				fmt.Fprintf(s.out, "unknown command %q\n", line)
			}
		}
	}
}

func (s *Shell) Stop(ctx context.Context) error {
	fmt.Fprintln(s.out, "shell stopped")
	return nil
}

func scanLines(in io.Reader) <-chan string {
	lines := make(chan string)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(in)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	return lines
}

func main() {
	a := app.New(
		context.Background(),
		app.WithDependency(&Config{Prompt: "> "}),
		app.WithSequentialStartup(app.Registered(NewShell(os.Stdin, os.Stdout))),
		app.WithStopTimeout(5*time.Second),
	)

	if err := a.Run(); err != nil {
		log.Fatal(err)
	}
}
