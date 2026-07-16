package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

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
		app.WithName("web server"),
		app.WithOnStart(s.Start),
		app.WithOnStop(s.Stop),
	)
}

func (s *Server) Start(ctx context.Context) error {
	s.cfg = app.MustGet[*Config](ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "hello from go-app")
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	listener, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	s.server = &http.Server{Handler: mux}
	go func() {
		if err := s.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("web server failed: %v", err)
		}
	}()

	log.Printf("listening on http://%s", listener.Addr())
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	a := app.New(
		context.Background(),
		app.WithDependency(&Config{Addr: addr}),
		app.WithSequentialStartup(app.Registered(NewServer())),
		app.WithStopTimeout(10*time.Second),
	)

	if err := a.Run(); err != nil {
		log.Fatal(err)
	}
}
