package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/scotthaleen/go-app"
)

type Config struct {
	Addr string
}

type Task struct {
	ID        int       `json:"id"`
	Name      string    `json:"name"`
	Ticks     int       `json:"ticks"`
	Running   bool      `json:"running"`
	CreatedAt time.Time `json:"created_at"`
	StoppedAt time.Time `json:"stopped_at,omitempty"`
}

type TaskManager struct {
	mu      sync.Mutex
	nextID  int
	tasks   map[int]*Task
	cancels map[int]context.CancelFunc
	wg      sync.WaitGroup
}

func NewTaskManager() *TaskManager {
	return &TaskManager{
		tasks:   make(map[int]*Task),
		cancels: make(map[int]context.CancelFunc),
	}
}

func (m *TaskManager) Component() *app.Component {
	return app.NewComponent(
		app.WithName("task manager"),
		app.WithOnStart(m.Start),
		app.WithOnStop(m.Stop),
	)
}

func (m *TaskManager) Start(ctx context.Context) error {
	return nil
}

func (m *TaskManager) Stop(ctx context.Context) error {
	m.mu.Lock()
	for _, cancel := range m.cancels {
		cancel()
	}
	m.mu.Unlock()

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *TaskManager) StartTask(parent context.Context, name string) Task {
	m.mu.Lock()
	m.nextID++
	id := m.nextID
	task := &Task{ID: id, Name: name, Running: true, CreatedAt: time.Now()}
	taskCtx, cancel := context.WithCancel(parent)
	m.tasks[id] = task
	m.cancels[id] = cancel
	m.wg.Add(1)
	m.mu.Unlock()

	go m.run(taskCtx, id)
	return *task
}

func (m *TaskManager) StopTask(id int) bool {
	m.mu.Lock()
	cancel, ok := m.cancels[id]
	m.mu.Unlock()
	if !ok {
		return false
	}
	cancel()
	return true
}

func (m *TaskManager) ListTasks() []Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	tasks := make([]Task, 0, len(m.tasks))
	for _, task := range m.tasks {
		tasks = append(tasks, *task)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })
	return tasks
}

func (m *TaskManager) run(ctx context.Context, id int) {
	defer m.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.mu.Lock()
			if task := m.tasks[id]; task != nil {
				task.Running = false
				task.StoppedAt = time.Now()
			}
			delete(m.cancels, id)
			m.mu.Unlock()
			return
		case <-ticker.C:
			m.mu.Lock()
			if task := m.tasks[id]; task != nil {
				task.Ticks++
			}
			m.mu.Unlock()
		}
	}
}

type Server struct {
	cfg    *Config
	tasks  *TaskManager
	server *http.Server
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Component() *app.Component {
	return app.NewComponent(
		app.WithName("api server"),
		app.WithOnStart(s.Start),
		app.WithOnStop(s.Stop),
	)
}

func (s *Server) Start(ctx context.Context) error {
	s.cfg = app.MustGet[*Config](ctx)
	s.tasks = app.MustGet[*TaskManager](ctx)
	runtime := app.MustGet[app.RuntimeContext](ctx)
	requestShutdown := app.MustGet[app.RequestShutdownFunc](ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /tasks", s.handleListTasks)
	mux.HandleFunc("POST /tasks", s.handleStartTask(runtime))
	mux.HandleFunc("DELETE /tasks/", s.handleStopTask)
	mux.HandleFunc("POST /shutdown", s.handleShutdown(requestShutdown))
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
			log.Printf("api server failed: %v", err)
		}
	}()

	log.Printf("advanced web example listening on http://%s", listener.Addr())
	log.Printf("try: curl -X POST http://%s/tasks -d '{\"name\":\"demo\"}'", listener.Addr())
	log.Printf("then: curl http://%s/tasks", listener.Addr())
	log.Printf("stop app: curl -X POST http://%s/shutdown", listener.Addr())
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.tasks.ListTasks())
}

func (s *Server) handleStartTask(ctx context.Context) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		req.Name = strings.TrimSpace(req.Name)
		if req.Name == "" {
			req.Name = "task"
		}
		writeJSON(w, http.StatusCreated, s.tasks.StartTask(ctx, req.Name))
	}
}

func (s *Server) handleStopTask(w http.ResponseWriter, r *http.Request) {
	idText := strings.TrimPrefix(r.URL.Path, "/tasks/")
	id, err := strconv.Atoi(idText)
	if err != nil {
		http.Error(w, "invalid task id", http.StatusBadRequest)
		return
	}
	if !s.tasks.StopTask(id) {
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleShutdown(requestShutdown app.RequestShutdownFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "shutting down"})
		go requestShutdown()
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8081"
	}

	a := app.New(
		context.Background(),
		app.WithDependency(&Config{Addr: addr}),
		app.WithSequentialStartup(
			app.Registered(NewTaskManager()),
			app.Managed(NewServer()),
		),
		app.WithStopTimeout(10*time.Second),
	)

	if err := a.Run(); err != nil {
		log.Fatal(err)
	}
}
