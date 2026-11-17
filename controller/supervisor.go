package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// Supervisor runs the backends that have a command, restarting them if they exit.
type Supervisor struct {
	mu    sync.Mutex
	procs map[string]*proc
}

type proc struct {
	backend  Backend
	cancel   context.CancelFunc
	done     chan struct{}
	mu       sync.Mutex
	cmd      *exec.Cmd
	restarts int
	started  time.Time
}

func NewSupervisor() *Supervisor { return &Supervisor{procs: map[string]*proc{}} }

func (s *Supervisor) Running(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.procs[name]
	return ok
}

// Start launches the backend process (no-op if it has no command or is already running).
func (s *Supervisor) Start(b Backend) {
	if !b.Managed() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.procs[b.Name]; ok {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &proc{backend: b, cancel: cancel, done: make(chan struct{})}
	s.procs[b.Name] = p
	go p.run(ctx)
}

func (p *proc) run(ctx context.Context) {
	defer close(p.done)
	backoff := time.Second
	for {
		b := p.backend
		cmd := exec.Command(b.Command[0], b.Command[1:]...)
		cmd.Env = append(os.Environ(), "PORT="+strconv.Itoa(b.Port), "BACKEND_NAME="+b.Name, "BACKEND_RUNTIME="+b.Runtime)
		for k, v := range b.Env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		out, _ := cmd.StdoutPipe()
		cmd.Stderr = cmd.Stdout
		start := time.Now()
		if err := cmd.Start(); err != nil {
			logEvent("error", "backend start failed", "backend", b.Name, "err", err)
		} else {
			p.mu.Lock()
			p.cmd, p.started = cmd, start
			p.mu.Unlock()
			logEvent("info", "backend started", "backend", b.Name, "runtime", b.Runtime, "pid", cmd.Process.Pid, "port", b.Port)
			go pipeLines(out, map[string]any{"type": "backend_log", "backend": b.Name})
			waitErr := make(chan error, 1)
			go func() { waitErr <- cmd.Wait() }()
			select {
			case err := <-waitErr:
				logEvent("warn", "backend exited", "backend", b.Name, "err", fmt.Sprint(err))
			case <-ctx.Done():
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
				select {
				case <-waitErr:
				case <-time.After(5 * time.Second):
					_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
					<-waitErr
				}
				logEvent("info", "backend stopped", "backend", b.Name)
				return
			}
		}
		if time.Since(start) > 30*time.Second {
			backoff = time.Second
		}
		p.mu.Lock()
		p.restarts++
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// Stop terminates a backend process and waits for it to exit.
func (s *Supervisor) Stop(name string) {
	s.mu.Lock()
	p, ok := s.procs[name]
	delete(s.procs, name)
	s.mu.Unlock()
	if ok {
		p.cancel()
		<-p.done
	}
}

func (s *Supervisor) StopAll() {
	s.mu.Lock()
	names := make([]string, 0, len(s.procs))
	for n := range s.procs {
		names = append(names, n)
	}
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, n := range names {
		wg.Add(1)
		go func(n string) { defer wg.Done(); s.Stop(n) }(n)
	}
	wg.Wait()
}

// Info reports process state for the API.
func (s *Supervisor) Info(name string) map[string]any {
	s.mu.Lock()
	p, ok := s.procs[name]
	s.mu.Unlock()
	if !ok {
		return map[string]any{"managed": false}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	info := map[string]any{"managed": true, "restarts": p.restarts}
	if p.cmd != nil && p.cmd.Process != nil {
		info["pid"] = p.cmd.Process.Pid
		info["uptime_s"] = int(time.Since(p.started).Seconds())
	}
	return info
}

// waitHealthy polls the backend's health endpoint until it returns 2xx.
func waitHealthy(ctx context.Context, b Backend, path string) error {
	client := &http.Client{Timeout: time.Second}
	url := fmt.Sprintf("http://%s%s", b.HostPort(), path)
	for {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("backend %s not healthy at %s: %v", b.Name, url, err)
		case <-time.After(200 * time.Millisecond):
		}
	}
}
