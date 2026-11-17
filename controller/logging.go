package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var stdoutMu sync.Mutex

// emit writes one JSON line to stdout. All controller output is structured JSON.
func emit(m map[string]any) {
	if _, ok := m["ts"]; !ok {
		m["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	}
	data, err := json.Marshal(m)
	if err != nil {
		return
	}
	stdoutMu.Lock()
	os.Stdout.Write(append(data, '\n'))
	stdoutMu.Unlock()
}

func logEvent(level, msg string, kv ...any) {
	m := map[string]any{"type": "event", "level": level, "msg": msg}
	for i := 0; i+1 < len(kv); i += 2 {
		if k, ok := kv[i].(string); ok {
			if e, isErr := kv[i+1].(error); isErr {
				m[k] = e.Error()
			} else {
				m[k] = kv[i+1]
			}
		}
	}
	emit(m)
}

// rotatingFile is an append-only JSON-lines file with simple size-based rotation (one .1 backup).
type rotatingFile struct {
	mu      sync.Mutex
	path    string
	maxSize int64
	f       *os.File
	size    int64
}

func newRotatingFile(path string, maxSize int64) (*rotatingFile, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	r := &rotatingFile{path: path, maxSize: maxSize}
	return r, r.open()
}

func (r *rotatingFile) open() error {
	f, err := os.OpenFile(r.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	st, _ := f.Stat()
	r.f, r.size = f, st.Size()
	return nil
}

func (r *rotatingFile) WriteLine(data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.f == nil {
		return
	}
	if r.size+int64(len(data))+1 > r.maxSize {
		r.f.Close()
		_ = os.Rename(r.path, r.path+".1")
		if err := r.open(); err != nil {
			r.f = nil
			return
		}
	}
	n, _ := r.f.Write(append(data, '\n'))
	r.size += int64(n)
}

func stdoutWrite(data []byte) { os.Stdout.Write(append(data, '\n')) }
