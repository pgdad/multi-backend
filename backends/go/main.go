// Sample Go backend (stdlib only). Same contract as the other sample backends:
// GET /health -> 200; anything else -> JSON echo. Query knobs: delay_ms[.<name>], status[.<name>].
package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

var name = env("BACKEND_NAME", "go")

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func knob(r *http.Request, key string, def int) int {
	q := r.URL.Query()
	for _, k := range []string{key + "." + name, key} {
		if v := q.Get(k); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				return n
			}
		}
	}
	return def
}

func main() {
	port := env("PORT", "8002")
	mux := http.NewServeMux()
	reply := func(w http.ResponseWriter, status int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Backend-Name", name)
		w.Header().Set("X-Backend-Runtime", "go")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		reply(w, 200, map[string]any{"status": "ok", "backend": name})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		delay := knob(r, "delay_ms", 0)
		if delay > 0 {
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}
		reply(w, knob(r, "status", 200), map[string]any{
			"backend": name, "runtime": "go", "method": r.Method, "path": r.URL.Path,
			"query": r.URL.RawQuery, "body_bytes": n,
			"request_id":     r.Header.Get("X-Request-Id"),
			"config_version": r.Header.Get("X-Mb-Config-Version"),
			"fanout":         r.Header.Get("X-Mb-Fanout") == "true",
			"delay_ms":       delay, "time": time.Now().UTC().Format(time.RFC3339Nano),
		})
	})
	log.Printf("go backend %s listening on 127.0.0.1:%s", name, port)
	log.Fatal(http.ListenAndServe("127.0.0.1:"+port, mux))
}
