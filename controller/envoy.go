package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Envoy manages the Envoy process and talks to its admin API.
type Envoy struct {
	srv       ServerSettings
	xdsDir    string
	accessLog string
	cmd       *exec.Cmd
	exited    chan error
	client    *http.Client
}

func (e *Envoy) adminURL(p string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", e.srv.EnvoyAdminPort, p)
}

// bootstrap is static: one listener whose routes come from rds.json, clusters from cds.json.
func (e *Envoy) bootstrap() map[string]any {
	str := func(s string) string { return s }
	accessLogFormat := map[string]any{
		"start_time":       "%START_TIME%",
		"request_id":       "%REQ(X-REQUEST-ID)%",
		"method":           "%REQ(:METHOD)%",
		"path":             "%REQ(X-ENVOY-ORIGINAL-PATH?:PATH)%",
		"protocol":         "%PROTOCOL%",
		"status":           "%RESPONSE_CODE%",
		"response_flags":   "%RESPONSE_FLAGS%",
		"bytes_in":         "%BYTES_RECEIVED%",
		"bytes_out":        "%BYTES_SENT%",
		"duration_ms":      "%DURATION%",
		"upstream_ms":      "%RESP(X-ENVOY-UPSTREAM-SERVICE-TIME)%",
		"upstream_host":    "%UPSTREAM_HOST%",
		"upstream_cluster": "%UPSTREAM_CLUSTER%",
		"route":            "%ROUTE_NAME%",
		"attempts":         "%UPSTREAM_REQUEST_ATTEMPT_COUNT%",
		"downstream":       "%DOWNSTREAM_REMOTE_ADDRESS%",
		"config_version":   "%REQ(X-MB-CONFIG-VERSION)%",
	}
	hcm := map[string]any{
		"@type":               "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
		"stat_prefix":         "ingress",
		"codec_type":          "AUTO",
		"generate_request_id": true,
		"rds": map[string]any{
			"route_config_name": routeConfig,
			"config_source":     pathSource(filepath.Join(e.xdsDir, "rds.json"), e.xdsDir),
		},
		"http_filters": []any{map[string]any{
			"name":         "envoy.filters.http.router",
			"typed_config": map[string]any{"@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router"},
		}},
		"access_log": []any{map[string]any{
			"name": "envoy.access_loggers.file",
			"typed_config": map[string]any{
				"@type":      "type.googleapis.com/envoy.extensions.access_loggers.file.v3.FileAccessLog",
				"path":       e.accessLog,
				"log_format": map[string]any{"json_format": accessLogFormat},
			},
		}},
	}
	return map[string]any{
		"node":  map[string]any{"id": "multi-backend", "cluster": "multi-backend"},
		"admin": map[string]any{"address": socketAddr("127.0.0.1", e.srv.EnvoyAdminPort)},
		"static_resources": map[string]any{
			"listeners": []any{map[string]any{
				"name":    "ingress",
				"address": socketAddr("0.0.0.0", e.srv.ProxyPort),
				"filter_chains": []any{map[string]any{"filters": []any{map[string]any{
					"name": "envoy.filters.network.http_connection_manager", "typed_config": hcm,
				}}}},
			}},
		},
		"dynamic_resources": map[string]any{"cds_config": pathSource(filepath.Join(e.xdsDir, str("cds.json")), e.xdsDir)},
	}
}

func (e *Envoy) Start() error {
	e.client = &http.Client{Timeout: 3 * time.Second}
	path := filepath.Join(e.srv.StateDir, "envoy-bootstrap.json")
	data, _ := json.MarshalIndent(e.bootstrap(), "", "  ")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	bin := os.Getenv("ENVOY_BIN")
	if bin == "" {
		bin = "envoy"
	}
	e.cmd = exec.Command(bin, "-c", path, "--concurrency", strconv.Itoa(e.srv.EnvoyConcurrency),
		"--log-level", e.srv.EnvoyLogLevel, "--drain-time-s", strconv.Itoa(e.srv.DrainSeconds),
		"--drain-strategy", "immediate", "--file-flush-interval-msec", "100")
	stdout, _ := e.cmd.StdoutPipe()
	e.cmd.Stderr = e.cmd.Stdout
	if err := e.cmd.Start(); err != nil {
		return err
	}
	go pipeLines(stdout, map[string]any{"type": "envoy_log"})
	e.exited = make(chan error, 1)
	go func() { e.exited <- e.cmd.Wait() }()
	logEvent("info", "envoy started", "pid", e.cmd.Process.Pid, "proxy_port", e.srv.ProxyPort)
	return nil
}

// WaitReady blocks until Envoy's admin reports LIVE.
func (e *Envoy) WaitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-e.exited:
			e.exited <- err // keep it observable for main
			return fmt.Errorf("envoy exited during startup: %v", err)
		default:
		}
		resp, err := e.client.Get(e.adminURL("/ready"))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("envoy not ready after %s", timeout)
}

// Stop drains Envoy gracefully: fail health checks, stop accepting, then terminate.
func (e *Envoy) Stop() {
	if e.cmd == nil || e.cmd.Process == nil {
		return
	}
	req, _ := http.NewRequest("POST", e.adminURL("/drain_listeners?graceful"), nil)
	if resp, err := e.client.Do(req); err == nil {
		resp.Body.Close()
	}
	select {
	case <-e.exited:
		return
	case <-time.After(time.Duration(e.srv.DrainSeconds) * time.Second):
	}
	_ = e.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-e.exited:
	case <-time.After(5 * time.Second):
		_ = e.cmd.Process.Kill()
	}
}

func (e *Envoy) Get(path string) ([]byte, int, error) {
	resp, err := e.client.Get(e.adminURL(path))
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return data, resp.StatusCode, err
}

func (e *Envoy) Post(path string) error {
	resp, err := e.client.Post(e.adminURL(path), "text/plain", nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// appliedVersions reads the xDS versions Envoy has accepted from its *.version_text stats.
func (e *Envoy) appliedVersions() (cds, eds, rds string, err error) {
	data, code, err := e.Get("/stats?filter=version_text$")
	if err != nil {
		return
	}
	if code != 200 {
		err = fmt.Errorf("stats status %d", code)
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		k, v, ok := strings.Cut(line, ": ")
		if !ok {
			continue
		}
		v = strings.Trim(v, "\"")
		switch k {
		case "cluster_manager.cds.version_text":
			cds = v
		case "cluster." + poolCluster + ".version_text":
			eds = v
		case "http.ingress.rds." + routeConfig + ".version_text":
			rds = v
		}
	}
	return
}

// WaitApplied waits until Envoy reports the given version for clusters, endpoints and routes.
func (e *Envoy) WaitApplied(ctx context.Context, version int64) error {
	want := strconv.FormatInt(version, 10)
	var cds, eds, rds string
	var err error
	for {
		cds, eds, rds, err = e.appliedVersions()
		if err == nil && cds == want && eds == want && rds == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("envoy did not apply version %s (cds=%s eds=%s rds=%s err=%v)", want, cds, eds, rds, err)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// pipeLines wraps each output line of a child process as a JSON log record.
func pipeLines(r io.Reader, base map[string]any) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		m := map[string]any{"line": sc.Text()}
		for k, v := range base {
			m[k] = v
		}
		emit(m)
	}
}

// poolHostsHealthy returns, for every host currently in the "pool" cluster, whether Envoy
// considers it healthy (no failure flags set).
func (e *Envoy) poolHostsHealthy() (map[string]bool, error) {
	data, code, err := e.Get("/clusters?format=json")
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("clusters status %d", code)
	}
	var cl struct {
		ClusterStatuses []struct {
			Name         string `json:"name"`
			HostStatuses []struct {
				Address struct {
					SocketAddress struct {
						Address   string `json:"address"`
						PortValue int    `json:"port_value"`
					} `json:"socket_address"`
				} `json:"address"`
				HealthStatus map[string]any `json:"health_status"`
			} `json:"host_statuses"`
		} `json:"cluster_statuses"`
	}
	if err := json.Unmarshal(data, &cl); err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, c := range cl.ClusterStatuses {
		if c.Name != poolCluster {
			continue
		}
		for _, h := range c.HostStatuses {
			healthy := true
			for _, v := range h.HealthStatus {
				if b, ok := v.(bool); ok && b {
					healthy = false
				}
			}
			out[fmt.Sprintf("%s:%d", h.Address.SocketAddress.Address, h.Address.SocketAddress.PortValue)] = healthy
		}
	}
	return out, nil
}

// WaitPoolHealthy waits until every host in want is in the pool and healthy.
func (e *Envoy) WaitPoolHealthy(ctx context.Context, want []string) error {
	for {
		hosts, err := e.poolHostsHealthy()
		missing := []string{}
		for _, h := range want {
			if err != nil || !hosts[h] {
				missing = append(missing, h)
			}
		}
		if len(missing) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("not yet healthy in envoy: %v", missing)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
