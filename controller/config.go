package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Routing modes.
const (
	ModeRoundRobin = "round_robin"
	ModeWeighted   = "weighted"
	ModeFanout     = "fanout"
)

// ServerSettings are fixed for the lifetime of the container (not changeable via API).
type ServerSettings struct {
	ProxyPort        int    `json:"proxy_port" yaml:"proxy_port"`
	APIPort          int    `json:"api_port" yaml:"api_port"`
	FanoutPort       int    `json:"fanout_port" yaml:"fanout_port"`
	EnvoyAdminPort   int    `json:"envoy_admin_port" yaml:"envoy_admin_port"`
	EnvoyConcurrency int    `json:"envoy_concurrency" yaml:"envoy_concurrency"`
	EnvoyLogLevel    string `json:"envoy_log_level" yaml:"envoy_log_level"`
	StateDir         string `json:"state_dir" yaml:"state_dir"`
	LogDir           string `json:"log_dir" yaml:"log_dir"`
	DrainSeconds     int    `json:"drain_seconds" yaml:"drain_seconds"`
	StatsLogSeconds  int    `json:"stats_log_seconds" yaml:"stats_log_seconds"`
	LogRequests      *bool  `json:"log_requests_stdout,omitempty" yaml:"log_requests_stdout,omitempty"`
}

// FanoutSettings control the "send to all backends" mode.
type FanoutSettings struct {
	TimeoutMs    int `json:"timeout_ms" yaml:"timeout_ms"`
	SuccessMin   int `json:"success_min" yaml:"success_min"`
	SuccessMax   int `json:"success_max" yaml:"success_max"`
	MaxBodyBytes int `json:"max_body_bytes" yaml:"max_body_bytes"`
}

// HealthCheckSettings configure Envoy's active health checking of backends.
type HealthCheckSettings struct {
	Path       string `json:"path" yaml:"path"`
	IntervalMs int    `json:"interval_ms" yaml:"interval_ms"`
}

// Backend is one upstream service. If Command is set, the controller runs and supervises it.
type Backend struct {
	Name    string            `json:"name" yaml:"name"`
	Runtime string            `json:"runtime,omitempty" yaml:"runtime,omitempty"`
	Address string            `json:"address" yaml:"address"`
	Port    int               `json:"port" yaml:"port"`
	Weight  *int              `json:"weight,omitempty" yaml:"weight,omitempty"`
	Enabled *bool             `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	Command []string          `json:"command,omitempty" yaml:"command,omitempty"`
	Env     map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
}

func (b Backend) IsEnabled() bool { return b.Enabled == nil || *b.Enabled }
func (b Backend) W() int {
	if b.Weight == nil {
		return 1
	}
	return *b.Weight
}
func (b Backend) HostPort() string    { return fmt.Sprintf("%s:%d", b.Address, b.Port) }
func (b Backend) Managed() bool       { return len(b.Command) > 0 }
func (b Backend) ClusterName() string { return "b_" + b.Name }

// RoutingConfig is the runtime-changeable part of the configuration.
type RoutingConfig struct {
	Version     int64               `json:"version" yaml:"-"`
	Mode        string              `json:"mode" yaml:"mode"`
	Fanout      FanoutSettings      `json:"fanout" yaml:"fanout"`
	HealthCheck HealthCheckSettings `json:"health_check" yaml:"health_check"`
	Backends    []Backend           `json:"backends" yaml:"backends"`
}

// FileConfig is the on-disk configuration format.
type FileConfig struct {
	Server        ServerSettings `yaml:"server"`
	RoutingConfig `yaml:",inline"`
}

// Built-in catalog of the sample backends shipped in the image (MB_BACKENDS_DIR, default /opt/mb/backends).
var catalog = func() map[string]Backend {
	dir := os.Getenv("MB_BACKENDS_DIR")
	if dir == "" {
		dir = "/opt/mb/backends"
	}
	return map[string]Backend{
		"python": {Name: "python", Runtime: "python", Port: 8001, Command: []string{"python3", "-u", dir + "/python/server.py"}},
		"go":     {Name: "go", Runtime: "go", Port: 8002, Command: []string{dir + "/go/backend"}},
		"java":   {Name: "java", Runtime: "java", Port: 8003, Command: []string{"java", "-XX:+UseSerialGC", "-XX:TieredStopAtLevel=1", "-Xms32m", "-cp", dir + "/java", "Server"}},
		"rust":   {Name: "rust", Runtime: "rust", Port: 8004, Command: []string{dir + "/rust/backend"}},
	}
}()

func defaultServer() ServerSettings {
	return ServerSettings{
		ProxyPort: 8080, APIPort: 9000, FanoutPort: 9100, EnvoyAdminPort: 9901,
		EnvoyConcurrency: 2, EnvoyLogLevel: "warning",
		StateDir: "/var/lib/multi-backend", LogDir: "/var/log/multi-backend",
		DrainSeconds: 5, StatsLogSeconds: 60,
	}
}

func intp(i int) *int    { return &i }
func boolp(b bool) *bool { return &b }

// loadConfig reads the YAML/JSON config file (optional) and applies environment overrides.
func loadConfig(path string) (ServerSettings, RoutingConfig, error) {
	fc := FileConfig{Server: defaultServer()}
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			return fc.Server, fc.RoutingConfig, err
		}
		if err == nil {
			if err := yaml.Unmarshal(data, &fc); err != nil {
				return fc.Server, fc.RoutingConfig, fmt.Errorf("parse %s: %w", path, err)
			}
		}
	}
	// MB_BACKENDS=python,go:3,java  → backends from the built-in catalog (name[:weight]).
	if spec := strings.TrimSpace(os.Getenv("MB_BACKENDS")); spec != "" {
		bs, err := backendsFromSpec(spec)
		if err != nil {
			return fc.Server, fc.RoutingConfig, err
		}
		fc.Backends = bs
	}
	if m := os.Getenv("MB_MODE"); m != "" {
		fc.Mode = m
	}
	envInt := func(name string, dst *int) {
		if v := os.Getenv(name); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				*dst = n
			}
		}
	}
	envInt("MB_PROXY_PORT", &fc.Server.ProxyPort)
	envInt("MB_API_PORT", &fc.Server.APIPort)
	envInt("ENVOY_CONCURRENCY", &fc.Server.EnvoyConcurrency)
	envInt("MB_FANOUT_TIMEOUT_MS", &fc.Fanout.TimeoutMs)
	envInt("MB_STATS_LOG_SECONDS", &fc.Server.StatsLogSeconds)
	envInt("MB_DRAIN_SECONDS", &fc.Server.DrainSeconds)
	if v := os.Getenv("ENVOY_LOG_LEVEL"); v != "" {
		fc.Server.EnvoyLogLevel = v
	}
	if v := os.Getenv("MB_LOG_REQUESTS_STDOUT"); v != "" {
		b := v == "1" || strings.EqualFold(v, "true")
		fc.Server.LogRequests = &b
	}
	rc := fc.RoutingConfig
	if len(rc.Backends) == 0 {
		rc.Backends, _ = backendsFromSpec("python,go")
	}
	normalize(&rc)
	return fc.Server, rc, validate(&rc)
}

func backendsFromSpec(spec string) ([]Backend, error) {
	var out []Backend
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		name, w := item, 1
		if i := strings.Index(item, ":"); i >= 0 {
			name = item[:i]
			n, err := strconv.Atoi(item[i+1:])
			if err != nil {
				return nil, fmt.Errorf("MB_BACKENDS: bad weight in %q", item)
			}
			w = n
		}
		c, ok := catalog[name]
		if !ok {
			return nil, fmt.Errorf("MB_BACKENDS: unknown runtime %q (known: python, go, java, rust)", name)
		}
		c.Weight = intp(w)
		out = append(out, c)
	}
	return out, nil
}

// normalize fills defaults in place.
func normalize(rc *RoutingConfig) {
	if rc.Mode == "" {
		rc.Mode = ModeRoundRobin
	}
	if rc.Fanout.TimeoutMs <= 0 {
		rc.Fanout.TimeoutMs = 10000
	}
	if rc.Fanout.SuccessMin == 0 && rc.Fanout.SuccessMax == 0 {
		rc.Fanout.SuccessMin, rc.Fanout.SuccessMax = 200, 299
	}
	if rc.Fanout.MaxBodyBytes <= 0 {
		rc.Fanout.MaxBodyBytes = 10 << 20
	}
	if rc.HealthCheck.Path == "" {
		rc.HealthCheck.Path = "/health"
	}
	if rc.HealthCheck.IntervalMs <= 0 {
		rc.HealthCheck.IntervalMs = 2000
	}
	for i := range rc.Backends {
		normalizeBackend(&rc.Backends[i])
	}
}

func normalizeBackend(b *Backend) {
	if c, ok := catalog[b.Runtime]; ok && len(b.Command) == 0 && (b.Address == "" || b.Address == "127.0.0.1") {
		b.Command = append([]string(nil), c.Command...)
	}
	if b.Address == "" {
		b.Address = "127.0.0.1"
	}
	if b.Weight == nil {
		b.Weight = intp(1)
	}
	if b.Enabled == nil {
		b.Enabled = boolp(true)
	}
}

var nameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,40}$`)

func validate(rc *RoutingConfig) error {
	switch rc.Mode {
	case ModeRoundRobin, ModeWeighted, ModeFanout:
	default:
		return fmt.Errorf("invalid mode %q (round_robin, weighted, fanout)", rc.Mode)
	}
	if len(rc.Backends) == 0 {
		return fmt.Errorf("at least one backend is required")
	}
	if rc.Fanout.SuccessMin < 100 || rc.Fanout.SuccessMax > 599 || rc.Fanout.SuccessMin > rc.Fanout.SuccessMax {
		return fmt.Errorf("invalid fanout success range %d-%d", rc.Fanout.SuccessMin, rc.Fanout.SuccessMax)
	}
	if !strings.HasPrefix(rc.HealthCheck.Path, "/") {
		return fmt.Errorf("health_check.path must start with /")
	}
	names, addrs := map[string]bool{}, map[string]bool{}
	enabled, weighted := 0, 0
	for _, b := range rc.Backends {
		if !nameRe.MatchString(b.Name) {
			return fmt.Errorf("invalid backend name %q (lowercase letters, digits, - and _)", b.Name)
		}
		if names[b.Name] {
			return fmt.Errorf("duplicate backend name %q", b.Name)
		}
		names[b.Name] = true
		if b.Port <= 0 || b.Port > 65535 {
			return fmt.Errorf("backend %q: invalid port %d", b.Name, b.Port)
		}
		if addrs[b.HostPort()] {
			return fmt.Errorf("backend %q: address %s already used", b.Name, b.HostPort())
		}
		addrs[b.HostPort()] = true
		if b.W() < 0 || b.W() > 1000 {
			return fmt.Errorf("backend %q: weight must be 0..1000", b.Name)
		}
		if b.IsEnabled() {
			enabled++
			if b.W() > 0 {
				weighted++
			}
		}
	}
	if enabled == 0 {
		return fmt.Errorf("at least one backend must be enabled")
	}
	if rc.Mode == ModeWeighted && weighted == 0 {
		return fmt.Errorf("weighted mode needs at least one enabled backend with weight > 0")
	}
	return nil
}

// clone deep-copies a routing config.
func (rc RoutingConfig) clone() RoutingConfig {
	data, _ := json.Marshal(rc)
	var out RoutingConfig
	_ = json.Unmarshal(data, &out)
	return out
}

func (rc RoutingConfig) find(name string) (int, bool) {
	for i, b := range rc.Backends {
		if b.Name == name {
			return i, true
		}
	}
	return -1, false
}

// nextFreePort returns the lowest port >= 8001 not used by a backend.
func (rc RoutingConfig) nextFreePort() int {
	used := map[int]bool{}
	for _, b := range rc.Backends {
		used[b.Port] = true
	}
	ports := []int{}
	for p := range used {
		ports = append(ports, p)
	}
	sort.Ints(ports)
	for p := 8001; ; p++ {
		if !used[p] {
			return p
		}
	}
}
