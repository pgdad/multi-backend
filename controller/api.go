package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// API is the HTTP control plane: inspect and change routing at runtime.
type API struct{ ctl *Controller }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	var ve ValidationError
	var ae ApplyError
	switch {
	case errors.As(err, &ve):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
	case errors.As(err, &ae):
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}
}

func decode(r *http.Request, v any) error {
	data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return ValidationError{err}
	}
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return ValidationError{fmt.Errorf("invalid JSON body: %w", err)}
	}
	return nil
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", a.index)
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /api/config", a.getConfig)
	mux.HandleFunc("PUT /api/config", a.putConfig)
	mux.HandleFunc("GET /api/mode", a.getMode)
	mux.HandleFunc("PUT /api/mode", a.putMode)
	mux.HandleFunc("GET /api/weights", a.getWeights)
	mux.HandleFunc("PUT /api/weights", a.putWeights)
	mux.HandleFunc("PUT /api/fanout", a.putFanout)
	mux.HandleFunc("GET /api/backends", a.listBackends)
	mux.HandleFunc("POST /api/backends", a.addBackend)
	mux.HandleFunc("GET /api/backends/{name}", a.getBackend)
	mux.HandleFunc("PATCH /api/backends/{name}", a.patchBackend)
	mux.HandleFunc("DELETE /api/backends/{name}", a.deleteBackend)
	mux.HandleFunc("GET /api/stats", a.getStats)
	mux.HandleFunc("DELETE /api/stats", a.resetStats)
	mux.HandleFunc("GET /api/requests", a.getRequests)
	mux.HandleFunc("GET /api/envoy/clusters", a.envoyClusters)
	mux.HandleFunc("GET /api/envoy/config", a.envoyConfig)
	return logRequests(mux)
}

func logRequests(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			logEvent("info", "api call", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
		}
		h.ServeHTTP(w, r)
	})
}

func (a *API) index(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"service": "multi-backend controller", "endpoints": []string{
		"GET    /api/config                full routing configuration",
		"PUT    /api/config                replace routing configuration",
		"GET    /api/mode | PUT /api/mode  {\"mode\":\"round_robin|weighted|fanout\"}",
		"GET    /api/weights | PUT /api/weights  {\"python\":3,\"go\":1}",
		"PUT    /api/fanout                {\"timeout_ms\":5000,\"success_min\":200,\"success_max\":299}",
		"GET    /api/backends              backends with process, health and stats",
		"POST   /api/backends              {\"name\":\"java2\",\"runtime\":\"java\",\"weight\":1}",
		"GET    /api/backends/{name}",
		"PATCH  /api/backends/{name}       {\"weight\":2,\"enabled\":false}",
		"DELETE /api/backends/{name}",
		"GET    /api/stats | DELETE /api/stats",
		"GET    /api/requests?limit=20&mode=fanout",
		"GET    /api/envoy/clusters        live Envoy host health/weights",
		"GET    /api/envoy/config          live Envoy dynamic config (clusters, endpoints, routes)",
	}})
}

func (a *API) healthz(w http.ResponseWriter, r *http.Request) {
	if !a.ctl.envoyUp.Load() {
		writeJSON(w, 503, map[string]any{"status": "starting"})
		return
	}
	writeJSON(w, 200, map[string]any{"status": "ok", "version": a.ctl.Current().Version})
}

func (a *API) respondConfig(w http.ResponseWriter, rc RoutingConfig, err error) {
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, rc)
}

func (a *API) getConfig(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, a.ctl.Current()) }

func (a *API) putConfig(w http.ResponseWriter, r *http.Request) {
	var in RoutingConfig
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	rc, err := a.ctl.Update(func(rc *RoutingConfig) error {
		v := rc.Version
		*rc = in
		rc.Version = v
		return nil
	})
	a.respondConfig(w, rc, err)
}

func (a *API) getMode(w http.ResponseWriter, r *http.Request) {
	rc := a.ctl.Current()
	writeJSON(w, 200, map[string]any{"mode": rc.Mode, "version": rc.Version})
}

func (a *API) putMode(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Mode string `json:"mode"`
	}
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	rc, err := a.ctl.Update(func(rc *RoutingConfig) error { rc.Mode = in.Mode; return nil })
	a.respondConfig(w, rc, err)
}

func (a *API) getWeights(w http.ResponseWriter, r *http.Request) {
	rc := a.ctl.Current()
	out := map[string]int{}
	for _, b := range rc.Backends {
		out[b.Name] = b.W()
	}
	writeJSON(w, 200, map[string]any{"mode": rc.Mode, "weights": out, "effective": effectiveShares(rc)})
}

// effectiveShares is the expected traffic share per backend under the current mode.
func effectiveShares(rc RoutingConfig) map[string]float64 {
	out := map[string]float64{}
	total := 0
	for _, b := range rc.Backends {
		if b.IsEnabled() {
			if rc.Mode == ModeWeighted {
				total += b.W()
			} else {
				total++
			}
		}
	}
	for _, b := range rc.Backends {
		share := 0.0
		if b.IsEnabled() && total > 0 {
			switch rc.Mode {
			case ModeWeighted:
				share = 100 * float64(b.W()) / float64(total)
			case ModeRoundRobin:
				share = 100 / float64(total)
			case ModeFanout:
				share = 100 // every request goes to every enabled backend
			}
		}
		out[b.Name] = round2(share)
	}
	return out
}

func (a *API) putWeights(w http.ResponseWriter, r *http.Request) {
	var in map[string]int
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	rc, err := a.ctl.Update(func(rc *RoutingConfig) error {
		for name, wt := range in {
			i, ok := rc.find(name)
			if !ok {
				return fmt.Errorf("unknown backend %q", name)
			}
			rc.Backends[i].Weight = intp(wt)
		}
		return nil
	})
	a.respondConfig(w, rc, err)
}

func (a *API) putFanout(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TimeoutMs    *int `json:"timeout_ms"`
		SuccessMin   *int `json:"success_min"`
		SuccessMax   *int `json:"success_max"`
		MaxBodyBytes *int `json:"max_body_bytes"`
	}
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	rc, err := a.ctl.Update(func(rc *RoutingConfig) error {
		if in.TimeoutMs != nil {
			if *in.TimeoutMs < 1 || *in.TimeoutMs > 300000 {
				return fmt.Errorf("timeout_ms must be 1..300000")
			}
			rc.Fanout.TimeoutMs = *in.TimeoutMs
		}
		if in.SuccessMin != nil {
			rc.Fanout.SuccessMin = *in.SuccessMin
		}
		if in.SuccessMax != nil {
			rc.Fanout.SuccessMax = *in.SuccessMax
		}
		if in.MaxBodyBytes != nil {
			rc.Fanout.MaxBodyBytes = *in.MaxBodyBytes
		}
		return nil
	})
	a.respondConfig(w, rc, err)
}

func (a *API) backendView(rc RoutingConfig, b Backend, health map[string]map[string]any, stats map[string]any) map[string]any {
	v := map[string]any{
		"name": b.Name, "runtime": b.Runtime, "address": b.Address, "port": b.Port,
		"weight": b.W(), "enabled": b.IsEnabled(), "command": b.Command,
		"expected_share_pct": effectiveShares(rc)[b.Name],
		"process":            a.ctl.sup.Info(b.Name),
	}
	if h, ok := health[b.HostPort()]; ok {
		v["envoy"] = h
	} else {
		v["envoy"] = map[string]any{"in_pool": false}
	}
	if s, ok := stats[b.Name]; ok {
		v["stats"] = s
	}
	return v
}

func (a *API) listBackends(w http.ResponseWriter, r *http.Request) {
	rc := a.ctl.Current()
	health := a.poolHealth()
	stats, _ := a.ctl.stats.Snapshot()["backends"].(map[string]any)
	out := []any{}
	for _, b := range rc.Backends {
		out = append(out, a.backendView(rc, b, health, stats))
	}
	writeJSON(w, 200, map[string]any{"mode": rc.Mode, "version": rc.Version, "backends": out})
}

func (a *API) getBackend(w http.ResponseWriter, r *http.Request) {
	rc := a.ctl.Current()
	i, ok := rc.find(r.PathValue("name"))
	if !ok {
		writeJSON(w, 404, map[string]any{"error": "no such backend"})
		return
	}
	stats, _ := a.ctl.stats.Snapshot()["backends"].(map[string]any)
	writeJSON(w, 200, a.backendView(rc, rc.Backends[i], a.poolHealth(), stats))
}

func (a *API) addBackend(w http.ResponseWriter, r *http.Request) {
	var in Backend
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	rc, err := a.ctl.Update(func(rc *RoutingConfig) error {
		if in.Name == "" {
			in.Name = in.Runtime
		}
		if _, exists := rc.find(in.Name); exists {
			return fmt.Errorf("backend %q already exists", in.Name)
		}
		if in.Runtime == "" && len(in.Command) == 0 && (in.Address == "" || in.Port == 0) {
			return fmt.Errorf("specify runtime (python|go|java|rust), a command, or an external address+port")
		}
		if in.Runtime != "" && len(in.Command) == 0 && (in.Address == "" || in.Address == "127.0.0.1") {
			if _, ok := catalog[in.Runtime]; !ok {
				return fmt.Errorf("unknown runtime %q", in.Runtime)
			}
		}
		if in.Port == 0 {
			in.Port = rc.nextFreePort()
		}
		rc.Backends = append(rc.Backends, in)
		return nil
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	i, _ := rc.find(in.Name)
	writeJSON(w, 201, map[string]any{"version": rc.Version, "backend": rc.Backends[i]})
}

func (a *API) patchBackend(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Weight  *int  `json:"weight"`
		Enabled *bool `json:"enabled"`
	}
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	name := r.PathValue("name")
	rc, err := a.ctl.Update(func(rc *RoutingConfig) error {
		i, ok := rc.find(name)
		if !ok {
			return fmt.Errorf("unknown backend %q", name)
		}
		if in.Weight != nil {
			rc.Backends[i].Weight = in.Weight
		}
		if in.Enabled != nil {
			rc.Backends[i].Enabled = in.Enabled
		}
		return nil
	})
	a.respondConfig(w, rc, err)
}

func (a *API) deleteBackend(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	rc, err := a.ctl.Update(func(rc *RoutingConfig) error {
		i, ok := rc.find(name)
		if !ok {
			return fmt.Errorf("unknown backend %q", name)
		}
		rc.Backends = append(rc.Backends[:i], rc.Backends[i+1:]...)
		return nil
	})
	a.respondConfig(w, rc, err)
}

func (a *API) getStats(w http.ResponseWriter, r *http.Request) {
	s := a.ctl.stats.Snapshot()
	s["config_version"] = a.ctl.Current().Version
	s["mode"] = a.ctl.Current().Mode
	writeJSON(w, 200, s)
}

func (a *API) resetStats(w http.ResponseWriter, r *http.Request) {
	a.ctl.stats.Reset()
	writeJSON(w, 200, map[string]any{"reset": true})
}

func (a *API) getRequests(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 20
	}
	mode := r.URL.Query().Get("mode")
	all := a.ctl.stats.Recent(1000)
	out := []RequestRecord{}
	for _, rec := range all {
		if mode != "" && rec.Mode != mode {
			continue
		}
		out = append(out, rec)
		if len(out) >= limit {
			break
		}
	}
	writeJSON(w, 200, out)
}

// poolHealth summarizes the hosts of Envoy's "pool" cluster keyed by "addr:port".
func (a *API) poolHealth() map[string]map[string]any {
	out := map[string]map[string]any{}
	data, code, err := a.ctl.envoy.Get("/clusters?format=json")
	if err != nil || code != 200 {
		return out
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
				Stats []struct {
					Name  string `json:"name"`
					Value string `json:"value"`
				} `json:"stats"`
				HealthStatus map[string]any `json:"health_status"`
				Weight       int            `json:"weight"`
			} `json:"host_statuses"`
		} `json:"cluster_statuses"`
	}
	if json.Unmarshal(data, &cl) != nil {
		return out
	}
	for _, c := range cl.ClusterStatuses {
		if c.Name != poolCluster {
			continue
		}
		for _, h := range c.HostStatuses {
			key := fmt.Sprintf("%s:%d", h.Address.SocketAddress.Address, h.Address.SocketAddress.PortValue)
			st := map[string]any{}
			for _, s := range h.Stats {
				if s.Value != "" {
					st[s.Name], _ = strconv.ParseUint(s.Value, 10, 64)
				}
			}
			healthy := true
			flags := []string{}
			for k, v := range h.HealthStatus {
				if b, ok := v.(bool); ok && b {
					healthy = false
					flags = append(flags, k)
				}
			}
			sort.Strings(flags)
			out[key] = map[string]any{"in_pool": true, "healthy": healthy, "health_flags": flags,
				"lb_weight": h.Weight, "eds_health_status": h.HealthStatus["eds_health_status"], "envoy_stats": st}
		}
	}
	return out
}

func (a *API) envoyClusters(w http.ResponseWriter, r *http.Request) {
	rc := a.ctl.Current()
	health := a.poolHealth()
	out := map[string]any{}
	for k, v := range health {
		out[a.ctl.backendNameFor(k, "")] = v
	}
	writeJSON(w, 200, map[string]any{"mode": rc.Mode, "version": rc.Version, "pool": out, "checked_at": time.Now().UTC()})
}

func (a *API) envoyConfig(w http.ResponseWriter, r *http.Request) {
	data, code, err := a.ctl.envoy.Get("/config_dump?include_eds")
	if err != nil || code != 200 {
		writeJSON(w, 502, map[string]any{"error": fmt.Sprintf("envoy admin: %v (status %d)", err, code)})
		return
	}
	var dump struct {
		Configs []map[string]any `json:"configs"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		writeJSON(w, 502, map[string]any{"error": err.Error()})
		return
	}
	out := map[string]any{}
	for _, c := range dump.Configs {
		switch c["@type"] {
		case "type.googleapis.com/envoy.admin.v3.ClustersConfigDump":
			out["clusters"] = map[string]any{"version_info": c["version_info"], "dynamic_active_clusters": c["dynamic_active_clusters"], "dynamic_warming_clusters": c["dynamic_warming_clusters"]}
		case "type.googleapis.com/envoy.admin.v3.EndpointsConfigDump":
			out["endpoints"] = c["dynamic_endpoint_configs"]
		case "type.googleapis.com/envoy.admin.v3.RoutesConfigDump":
			out["routes"] = c["dynamic_route_configs"]
		}
	}
	writeJSON(w, 200, out)
}
