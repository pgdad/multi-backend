package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// Envoy gets its dynamic configuration (CDS, EDS, RDS) from files in xdsDir. Envoy watches the
// directory and hot-reloads a file when it is atomically moved into place, so every change made via
// the API is applied without restarting Envoy or dropping connections.
//
//   cds.json  clusters: "pool" (balanced, EDS), "fanout" (controller's fan-out proxy), b_<name> per backend
//   eds.json  endpoints of "pool" with per-endpoint weights (weighted round robin)
//   rds.json  route config "main": header overrides + default route chosen by mode

const (
	poolCluster   = "pool"
	fanoutCluster = "fanout"
	routeConfig   = "main"
)

type XDS struct {
	dir        string
	fanoutPort int
}

func (x *XDS) path(name string) string { return filepath.Join(x.dir, name) }

func typed(t string, m map[string]any) map[string]any {
	m["@type"] = t
	return m
}

func pathSource(file, dir string) map[string]any {
	return map[string]any{
		"path_config_source":   map[string]any{"path": file, "watched_directory": map[string]any{"path": dir}},
		"resource_api_version": "V3",
	}
}

func socketAddr(addr string, port int) map[string]any {
	return map[string]any{"socket_address": map[string]any{"address": addr, "port_value": port}}
}

func staticCluster(name, addr string, port int, timeout string) map[string]any {
	return typed("type.googleapis.com/envoy.config.cluster.v3.Cluster", map[string]any{
		"name": name, "type": "STATIC", "connect_timeout": timeout, "lb_policy": "ROUND_ROBIN",
		"load_assignment": map[string]any{
			"cluster_name": name,
			"endpoints": []any{map[string]any{"lb_endpoints": []any{
				map[string]any{"endpoint": map[string]any{"address": socketAddr(addr, port)}},
			}}},
		},
	})
}

// clusters returns the CDS resources for the given backends.
func (x *XDS) clusters(rc RoutingConfig, backends []Backend) []any {
	ms := func(n int) string { return fmt.Sprintf("%.3fs", float64(n)/1000) }
	pool := typed("type.googleapis.com/envoy.config.cluster.v3.Cluster", map[string]any{
		"name": poolCluster, "type": "EDS", "connect_timeout": "1s", "lb_policy": "ROUND_ROBIN",
		"eds_cluster_config": map[string]any{"eds_config": pathSource(x.path("eds.json"), x.dir)},
		"health_checks": []any{map[string]any{
			"timeout": "1s", "interval": ms(rc.HealthCheck.IntervalMs), "no_traffic_interval": ms(rc.HealthCheck.IntervalMs),
			"unhealthy_threshold": 2, "healthy_threshold": 1,
			"http_health_check": map[string]any{"path": rc.HealthCheck.Path},
		}},
		// a backend removed from rotation (disabled / weight 0 / deleted) leaves immediately,
		// even though it is still passing health checks
		"ignore_health_on_host_removal": true,
		// never spill traffic onto unhealthy hosts
		// and push membership/weight changes to the worker threads immediately (default merges for 1s)
		"common_lb_config": map[string]any{"healthy_panic_threshold": map[string]any{"value": 0}, "update_merge_window": "0s"},
		"outlier_detection": map[string]any{
			"split_external_local_origin_errors":    true,
			"consecutive_local_origin_failure":      3,
			"enforcing_consecutive_5xx":             0,
			"enforcing_consecutive_gateway_failure": 0,
			"enforcing_success_rate":                0,
			"base_ejection_time":                    "5s",
		},
	})
	out := []any{pool, staticCluster(fanoutCluster, "127.0.0.1", x.fanoutPort, "1s")}
	for _, b := range backends {
		out = append(out, staticCluster(b.ClusterName(), b.Address, b.Port, "1s"))
	}
	return out
}

func (x *XDS) endpoints(rc RoutingConfig) []any {
	lbs := []any{}
	for _, b := range rc.Backends {
		if !b.IsEnabled() {
			continue
		}
		w := 1
		if rc.Mode == ModeWeighted {
			w = b.W()
			if w == 0 {
				continue // weight 0 = take out of rotation
			}
		}
		lbs = append(lbs, map[string]any{
			"endpoint":              map[string]any{"address": socketAddr(b.Address, b.Port), "hostname": b.Name},
			"load_balancing_weight": w,
			"metadata": map[string]any{"filter_metadata": map[string]any{
				"multi_backend": map[string]any{"backend": b.Name, "runtime": b.Runtime},
			}},
		})
	}
	return []any{typed("type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment", map[string]any{
		"cluster_name": poolCluster,
		"endpoints":    []any{map[string]any{"lb_endpoints": lbs}},
	})}
}

func (x *XDS) routes(rc RoutingConfig) []any {
	poolTimeout := "60s"
	fanoutTimeout := fmt.Sprintf("%.3fs", float64(rc.Fanout.TimeoutMs+2000)/1000)
	poolAction := func() map[string]any {
		return map[string]any{
			"cluster": poolCluster, "timeout": poolTimeout,
			"retry_policy": map[string]any{
				"retry_on":    "connect-failure,refused-stream,reset-before-request",
				"num_retries": 2,
				"retry_host_predicate": []any{map[string]any{
					"name":         "envoy.retry_host_predicates.previous_hosts",
					"typed_config": map[string]any{"@type": "type.googleapis.com/envoy.extensions.retry.host.previous_hosts.v3.PreviousHostsPredicate"},
				}},
				"host_selection_retry_max_attempts": 3,
			},
		}
	}
	fanoutAction := func() map[string]any {
		return map[string]any{"cluster": fanoutCluster, "timeout": fanoutTimeout}
	}
	hdr := func(name, value string) map[string]any {
		return map[string]any{"name": name, "string_match": map[string]any{"exact": value}}
	}
	balanceMode := ModeRoundRobin
	if rc.Mode == ModeWeighted {
		balanceMode = ModeWeighted
	}

	routes := []any{}
	// x-mb-backend: <name> → send to exactly that backend (enabled or not; useful for debugging).
	for _, b := range rc.Backends {
		routes = append(routes, map[string]any{
			"name":  "direct:" + b.Name,
			"match": map[string]any{"prefix": "/", "headers": []any{hdr("x-mb-backend", b.Name)}},
			"route": map[string]any{"cluster": b.ClusterName(), "timeout": poolTimeout},
		})
	}
	// x-mb-mode: fanout | balance → per-request override of the configured mode.
	routes = append(routes,
		map[string]any{"name": "fanout", "match": map[string]any{"prefix": "/", "headers": []any{hdr("x-mb-mode", "fanout")}}, "route": fanoutAction()},
		map[string]any{"name": "balance:" + balanceMode, "match": map[string]any{"prefix": "/", "headers": []any{hdr("x-mb-mode", "balance")}}, "route": poolAction()},
	)
	if rc.Mode == ModeFanout {
		routes = append(routes, map[string]any{"name": "fanout", "match": map[string]any{"prefix": "/"}, "route": fanoutAction()})
	} else {
		routes = append(routes, map[string]any{"name": "balance:" + balanceMode, "match": map[string]any{"prefix": "/"}, "route": poolAction()})
	}
	version := strconv.FormatInt(rc.Version, 10)
	return []any{typed("type.googleapis.com/envoy.config.route.v3.RouteConfiguration", map[string]any{
		"name": routeConfig,
		"request_headers_to_add": []any{
			map[string]any{"header": map[string]any{"key": "x-mb-config-version", "value": version}, "append_action": "OVERWRITE_IF_EXISTS_OR_ADD"},
		},
		"response_headers_to_add": []any{
			map[string]any{"header": map[string]any{"key": "x-mb-config-version", "value": version}, "append_action": "OVERWRITE_IF_EXISTS_OR_ADD"},
			map[string]any{"header": map[string]any{"key": "x-mb-upstream-host", "value": "%UPSTREAM_HOST%"}, "append_action": "OVERWRITE_IF_EXISTS_OR_ADD"},
			map[string]any{"header": map[string]any{"key": "x-mb-route", "value": "%ROUTE_NAME%"}, "append_action": "OVERWRITE_IF_EXISTS_OR_ADD"},
		},
		"virtual_hosts": []any{map[string]any{"name": "all", "domains": []any{"*"}, "routes": routes}},
	})}
}

// writeResources atomically replaces an xDS file (write temp + rename inside the watched dir).
func (x *XDS) writeResources(file string, version int64, resources []any) error {
	doc := map[string]any{"version_info": strconv.FormatInt(version, 10), "resources": resources}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	dst := x.path(file)
	if old, err := os.ReadFile(dst); err == nil && bytes.Equal(old, data) {
		return nil
	}
	tmp, err := os.CreateTemp(x.dir, "."+file+".tmp-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	tmp.Close()
	return os.Rename(tmp.Name(), dst)
}

// write renders the whole configuration. extraClusters keeps clusters of backends that are being
// removed alive until routes no longer reference them.
func (x *XDS) write(rc RoutingConfig, clusterBackends []Backend) error {
	if err := x.writeResources("cds.json", rc.Version, x.clusters(rc, clusterBackends)); err != nil {
		return err
	}
	if err := x.writeResources("eds.json", rc.Version, x.endpoints(rc)); err != nil {
		return err
	}
	return x.writeResources("rds.json", rc.Version, x.routes(rc))
}
