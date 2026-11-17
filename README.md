# multi-backend

One container: an **Envoy** front proxy that spreads incoming requests over **one or more backends running inside the same container**, plus a control API to inspect and change routing at runtime without downtime.

```
             :8080                                  ┌──────────────── container ────────────────┐
 client ───────────► Envoy ── pool (RR / weighted RR) ──► python :8001   go :8002                │
                       │                                  java   :8003   rust :8004   ...        │
                       └─── fanout ──► controller :9100 ──► all enabled backends in parallel     │
                                                                                                 │
 operator ── :9000 ──► controller (API, xDS files, process supervisor, stats, request log)        │
                                                    └──────────────────────────────────────────┘
```

## Features

| Requirement | How |
|---|---|
| 1..N backends | Python, Go, Java, Rust sample backends built in; any other command or external address can be added |
| Round robin (default) | Envoy `ROUND_ROBIN` over the `pool` cluster, all weights 1 |
| Weighted routing | `mode: weighted` – endpoint `load_balancing_weight` (weighted round robin); weight 0 = out of rotation |
| Fan out to all backends | `mode: fanout` – request is sent to every enabled backend concurrently; the **first response with a success status (default 2xx) is returned**. If none succeeds, the first response received (or 502) is returned |
| Inspect / change routing live | REST API on `:9000`. Changes are pushed to Envoy as CDS/EDS/RDS file updates (hot reload, no restart, no dropped connections); the API returns once Envoy has confirmed the new version |
| Statistics + logging | Every request becomes one JSON record (`type: request`) on stdout and in `/var/log/multi-backend/requests.log`; aggregated per-backend stats via `GET /api/stats` and every 60 s as a `type: stats` log line |
| Fan-out summary | Fan-out records contain every backend's status, latency and result plus a `summary` (winner, succeeded/failed, fastest/slowest, status codes). Slower backends are still awaited (up to the timeout) so their stats are complete |

## Build and run

```bash
docker build -t multi-backend .          # or: podman build -t multi-backend .

# default: python + go, round robin
docker run --rm -p 8080:8080 -p 9000:9000 multi-backend

# choose backends (and optional weights) from the built-in runtimes
docker run --rm -p 8080:8080 -p 9000:9000 -e MB_BACKENDS=python,go,java,rust multi-backend
docker run --rm -p 8080:8080 -p 9000:9000 -e MB_BACKENDS=python:3,go:1 -e MB_MODE=weighted multi-backend

# full configuration file
docker run --rm -p 8080:8080 -p 9000:9000 -v $PWD/config/config.yaml:/etc/multi-backend/config.yaml multi-backend
```

See [`config/config.yaml`](config/config.yaml) for all options. Environment overrides: `MB_BACKENDS`, `MB_MODE`, `MB_FANOUT_TIMEOUT_MS`, `MB_LOG_REQUESTS_STDOUT`, `MB_STATS_LOG_SECONDS`, `MB_DRAIN_SECONDS`, `ENVOY_CONCURRENCY`, `ENVOY_LOG_LEVEL`, `MB_CONFIG`.

### Your own backends

A backend is either
* `runtime: python|go|java|rust` – the bundled sample server,
* `command: [...]` – any program the controller should run and supervise (it gets `PORT` and `BACKEND_NAME` env vars and must listen on `127.0.0.1:$PORT`); copy your app into the image with a `FROM multi-backend` Dockerfile,
* `address` + `port` only – a server the controller does not start.

Backends must answer `GET /health` (configurable via `health_check.path`) with 2xx; Envoy actively health-checks them and never routes to an unhealthy one.

## Control API (`:9000`)

```bash
A=localhost:9000
curl $A/api/config                                   # everything: mode, fanout settings, backends, version
curl $A/api/backends                                 # + process state, Envoy health, per-backend stats
curl $A/api/envoy/clusters                           # live Envoy host health / weights
curl $A/api/envoy/config                             # live Envoy dynamic config (clusters, endpoints, routes)

curl -X PUT $A/api/mode    -d '{"mode":"weighted"}'  # round_robin | weighted | fanout
curl -X PUT $A/api/weights -d '{"python":3,"go":1}'
curl $A/api/weights                                  # weights + expected traffic share per backend
curl -X PATCH $A/api/backends/go -d '{"enabled":false}'   # take out of rotation (process keeps running)
curl -X PATCH $A/api/backends/go -d '{"weight":5,"enabled":true}'
curl -X PUT $A/api/fanout  -d '{"timeout_ms":3000,"success_min":200,"success_max":299}'

curl -X POST   $A/api/backends -d '{"name":"java2","runtime":"java","weight":2}'   # starts it, waits for health, then routes
curl -X POST   $A/api/backends -d '{"name":"ext","address":"10.0.0.5","port":8080}'
curl -X DELETE $A/api/backends/java2                 # removed from routing first, process stopped after drain_seconds

curl -X PUT $A/api/config -d @new-config.json        # replace the whole routing config (same shape as GET)

curl $A/api/stats                                    # per backend: requests, success/failure, status classes,
                                                     #   by mode, fanout wins, latency avg/p50/p90/p99/max
curl -X DELETE $A/api/stats                          # reset
curl "$A/api/requests?limit=20&mode=fanout"          # most recent request records
```

Every change bumps the config `version`; responses carry `x-mb-config-version` so you can see which version served a request.

### Per-request overrides (headers on the proxy port)

| Header | Effect |
|---|---|
| `x-mb-mode: fanout` | fan out this request regardless of the configured mode |
| `x-mb-mode: balance` | load-balance this request (round robin / weighted) even in fanout mode |
| `x-mb-backend: <name>` | send to exactly this backend (also works for disabled backends) |

Response headers: `x-mb-route`, `x-mb-upstream-host`, `x-mb-config-version`, and for fan-out `x-mb-fanout-winner`, `x-mb-fanout-backends`.

## Logs

All container output is JSON lines, distinguished by `type`:

* `request` – one per client request:
  ```json
  {"type":"request","request_id":"…","mode":"fanout","method":"GET","path":"/orders","status":200,
   "duration_ms":0.44,"total_duration_ms":412.1,"config_version":7,"winner":"go",
   "backends":[{"name":"go","status":200,"duration_ms":0.43,"result":"success","winner":true},
               {"name":"python","status":200,"duration_ms":0.8,"result":"success"},
               {"name":"java","status":503,"duration_ms":2.1,"result":"http_error"},
               {"name":"rust","status":0,"duration_ms":412.1,"result":"timeout","error":"…"}],
   "summary":{"backends":4,"succeeded":2,"failed":2,"winner":"go","fastest":"go","slowest":"rust",
              "slowest_ms":412.1,"status_codes":{"200":2,"503":1,"timeout":1}}}
  ```
  Balanced (`round_robin`, `weighted`) and `direct` requests have a single entry in `backends`. `result` is one of `success`, `http_error`, `conn_error`, `timeout`.
* `stats` – periodic snapshot of `GET /api/stats`.
* `event` – controller events (config applied, backend started/exited, API calls).
* `backend_log` / `envoy_log` – output of the backend processes and Envoy.

Files in the container: `/var/log/multi-backend/requests.log` (request records), `/var/log/multi-backend/envoy-access.log` (raw Envoy access log), both rotated at 100 MB.

## Sample backends

All four implement the same contract, which is handy for testing:

* `GET /health` → `200 {"status":"ok"}`
* anything else → JSON echo (`backend`, `runtime`, `method`, `path`, `request_id`, `config_version`, `fanout`, …)
* query knobs: `delay_ms=N`, `status=N`, or per backend `delay_ms.<name>=N`, `status.<name>=N`
  e.g. `curl -H 'x-mb-mode: fanout' 'localhost:8080/x?delay_ms.python=500&status.go=503'`

## How it works

* `controller/` (Go) is PID 1's child (under `tini`). It starts the backends, writes the Envoy bootstrap and the xDS files (`/var/lib/multi-backend/xds/{cds,eds,rds}.json`), starts Envoy, and serves the API.
* Envoy has a static listener; clusters come from `cds.json`, the `pool` endpoints/weights from `eds.json`, routes from `rds.json`. Files are replaced atomically (write + rename into the watched directory) so Envoy hot-reloads them. The controller waits until Envoy's `*.version_text` stats show the new version.
* Changes are rolled out so that nothing is ever routed to a missing target: new backend processes are started and health-checked first; clusters of removed backends stay until routes no longer reference them; their processes are stopped after `drain_seconds`.
* Pool settings: active HTTP health checks, `healthy_panic_threshold: 0` (never send to unhealthy hosts), `ignore_health_on_host_removal` (removed hosts leave immediately), `update_merge_window: 0s` (changes hit the workers immediately), retries on connect failure to a different host.
* Fan-out: Envoy routes to the controller's fan-out proxy (`127.0.0.1:9100`), which buffers the body (max 10 MB), calls all enabled backends in parallel, streams back the first success and records the full outcome when the last backend finishes or times out.
* Statistics: balanced requests are taken from Envoy's JSON access log (tailed by the controller); fan-out requests are recorded by the fan-out proxy.

Note: Envoy's round robin state is per worker thread (`envoy_concurrency`, default 2), so the distribution is exact over time but not strictly alternating per request. Set `ENVOY_CONCURRENCY=1` for strict alternation.

## Tests

```bash
docker run -d --name mb -p 8080:8080 -p 9000:9000 -e MB_BACKENDS=python,go,java,rust multi-backend
python3 tests/e2e.py --proxy http://localhost:8080 --api http://localhost:9000
```

Covers round robin, weighted (incl. weight 0), enable/disable, fan-out winner selection / no-success / timeouts / dead backends, header overrides, adding and removing backends, statistics, and a zero-downtime test that runs continuous load while switching modes and adding/removing backends (asserts no failed request).

Rootless podman note: if published ports hang with the default `pasta` network, use `--network slirp4netns:port_handler=slirp4netns`.
