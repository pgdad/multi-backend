#!/usr/bin/env python3
"""End-to-end tests for a running multi-backend instance.

Usage: tests/e2e.py [--proxy http://localhost:8080] [--api http://localhost:9000]

Expects the instance to start with backends python, go, java, rust (MB_BACKENDS=python,go,java,rust).
Leaves the instance in round_robin mode with all four backends at weight 1.
"""
import argparse
import collections
import json
import sys
import threading
import time
import urllib.error
import urllib.request

PROXY = API = ""
FAILURES = []
ALL = ["python", "go", "java", "rust"]


def http(method, url, body=None, headers=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def api(method, path, body=None):
    status, _, data = http(method, API + path, body)
    try:
        return status, json.loads(data or b"null")
    except json.JSONDecodeError:
        return status, data


def proxy(path="/", headers=None):
    status, hdrs, data = http("GET", PROXY + path, headers=headers)
    hdrs = {k.lower(): v for k, v in hdrs.items()}
    return status, hdrs, data


def check(cond, msg):
    print(("  ok   " if cond else "  FAIL ") + msg)
    if not cond:
        FAILURES.append(msg)


def distribution(n, path="/dist"):
    c = collections.Counter()
    for _ in range(n):
        s, h, _ = proxy(path)
        c[h.get("x-backend-name") if s == 200 else f"status_{s}"] += 1
    return c


def near(actual, expected, tol):
    return abs(actual - expected) <= tol


def reset():
    s, _ = api("PUT", "/api/config", {
        "mode": "round_robin",
        "backends": [{"name": n, "runtime": n, "port": 8001 + i, "weight": 1} for i, n in enumerate(ALL)],
    })
    return s


def test_inspect():
    print("inspect configuration")
    s, cfg = api("GET", "/api/config")
    check(s == 200 and cfg["mode"] == "round_robin", f"GET /api/config -> mode {cfg.get('mode')}")
    check(sorted(b["name"] for b in cfg["backends"]) == sorted(ALL), "four backends configured")
    s, live = api("GET", "/api/envoy/config")
    check(s == 200 and live["routes"][0]["route_config"]["name"] == "main", "live Envoy routes visible")
    s, cl = api("GET", "/api/envoy/clusters")
    check(s == 200 and all(cl["pool"].get(n, {}).get("healthy") for n in ALL), f"all backends healthy in Envoy: {sorted(cl['pool'])}")


def test_round_robin():
    print("round robin")
    c = distribution(200)
    check(set(c) == set(ALL), f"every backend served traffic {dict(c)}")
    check(all(near(c[n], 50, 15) for n in ALL), "roughly equal shares")


def test_weighted():
    print("weighted")
    s, cfg = api("PUT", "/api/weights", {"python": 6, "go": 2, "java": 1, "rust": 1})
    check(s == 200, "PUT /api/weights")
    s, cfg = api("PUT", "/api/mode", {"mode": "weighted"})
    check(s == 200 and cfg["mode"] == "weighted", "PUT /api/mode weighted")
    c = distribution(400)
    check(near(c["python"], 240, 40) and near(c["go"], 80, 25) and near(c["java"], 40, 20) and near(c["rust"], 40, 20),
          f"6:2:1:1 distribution {dict(c)}")
    api("PATCH", "/api/backends/java", {"weight": 0})
    c = distribution(100)
    check(c["java"] == 0 and sum(c.values()) == 100, f"weight 0 removes java from rotation {dict(c)}")
    s, w = api("GET", "/api/weights")
    check(w["effective"]["java"] == 0 and w["effective"]["python"] == 66.67, f"effective shares {w['effective']}")


def test_disable():
    print("enable / disable")
    api("PUT", "/api/mode", {"mode": "round_robin"})
    s, _ = api("PATCH", "/api/backends/go", {"enabled": False})
    check(s == 200, "disable go")
    c = distribution(60)
    check(c["go"] == 0 and len(c) == 3, f"go out of rotation {dict(c)}")
    s, h, _ = proxy("/direct", headers={"x-mb-backend": "go"})
    check(s == 200 and h.get("x-backend-name") == "go", "x-mb-backend header still reaches a disabled backend")
    api("PATCH", "/api/backends/go", {"enabled": True})
    s, _ = api("PATCH", "/api/backends/python", {"enabled": False, "weight": 0})
    api("PATCH", "/api/backends/go", {"enabled": False})
    api("PATCH", "/api/backends/java", {"enabled": False})
    s, err = api("PATCH", "/api/backends/rust", {"enabled": False})
    check(s == 400, f"cannot disable every backend: {err}")
    reset()


def last_fanout(rid):
    for _ in range(50):
        s, recs = api("GET", "/api/requests?mode=fanout&limit=50")
        for r in recs:
            if r["request_id"] == rid:
                return r
        time.sleep(0.1)
    return None


def test_fanout():
    print("fanout (first success wins)")
    s, cfg = api("PUT", "/api/mode", {"mode": "fanout"})
    check(s == 200 and cfg["mode"] == "fanout", "PUT /api/mode fanout")
    t = time.time()
    s, h, body = proxy("/f?delay_ms=400&delay_ms.java=20&status.go=500&delay_ms.go=0")
    elapsed = time.time() - t
    check(s == 200 and h.get("x-mb-fanout-winner") == "java", f"fastest successful backend (java) wins, got {h.get('x-mb-fanout-winner')} {s}")
    check(elapsed < 0.3, f"responded before slow backends finished ({elapsed*1000:.0f} ms)")
    check(json.loads(body)["fanout"] is True, "backend saw x-mb-fanout")
    rec = last_fanout(h.get("x-request-id") or json.loads(body)["request_id"])
    check(rec is not None, "fanout request recorded")
    if rec:
        names = sorted(b["name"] for b in rec["backends"])
        check(names == sorted(ALL), f"record has all backends {names}")
        sm = rec["summary"]
        check(sm["succeeded"] == 3 and sm["failed"] == 1 and sm["winner"] == "java", f"summary {sm}")
        check(rec["total_duration_ms"] >= 400, f"stats waited for slowest backend ({rec['total_duration_ms']} ms)")
        go = [b for b in rec["backends"] if b["name"] == "go"][0]
        check(go["status"] == 500 and go["result"] == "http_error", "failing backend recorded with its status")

    s, h, _ = proxy("/f?status=503&delay_ms.rust=50")
    check(s == 503, f"no success -> first response returned ({s})")

    s, cfg = api("PUT", "/api/fanout", {"timeout_ms": 200})
    s, h, _ = proxy("/f?delay_ms=1000&delay_ms.rust=0")
    check(s == 200 and h.get("x-mb-fanout-winner") == "rust", "timeout config applied, rust wins")
    rid = h.get("x-request-id")
    time.sleep(0.4)
    rec = last_fanout(rid)
    if rec:
        check(rec["summary"]["status_codes"].get("timeout") == 3, f"slow backends recorded as timeout {rec['summary']['status_codes']}")
    api("PUT", "/api/fanout", {"timeout_ms": 10000})

    s, body = api("POST", "/api/backends", {"name": "external-dead", "address": "127.0.0.1", "port": 8999})
    # no process listens on 8999: adding works (unmanaged), fanout records conn_error
    check(s == 201, f"add unmanaged backend {s}")
    s, h, b = proxy("/f")
    check(s == 200, "fanout still succeeds with a dead backend")
    rec = last_fanout(json.loads(b)["request_id"])
    if rec:
        dead = [x for x in rec["backends"] if x["name"] == "external-dead"]
        check(dead and dead[0]["result"] == "conn_error", f"dead backend recorded as conn_error {dead}")
    api("DELETE", "/api/backends/external-dead")
    reset()


def test_overrides():
    print("per-request overrides")
    s, h, b = proxy("/o", headers={"x-mb-mode": "fanout"})
    check(s == 200 and h.get("x-mb-route") == "fanout" and h.get("x-mb-fanout-backends") == "4", "x-mb-mode: fanout in round_robin mode")
    for n in ALL:
        s, h, _ = proxy("/o", headers={"x-mb-backend": n})
        check(s == 200 and h.get("x-backend-name") == n, f"x-mb-backend: {n}")
    api("PUT", "/api/mode", {"mode": "fanout"})
    s, h, _ = proxy("/o", headers={"x-mb-mode": "balance"})
    check(s == 200 and h.get("x-mb-route", "").startswith("balance:"), "x-mb-mode: balance in fanout mode")
    reset()


class Load(threading.Thread):
    """Continuous traffic; counts every non-2xx response or error."""

    def __init__(self, workers=8, path="/load"):
        super().__init__(daemon=True)
        self.stop = threading.Event()
        self.ok = 0
        self.bad = collections.Counter()
        self.lock = threading.Lock()
        self.workers = workers
        self.path = path

    def worker(self):
        while not self.stop.is_set():
            try:
                s, h, _ = proxy(self.path)
                key = None if 200 <= s < 300 else f"{s} {h.get('x-mb-route')}"
            except Exception as e:  # noqa: BLE001
                key = type(e).__name__
            with self.lock:
                if key:
                    self.bad[key] += 1
                else:
                    self.ok += 1

    def run(self):
        ts = [threading.Thread(target=self.worker, daemon=True) for _ in range(self.workers)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()


def test_zero_downtime():
    print("zero downtime while reconfiguring under load")
    load = Load()
    load.start()
    time.sleep(1)
    steps = [
        ("PUT", "/api/mode", {"mode": "weighted"}),
        ("PUT", "/api/weights", {"python": 1, "go": 5, "java": 2, "rust": 0}),
        ("PUT", "/api/mode", {"mode": "fanout"}),
        ("POST", "/api/backends", {"name": "python2", "runtime": "python", "weight": 3}),
        ("PUT", "/api/mode", {"mode": "weighted"}),
        ("PATCH", "/api/backends/go", {"enabled": False}),
        ("DELETE", "/api/backends/python2", None),
        ("PUT", "/api/mode", {"mode": "round_robin"}),
        ("POST", "/api/backends", {"name": "rust2", "runtime": "rust"}),
        ("DELETE", "/api/backends/rust2", None),
        ("PATCH", "/api/backends/go", {"enabled": True}),
    ]
    for method, path, body in steps:
        t = time.time()
        s, r = api(method, path, body)
        check(s in (200, 201), f"{method} {path} {json.dumps(body) if body else ''} -> {s} in {(time.time()-t)*1000:.0f} ms")
        time.sleep(1)
    time.sleep(6)  # outlast drain of removed backends
    load.stop.set()
    load.join()
    check(load.ok > 500 and not load.bad, f"{load.ok} requests ok, failures: {dict(load.bad)}")
    reset()


def test_add_remove():
    print("add / remove backends")
    s, r = api("POST", "/api/backends", {"name": "java2", "runtime": "java", "weight": 1})
    check(s == 201 and r["backend"]["port"] == 8005, f"add java2 -> {s} port {r.get('backend', {}).get('port')}")
    c = distribution(100)
    check(c["java2"] > 5, f"java2 receives traffic {dict(c)}")
    s, b = api("GET", "/api/backends/java2")
    check(s == 200 and b["process"]["managed"] and b["envoy"]["healthy"], "java2 process managed + healthy")
    s, _ = api("POST", "/api/backends", {"name": "java2", "runtime": "java"})
    check(s == 400, "duplicate name rejected")
    s, _ = api("POST", "/api/backends", {"name": "x", "runtime": "cobol"})
    check(s == 400, "unknown runtime rejected")
    s, _ = api("DELETE", "/api/backends/java2")
    check(s == 200, "delete java2")
    c = distribution(40)
    check(c["java2"] == 0 and sum(c.values()) == 40, f"java2 gone {dict(c)}")


def test_stats():
    print("statistics")
    time.sleep(1)  # let earlier access-log lines flush before resetting
    api("DELETE", "/api/stats")
    distribution(40)
    api("PUT", "/api/mode", {"mode": "fanout"})
    for _ in range(5):
        proxy("/s?delay_ms.python=30")
    time.sleep(1)
    s, st = api("GET", "/api/stats")
    check(st["by_mode"].get("round_robin") == 40 and st["by_mode"].get("fanout") == 5, f"by_mode {st['by_mode']}")
    py = st["backends"]["python"]
    check(py["by_mode"].get("fanout") == 5 and 8 <= py["by_mode"].get("round_robin", 0) <= 12,
          f"python counted in both modes {py['by_mode']}")
    check(sum(b["fanout_wins"] for b in st["backends"].values()) == 5, "5 fanout wins attributed")
    check(py["latency_ms"]["max"] >= 30, f"latency recorded {py['latency_ms']}")
    s, recs = api("GET", "/api/requests?limit=3&mode=round_robin")
    check(len(recs) == 3 and len(recs[0]["backends"]) == 1 and recs[0]["backends"][0]["name"] in ALL, "balanced request records name the backend")
    reset()


def main():
    global PROXY, API
    ap = argparse.ArgumentParser()
    ap.add_argument("--proxy", default="http://localhost:8080")
    ap.add_argument("--api", default="http://localhost:9000")
    ap.add_argument("--only", nargs="*")
    a = ap.parse_args()
    PROXY, API = a.proxy.rstrip("/"), a.api.rstrip("/")
    for _ in range(60):
        try:
            if api("GET", "/healthz")[0] == 200:
                break
        except OSError:
            pass
        time.sleep(1)
    check(reset() == 200, "reset to round robin with python, go, java, rust")
    time.sleep(3)  # health checks
    tests = [test_inspect, test_round_robin, test_weighted, test_disable, test_fanout, test_overrides,
             test_add_remove, test_stats, test_zero_downtime]
    for t in tests:
        if not a.only or t.__name__.removeprefix("test_") in a.only:
            t()
    print(f"\n{'FAILED: ' + str(len(FAILURES)) if FAILURES else 'ALL PASSED'}")
    for f in FAILURES:
        print("  -", f)
    sys.exit(1 if FAILURES else 0)


if __name__ == "__main__":
    main()
