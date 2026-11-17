// Command controller is the control plane of the multi-backend container. It
//   - supervises the backend processes (python, go, java, rust, or any command),
//   - starts Envoy and feeds it CDS/EDS/RDS from watched files (hot reload, no downtime),
//   - exposes the routing API (default :9000),
//   - runs the fan-out proxy that sends a request to all backends (first success wins),
//   - records per-backend statistics and writes one JSON log record per request.
package main

import (
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

func main() {
	cfgPath := os.Getenv("MB_CONFIG")
	if cfgPath == "" {
		cfgPath = "/etc/multi-backend/config.yaml"
	}
	srv, rc, err := loadConfig(cfgPath)
	if err != nil {
		logEvent("fatal", "invalid configuration", "err", err, "config", cfgPath)
		os.Exit(2)
	}
	rc.Version = 1
	xdsDir := filepath.Join(srv.StateDir, "xds")
	for _, d := range []string{srv.StateDir, xdsDir, srv.LogDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			logEvent("fatal", "cannot create directory", "dir", d, "err", err)
			os.Exit(1)
		}
	}
	reqLog, err := newRotatingFile(filepath.Join(srv.LogDir, "requests.log"), 100<<20)
	if err != nil {
		logEvent("fatal", "cannot open request log", "err", err)
		os.Exit(1)
	}
	logStdout := srv.LogRequests == nil || *srv.LogRequests
	accessLog := filepath.Join(srv.LogDir, "envoy-access.log")
	if f, err := os.OpenFile(accessLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
		f.Close()
	}

	ctl := &Controller{srv: srv, sup: NewSupervisor()}
	ctl.cur.Store(&rc)
	ctl.remember(rc)
	ctl.stats = NewStats(reqLog, logStdout, nil)
	ctl.xds = &XDS{dir: xdsDir, fanoutPort: srv.FanoutPort}
	ctl.envoy = &Envoy{srv: srv, xdsDir: xdsDir, accessLog: accessLog}

	cfgJSON, _ := json.Marshal(rc)
	logEvent("info", "starting multi-backend", "config", json.RawMessage(cfgJSON), "proxy_port", srv.ProxyPort, "api_port", srv.APIPort)

	if err := ctl.xds.write(rc, rc.Backends); err != nil {
		logEvent("fatal", "cannot write xDS files", "err", err)
		os.Exit(1)
	}
	for _, b := range rc.Backends {
		ctl.sup.Start(b)
	}

	fanout := &http.Server{Addr: "127.0.0.1:" + strconv.Itoa(srv.FanoutPort), Handler: NewFanout(ctl), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := fanout.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logEvent("fatal", "fanout listener failed", "err", err)
			os.Exit(1)
		}
	}()
	api := &http.Server{Addr: ":" + strconv.Itoa(srv.APIPort), Handler: (&API{ctl: ctl}).Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := api.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logEvent("fatal", "api listener failed", "err", err)
			os.Exit(1)
		}
	}()

	if err := ctl.envoy.Start(); err != nil {
		logEvent("fatal", "cannot start envoy", "err", err)
		ctl.sup.StopAll()
		os.Exit(1)
	}
	if err := ctl.envoy.WaitReady(60 * time.Second); err != nil {
		logEvent("fatal", "envoy did not become ready", "err", err)
		ctl.envoy.Stop()
		ctl.sup.StopAll()
		os.Exit(1)
	}
	ctl.envoyUp.Store(true)
	logEvent("info", "ready", "proxy", "http://0.0.0.0:"+strconv.Itoa(srv.ProxyPort), "api", "http://0.0.0.0:"+strconv.Itoa(srv.APIPort))

	go (&AccessLogTailer{path: accessLog, ctl: ctl, maxSize: 100 << 20}).Run()
	if srv.StatsLogSeconds > 0 {
		go func() {
			for range time.Tick(time.Duration(srv.StatsLogSeconds) * time.Second) {
				s := ctl.stats.Snapshot()
				s["type"] = "stats"
				s["mode"] = ctl.Current().Mode
				s["config_version"] = ctl.Current().Version
				emit(s)
			}
		}()
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	select {
	case s := <-sig:
		logEvent("info", "shutting down", "signal", s.String())
		ctl.envoy.Stop()
		ctl.sup.StopAll()
		os.Exit(0)
	case err := <-ctl.envoy.exited:
		logEvent("fatal", "envoy exited unexpectedly", "err", err)
		ctl.sup.StopAll()
		os.Exit(1)
	}
}
