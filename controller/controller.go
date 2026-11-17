package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Controller owns the routing configuration and applies changes to Envoy, the backend processes
// and the fanout proxy. All changes are serialized.
type Controller struct {
	srv     ServerSettings
	mu      sync.Mutex
	cur     atomic.Pointer[RoutingConfig]
	xds     *XDS
	envoy   *Envoy
	sup     *Supervisor
	stats   *Stats
	envoyUp atomic.Bool
	hosts   sync.Map // "addr:port" -> backend name (kept for removed backends too)
}

func (c *Controller) Current() RoutingConfig { return *c.cur.Load() }

func (c *Controller) remember(rc RoutingConfig) {
	for _, b := range rc.Backends {
		c.hosts.Store(b.HostPort(), b.Name)
	}
}

func (c *Controller) backendNameFor(host, cluster string) string {
	if v, ok := c.hosts.Load(host); ok {
		return v.(string)
	}
	if len(cluster) > 2 && cluster[:2] == "b_" {
		return cluster[2:]
	}
	return host
}

// ApplyError is returned when a change was validated but could not be confirmed by Envoy.
type ApplyError struct{ Err error }

func (e ApplyError) Error() string { return e.Err.Error() }

// ValidationError marks client errors (HTTP 400).
type ValidationError struct{ Err error }

func (e ValidationError) Error() string { return e.Err.Error() }

func backendsUnion(a, b []Backend) []Backend {
	seen := map[string]bool{}
	var out []Backend
	for _, x := range append(append([]Backend{}, b...), a...) {
		if !seen[x.Name] {
			seen[x.Name] = true
			out = append(out, x)
		}
	}
	return out
}

// procIdentity: fields which, if changed, would require restarting/re-addressing a backend.
func procIdentity(b Backend) []any { return []any{b.Address, b.Port, b.Command, b.Env, b.Runtime} }

// Update applies fn to a copy of the current config and rolls the result out without downtime:
//  1. start + health-check any new backend processes
//  2. publish clusters for old∪new backends, the new endpoints/weights and the new routes
//  3. once Envoy confirms, drop clusters of removed backends and stop their processes after draining
func (c *Controller) Update(fn func(*RoutingConfig) error) (RoutingConfig, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.Current()
	next := cur.clone()
	if err := fn(&next); err != nil {
		return cur, ValidationError{err}
	}
	normalize(&next)
	if err := validate(&next); err != nil {
		return cur, ValidationError{err}
	}

	oldByName := map[string]Backend{}
	for _, b := range cur.Backends {
		oldByName[b.Name] = b
	}
	newNames := map[string]bool{}
	var added []Backend
	for _, b := range next.Backends {
		newNames[b.Name] = true
		if old, ok := oldByName[b.Name]; ok {
			if !reflect.DeepEqual(procIdentity(old), procIdentity(b)) {
				return cur, ValidationError{fmt.Errorf("backend %q: address/port/command/runtime/env cannot be changed in place; remove it and add it again", b.Name)}
			}
			continue
		}
		added = append(added, b)
	}
	var removed []Backend
	for _, b := range cur.Backends {
		if !newNames[b.Name] {
			removed = append(removed, b)
		}
	}

	// 1. start new processes and wait until they are healthy before routing to them.
	for _, b := range added {
		if !b.Managed() {
			continue
		}
		if c.sup.Running(b.Name) {
			return cur, ValidationError{fmt.Errorf("backend %q is still shutting down, retry shortly", b.Name)}
		}
		c.sup.Start(b)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		err := waitHealthy(ctx, b, next.HealthCheck.Path)
		cancel()
		if err != nil {
			for _, a := range added {
				c.sup.Stop(a.Name)
			}
			return cur, ApplyError{err}
		}
	}

	// 2. publish.
	next.Version = cur.Version + 1
	if err := c.publish(next, backendsUnion(cur.Backends, next.Backends)); err != nil {
		c.cur.Store(&next)
		return next, err
	}
	c.remember(next)
	c.cur.Store(&next)

	// 3. retire removed backends.
	if len(removed) > 0 {
		next.Version++
		err := c.publish(next, next.Backends)
		c.cur.Store(&next)
		drain := time.Duration(c.srv.DrainSeconds) * time.Second
		for _, b := range removed {
			go func(b Backend) {
				time.Sleep(drain)
				c.sup.Stop(b.Name)
			}(b)
		}
		if err != nil {
			return next, err
		}
	}
	// 4. new members of the balanced pool only take traffic once Envoy's own health check passed;
	//    wait for that so that a successful API response means the change is fully in effect.
	if c.envoyUp.Load() {
		before := poolMembers(cur)
		var fresh []string
		for _, h := range poolMembers(next) {
			if !contains(before, h) {
				fresh = append(fresh, h)
			}
		}
		if len(fresh) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(3*next.HealthCheck.IntervalMs+1000)*time.Millisecond)
			if err := c.envoy.WaitPoolHealthy(ctx, fresh); err != nil {
				logEvent("warn", "configuration applied but some backends are not healthy yet", "version", next.Version, "err", err)
			}
			cancel()
		}
	}
	logEvent("info", "configuration applied", "version", next.Version, "mode", next.Mode, "backends", summaryOf(next))
	return next, nil
}

func (c *Controller) publish(rc RoutingConfig, clusterBackends []Backend) error {
	if err := c.xds.write(rc, clusterBackends); err != nil {
		return ApplyError{fmt.Errorf("writing xDS files: %w", err)}
	}
	if !c.envoyUp.Load() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.envoy.WaitApplied(ctx, rc.Version); err != nil {
		return ApplyError{err}
	}
	return nil
}

func summaryOf(rc RoutingConfig) []string {
	out := []string{}
	for _, b := range rc.Backends {
		s := b.Name + ":w" + strconv.Itoa(b.W())
		if !b.IsEnabled() {
			s += ":disabled"
		}
		out = append(out, s)
	}
	return out
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func itoa(i int) string { return strconv.Itoa(i) }

// poolMembers lists the addresses that the balanced pool should contain for rc.
func poolMembers(rc RoutingConfig) []string {
	out := []string{}
	for _, b := range rc.Backends {
		if b.IsEnabled() && (rc.Mode != ModeWeighted || b.W() > 0) {
			out = append(out, b.HostPort())
		}
	}
	return out
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
