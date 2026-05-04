//go:build exclude

// Phase 13.0 reference mock backend for Go SDK porters (Phase 13.6 build target).
// Carries //go:build exclude -- this file does NOT participate in any binary
// build during 13.0. Demo files (adtech.go, fraud.go, ecommerce.go) inline
// their own mockApp struct so each demo is a self-contained `go run` target.
//
// Per WARNING 8 fix (Option A) -- all 4 Go files declare package main; this
// _mock.go carries the build-exclude directive so it stays as reference code
// only. Trade-off: ~30 LOC duplication across the 3 demos; gain: zero-config
// `go run examples/go/<demo>.go`.
//
// Replaced by real beava-go client in Phase 13.6.
//
// Per Q2 locked answer + BLOCKER 4 checker fix: this mock COMPUTES features
// by applying registered descriptors on push. Demos go through the full
// register -> push -> query flow (no pre-seeding) so contract drift between
// specs and the real engine surfaces immediately at 13.6 re-verification.
//
// Supported ops in this mock (minimum for the 9 vertical demos):
//   - count: increment per matching event
//   - sum: accumulate field value
//   - mean: running sum / count
//   - min, max: comparison
//
// Sketches (NUnique, Quantile, TopK), decays, velocity, and geo ops are
// NOT computed -- demo files document the no-op fallback inline.

package main

import (
	"context"
	"fmt"
	"strings"
)

// AggSpec records one registered aggregation op.
type AggSpec struct {
	Op    string
	Field string // "" if no field (count)
}

// Descriptor is a registered event source or table aggregation.
type Descriptor struct {
	Name    string
	Kind    string // "event" | "table"
	Source  string // source event name (empty for kind="event")
	KeyCols []string
	Ops     map[string]AggSpec
}

// MockApp is the reference Phase 13.0 in-memory shim.
// Demo files inline an equivalent mockApp; this struct exists only as
// the canonical reference for Phase 13.6 SDK porters.
type MockApp struct {
	registered      []Descriptor
	tables          map[string]map[string]map[string]any
	aggState        map[string]map[string]float64
	registryVersion int
}

func NewMockApp() *MockApp {
	return &MockApp{
		registered: nil,
		tables:     make(map[string]map[string]map[string]any),
		aggState:   make(map[string]map[string]float64),
	}
}

// Register adds descriptors to the in-memory registry.
func (a *MockApp) Register(ctx context.Context, descs []Descriptor) (map[string]any, error) {
	a.registered = append(a.registered, descs...)
	a.registryVersion = len(a.registered)
	added := make([]string, 0, len(a.registered))
	for _, d := range a.registered {
		added = append(added, d.Name)
	}
	return map[string]any{
		"status":           "ok",
		"registry_version": a.registryVersion,
		"added":            added,
	}, nil
}

// Push applies registered table descriptors whose source matches the event.
func (a *MockApp) Push(ctx context.Context, eventName string, fields map[string]any) (map[string]any, error) {
	for _, desc := range a.registered {
		if desc.Kind != "table" {
			continue
		}
		if desc.Source != eventName {
			continue
		}
		key := keyFromEvent(desc.KeyCols, fields)
		for featureName, agg := range desc.Ops {
			a.update(desc.Name, key, featureName, agg, fields)
		}
	}
	return map[string]any{
		"ack_lsn":          1,
		"registry_version": a.registryVersion,
	}, nil
}

// Get returns the row-shape feature dict for (table, key).
func (a *MockApp) Get(ctx context.Context, table string, key string) (map[string]any, error) {
	if t, ok := a.tables[table]; ok {
		if row, ok := t[key]; ok {
			return row, nil
		}
	}
	return map[string]any{}, nil
}

// BatchGet fetches multiple (table, key) rows.
func (a *MockApp) BatchGet(ctx context.Context, requests []GetRequest) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(requests))
	for _, r := range requests {
		row, _ := a.Get(ctx, r.Table, r.Key)
		out = append(out, row)
	}
	return out, nil
}

// Reset clears all in-memory state.
func (a *MockApp) Reset(ctx context.Context) error {
	a.tables = make(map[string]map[string]map[string]any)
	a.aggState = make(map[string]map[string]float64)
	return nil
}

// Ping returns mock server info.
func (a *MockApp) Ping(ctx context.Context) (map[string]any, error) {
	return map[string]any{
		"server_version":   "0.0.0-mock",
		"registry_version": a.registryVersion,
	}, nil
}

// Close is a no-op for the mock.
func (a *MockApp) Close(ctx context.Context) error {
	return nil
}

// GetRequest is the batch_get request shape.
type GetRequest struct {
	Table string
	Key   string
}

func (a *MockApp) update(table, key, feature string, agg AggSpec, event map[string]any) {
	stateKey := table + "|" + key + "|" + feature
	state, ok := a.aggState[stateKey]
	if !ok {
		state = make(map[string]float64)
		a.aggState[stateKey] = state
	}
	switch agg.Op {
	case "count":
		state["count"]++
		a.setValue(table, key, feature, state["count"])
	case "sum":
		state["sum"] += getFloat(event, agg.Field)
		a.setValue(table, key, feature, state["sum"])
	case "mean":
		state["sum"] += getFloat(event, agg.Field)
		state["count"]++
		a.setValue(table, key, feature, state["sum"]/state["count"])
	case "min":
		v := getFloat(event, agg.Field)
		if _, seen := state["min_seen"]; !seen {
			state["min"] = v
			state["min_seen"] = 1
		} else if v < state["min"] {
			state["min"] = v
		}
		a.setValue(table, key, feature, state["min"])
	case "max":
		v := getFloat(event, agg.Field)
		if _, seen := state["max_seen"]; !seen {
			state["max"] = v
			state["max_seen"] = 1
		} else if v > state["max"] {
			state["max"] = v
		}
		a.setValue(table, key, feature, state["max"])
	default:
		// Unsupported in mock (sketches, decays, geo): no-op.
	}
}

func (a *MockApp) setValue(table, key, feature string, value any) {
	if _, ok := a.tables[table]; !ok {
		a.tables[table] = make(map[string]map[string]any)
	}
	if _, ok := a.tables[table][key]; !ok {
		a.tables[table][key] = make(map[string]any)
	}
	a.tables[table][key][feature] = value
}

func keyFromEvent(keyCols []string, event map[string]any) string {
	if len(keyCols) == 0 {
		return "_global"
	}
	parts := make([]string, len(keyCols))
	for i, k := range keyCols {
		parts[i] = fmt.Sprintf("%v", event[k])
	}
	return strings.Join(parts, "|")
}

func getFloat(event map[string]any, field string) float64 {
	if field == "" {
		return 0
	}
	v, ok := event[field]
	if !ok || v == nil {
		return 0
	}
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	}
	return 0
}
