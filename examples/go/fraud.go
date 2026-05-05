// Fraud demo: high-cardinality velocity + sketch + geo aggregations
// (transaction velocity, unique merchants, geo velocity for impossible-travel).
//
// Mirrors crates/beava-bench/configs/fraud-team.json shape -- 5 event types,
// 5 group-by axes -- using Polars-renamed op names per ADR-002 (mean / n_unique
// / quantile).
//
// The mock supports count/sum/mean/min/max. Sketches (n_unique, quantile,
// top_k), decays, and geo ops are no-ops here; they're shown for shape so
// the registered surface mirrors the real benchmark.

package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
)

// Inline mockApp.

type aggSpec struct{ op, field string }
type descriptor struct {
	name, kind, source string
	keyCols            []string
	ops                map[string]aggSpec
}
type mockApp struct {
	regs   []descriptor
	tables map[string]map[string]map[string]float64
}

func newMockApp() *mockApp {
	return &mockApp{tables: make(map[string]map[string]map[string]float64)}
}
func (a *mockApp) register(descs ...descriptor) { a.regs = append(a.regs, descs...) }
func (a *mockApp) push(eventName string, fields map[string]float64, strFields map[string]string) {
	for _, d := range a.regs {
		if d.kind != "table" || d.source != eventName {
			continue
		}
		parts := make([]string, len(d.keyCols))
		for i, k := range d.keyCols {
			parts[i] = strFields[k]
		}
		key := strings.Join(parts, "|")
		for f, agg := range d.ops {
			if _, ok := a.tables[d.name]; !ok {
				a.tables[d.name] = make(map[string]map[string]float64)
			}
			if _, ok := a.tables[d.name][key]; !ok {
				a.tables[d.name][key] = make(map[string]float64)
			}
			row := a.tables[d.name][key]
			v := fields[agg.field]
			switch agg.op {
			case "count":
				row[f]++
			case "sum":
				row[f] += v
			case "mean":
				row[f+"__sum"] += v
				row[f+"__cnt"]++
				row[f] = row[f+"__sum"] / row[f+"__cnt"]
			case "min":
				if _, seen := row[f+"__seen"]; !seen || v < row[f] {
					row[f] = v
					row[f+"__seen"] = 1
				}
			case "max":
				if _, seen := row[f+"__seen"]; !seen || v > row[f] {
					row[f] = v
					row[f+"__seen"] = 1
				}
			}
		}
	}
}
func (a *mockApp) get(table, key string) map[string]float64 {
	if t, ok := a.tables[table]; ok {
		if row, ok := t[key]; ok {
			return row
		}
	}
	return map[string]float64{}
}

// Demo body.

func main() {
	_ = context.Background()
	app := newMockApp()

	// 5 event types mirror fraud-team.json.
	Txn := descriptor{name: "Txn", kind: "event"}
	Login := descriptor{name: "Login", kind: "event"}
	Signup := descriptor{name: "Signup", kind: "event"}
	CardAdd := descriptor{name: "CardAdd", kind: "event"}
	Refund := descriptor{name: "Refund", kind: "event"}

	// The user-axis aggregation table is the busiest in fraud detection.
	// n_unique / quantile / geo_velocity are real-engine ops shown for
	// shape; the mock no-ops them.
	UserFraudStats := descriptor{
		name: "UserFraudStats", kind: "table", source: "Txn",
		keyCols: []string{"user_id"},
		ops: map[string]aggSpec{
			"tx_count_1h":            {"count", ""},
			"tx_sum_1h":              {"sum", "amount"},
			"tx_mean_1h":             {"mean", "amount"},
			"tx_min_1h":              {"min", "amount"},
			"tx_max_1h":              {"max", "amount"},
			"tx_unique_merchants_1h": {"n_unique", "merchant_id"}, // mock no-op
			"tx_p99_1h":              {"quantile", "amount"},      // mock no-op
			"tx_geo_velocity_1h":     {"geo_velocity", ""},        // mock no-op
		},
	}
	app.register(Txn, Login, Signup, CardAdd, Refund, UserFraudStats)

	// 10 transactions for 'alice'.
	type txn struct {
		amount   float64
		merchant string
	}
	txns := []txn{
		{12.50, "amazon"},
		{150.00, "amazon"},
		{89.99, "ebay"},
		{1500.00, "fancy_store"},
		{5.00, "starbucks"},
		{35.00, "amazon"},
		{220.00, "ebay"},
		{10.00, "starbucks"},
		{45.00, "amazon"},
		{12.00, "starbucks"},
	}
	for _, t := range txns {
		app.push("Txn",
			map[string]float64{"amount": t.amount, "lat": 37.7749, "lon": -122.4194},
			map[string]string{
				"user_id":     "alice",
				"card_fp":     "card_001",
				"merchant_id": t.merchant,
				"ip_address":  "203.0.113.42",
				"device_id":   "phone_xyz",
			},
		)
	}

	result := app.get("UserFraudStats", "alice")
	fmt.Printf("alice fraud stats: %v\n", result)

	check := func(name string, got, want, tol float64) {
		if math.Abs(got-want) > tol {
			fmt.Fprintf(os.Stderr, "FAIL %s: got %v want %v\n", name, got, want)
			os.Exit(1)
		}
	}
	expectedSum := 0.0
	for _, t := range txns {
		expectedSum += t.amount
	}
	check("tx_count", result["tx_count_1h"], 10, 1e-6)
	check("tx_sum", result["tx_sum_1h"], expectedSum, 1e-3)
	check("tx_mean", result["tx_mean_1h"], expectedSum/10, 1e-3)
	check("tx_min", result["tx_min_1h"], 5.00, 1e-6)
	check("tx_max", result["tx_max_1h"], 1500.00, 1e-6)
	// n_unique / quantile / geo_velocity: not asserted (mock no-ops).

	fmt.Println("OK -- fraud.go")
}
