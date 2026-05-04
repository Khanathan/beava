// Adtech demo: impression event type; campaign aggregations
// (impressions per campaign, sum of bid amounts, mean bid).
//
// Phase 13.0 runs against an inlined mockApp helper struct (Option A
// per WARNING 8 -- _mock.go carries //go:build exclude and is reference
// code only, so each demo is a self-contained `go run` target).
//
// Phase 13.6 swaps the inline mockApp for the real `beava-go` client
// (see docs/sdk-api/go.md for the v0-target Go SDK shape).

package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
)

// --- Inline mockApp (~30 LOC, duplicated across demos per Option A).
// Computes count/sum/mean/min/max via push -- no pre-seeding (BLOCKER 4 fix).

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

// --- Demo body.

func main() {
	_ = context.Background()
	app := newMockApp()
	Impression := descriptor{name: "Impression", kind: "event"}
	CampaignStats := descriptor{
		name: "CampaignStats", kind: "table", source: "Impression",
		keyCols: []string{"campaign_id"},
		ops: map[string]aggSpec{
			"impressions_1h": {"count", ""},
			"bid_sum_1h":     {"sum", "bid"},
			"bid_mean_1h":    {"mean", "bid"},
			"bid_min_1h":     {"min", "bid"},
			"bid_max_1h":     {"max", "bid"},
		},
	}
	app.register(Impression, CampaignStats)
	fmt.Printf("Registered %d descriptors\n", len(app.regs))

	type evt struct{ camp, creative string; bid float64 }
	events := []evt{
		{"c1", "cr1", 0.50},
		{"c1", "cr1", 0.75},
		{"c1", "cr2", 1.00},
		{"c2", "cr3", 0.25},
		{"c2", "cr3", 0.40},
	}
	for _, e := range events {
		app.push("Impression",
			map[string]float64{"bid": e.bid},
			map[string]string{"campaign_id": e.camp, "creative_id": e.creative},
		)
	}

	c1 := app.get("CampaignStats", "c1")
	c2 := app.get("CampaignStats", "c2")
	fmt.Printf("Campaign c1: %v\n", c1)
	fmt.Printf("Campaign c2: %v\n", c2)

	// Assertions on COMPUTED values.
	check := func(name string, got, want, tol float64) {
		if math.Abs(got-want) > tol {
			fmt.Fprintf(os.Stderr, "FAIL %s: got %v want %v\n", name, got, want)
			os.Exit(1)
		}
	}
	check("c1 impressions", c1["impressions_1h"], 3, 1e-6)
	check("c1 bid_sum", c1["bid_sum_1h"], 2.25, 1e-6)
	check("c1 bid_mean", c1["bid_mean_1h"], 0.75, 1e-6)
	check("c1 bid_min", c1["bid_min_1h"], 0.50, 1e-6)
	check("c1 bid_max", c1["bid_max_1h"], 1.00, 1e-6)
	check("c2 impressions", c2["impressions_1h"], 2, 1e-6)
	check("c2 bid_sum", c2["bid_sum_1h"], 0.65, 1e-6)

	fmt.Println("OK -- adtech.go")
}
