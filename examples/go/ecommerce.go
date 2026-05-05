// E-commerce demo: purchase event type; basket aggregations
// (items per user, mean basket size, total spend).
//
// Each demo inlines its own mockApp helper struct (see _mock.go for the
// canonical reference) so it stays a self-contained `go run` target.
// Swap the inline mockApp for the real `beava-go` client once published.

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

	Purchase := descriptor{name: "Purchase", kind: "event"}
	UserBasket := descriptor{
		name: "UserBasket", kind: "table", source: "Purchase",
		keyCols: []string{"user_id"},
		ops: map[string]aggSpec{
			"items_purchased_1h":     {"sum", "qty"},
			"purchase_count_1h":      {"count", ""},
			"mean_purchase_value_1h": {"mean", "price"},
			"total_spend_1h":         {"sum", "price"},
			"min_price_1h":           {"min", "price"},
			"max_price_1h":           {"max", "price"},
		},
	}
	app.register(Purchase, UserBasket)

	type purchase struct {
		sku   string
		qty   float64
		price float64
	}
	purchases := []purchase{
		{"sku_a", 1, 10.0},
		{"sku_b", 2, 5.0},
		{"sku_a", 1, 10.0},
		{"sku_c", 3, 7.5},
	}
	for _, p := range purchases {
		app.push("Purchase",
			map[string]float64{"qty": p.qty, "price": p.price},
			map[string]string{"user_id": "bob", "sku": p.sku},
		)
	}

	bob := app.get("UserBasket", "bob")
	fmt.Printf("bob basket: %v\n", bob)

	check := func(name string, got, want, tol float64) {
		if math.Abs(got-want) > tol {
			fmt.Fprintf(os.Stderr, "FAIL %s: got %v want %v\n", name, got, want)
			os.Exit(1)
		}
	}
	expectedTotal := 10.0 + 5.0 + 10.0 + 7.5
	check("purchase_count", bob["purchase_count_1h"], 4, 1e-6)
	check("items_purchased", bob["items_purchased_1h"], 7, 1e-6)
	check("total_spend", bob["total_spend_1h"], expectedTotal, 1e-6)
	check("mean_purchase_value", bob["mean_purchase_value_1h"], expectedTotal/4, 1e-6)
	check("min_price", bob["min_price_1h"], 5.0, 1e-6)
	check("max_price", bob["max_price_1h"], 10.0, 1e-6)

	fmt.Println("OK -- ecommerce.go")
}
