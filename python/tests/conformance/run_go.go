// Go adapter for the cross-SDK conformance harness (Plan 13.6-07).
//
// Reads scenario.json, registers the payload, replays events, queries gets,
// prints {"sdk":"go", "results":[...]} to stdout.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	beava "github.com/beava-dev/beava/sdk/go"
)

type Scenario struct {
	RegisterPayload map[string]any   `json:"register_payload"`
	Events          []map[string]any `json:"events"`
	Gets            []map[string]any `json:"gets"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: run_go.go <scenario.json>")
		os.Exit(2)
	}
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var s Scenario
	if err := json.Unmarshal(raw, &s); err != nil {
		panic(err)
	}

	ctx := context.Background()
	app, err := beava.NewApp(ctx, "", beava.WithTestMode())
	if err != nil {
		panic(err)
	}
	defer app.Close(ctx)

	nodes, _ := s.RegisterPayload["nodes"].([]any)
	descs := make([]beava.Descriptor, len(nodes))
	for i, n := range nodes {
		descs[i] = n.(map[string]any)
	}
	if _, err := app.Register(ctx, descs); err != nil {
		panic(err)
	}

	for _, ev := range s.Events {
		eventName, _ := ev["event_name"].(string)
		fields, _ := ev["fields"].(map[string]any)
		if _, err := app.Push(ctx, eventName, fields); err != nil {
			panic(err)
		}
	}

	results := make([]beava.FeatureResult, 0, len(s.Gets))
	for _, g := range s.Gets {
		table := g["table"].(string)
		keyVal := g["key"]
		if keyVal == "" {
			r, err := app.GetGlobal(ctx, table)
			if err != nil {
				panic(err)
			}
			results = append(results, r)
		} else {
			r, err := app.Get(ctx, table, keyVal)
			if err != nil {
				panic(err)
			}
			results = append(results, r)
		}
	}

	out, _ := json.Marshal(map[string]any{"sdk": "go", "results": results})
	_, _ = os.Stdout.Write(out)
}
