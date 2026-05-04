// Stub Go adapter — Task 3.b replaces with the real impl.
package main

import (
	"encoding/json"
	"os"
)

func main() {
	out, _ := json.Marshal(map[string]any{"sdk": "go", "results": []any{}})
	_, _ = os.Stdout.Write(out)
}
