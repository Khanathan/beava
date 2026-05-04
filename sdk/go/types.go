package beava

// Descriptor is an opaque pre-compiled register node (event / table / derivation).
// Per Phase 13.6 communicate-only scope, the Go SDK does not author descriptors;
// users supply pre-compiled JSON authored by the Python SDK or hand-written.
type Descriptor = map[string]any

// FeatureResult is the row-shape returned by Get / per-entry of BatchGet.
// Cold-start returns an empty map (not nil and not an error).
type FeatureResult = map[string]any

// RegisterResult mirrors wire-spec OP_REGISTER response.
type RegisterResult struct {
	Status          string   `json:"status"`
	RegistryVersion int64    `json:"registry_version"`
	Added           []string `json:"added,omitempty"`
	Removed         []string `json:"removed,omitempty"`
	Changed         []string `json:"changed,omitempty"`
}

// PushResult mirrors wire-spec OP_PUSH response.
type PushResult struct {
	AckLsn          int64 `json:"ack_lsn,omitempty"`
	RegistryVersion int64 `json:"registry_version"`
}

// PingResult mirrors wire-spec OP_PING response.
type PingResult struct {
	ServerVersion   string `json:"server_version"`
	RegistryVersion int64  `json:"registry_version"`
}

// GetRequest is one entry in a BatchGet request slice.
type GetRequest struct {
	Table    string   `json:"table"`
	Key      any      `json:"key"`
	Features []string `json:"features,omitempty"`
}

// RegisterOption flags for App.Register.
type RegisterOption func(*registerConfig)

type registerConfig struct {
	force  bool
	dryRun bool
}

// WithForce permits destructive register changes.
func WithForce() RegisterOption {
	return func(c *registerConfig) { c.force = true }
}

// WithDryRun returns the diff without applying.
func WithDryRun() RegisterOption {
	return func(c *registerConfig) { c.dryRun = true }
}
