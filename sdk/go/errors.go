package beava

import "fmt"

// ValidationError is one entry in RegistrationError.Errors.
type ValidationError struct {
	Kind    string `json:"kind"`
	Path    string `json:"path"`
	Message string `json:"message"`
}

// RegistrationError surfaces structured server errors (register / push / get).
type RegistrationError struct {
	Code    string            `json:"code"`
	Path    string            `json:"path,omitempty"`
	Message string            `json:"message"`
	Errors  []ValidationError `json:"errors,omitempty"`
}

func (e *RegistrationError) Error() string {
	return fmt.Sprintf("beava: %s [%s]: %s", e.Code, e.Path, e.Message)
}

// BinaryNotFoundError is raised when embed-mode binary discovery fails.
type BinaryNotFoundError struct {
	Searched []string
	Reason   string
}

func (e *BinaryNotFoundError) Error() string {
	return fmt.Sprintf("beava: binary not found (%s); searched: %v", e.Reason, e.Searched)
}
