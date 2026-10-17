package contracts

// ProblemResponse is returned for HTTP error responses.
type ProblemResponse struct {
	Code    string              `json:"code"`
	Message string              `json:"message"`
	Errors  []ProblemFieldError `json:"errors,omitempty"`
}

// ProblemFieldError is a single field-level error inside a ProblemResponse.
type ProblemFieldError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
