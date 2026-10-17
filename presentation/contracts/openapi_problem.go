package contracts

const (
	ProblemResponseSchemaName   = "ProblemResponse"
	ProblemFieldErrorSchemaName = "ProblemFieldError"
	ProblemResponseSchemaRef    = "#/components/schemas/ProblemResponse"
	ProblemFieldErrorSchemaRef  = "#/components/schemas/ProblemFieldError"
)

func ProblemResponseOpenAPISchema() map[string]any {
	return map[string]any{
		"type": "object",
		"required": []string{
			"code",
			"message",
		},
		"properties": map[string]any{
			"code": map[string]any{
				"type": "string",
			},
			"message": map[string]any{
				"type": "string",
			},
			"errors": map[string]any{
				"type": "array",
				"items": map[string]any{
					"$ref": ProblemFieldErrorSchemaRef,
				},
			},
		},
	}
}

func ProblemFieldErrorOpenAPISchema() map[string]any {
	return map[string]any{
		"type": "object",
		"required": []string{
			"code",
			"message",
		},
		"properties": map[string]any{
			"code": map[string]any{
				"type": "string",
			},
			"message": map[string]any{
				"type": "string",
			},
		},
	}
}

func ProblemOpenAPIResponse(description string) map[string]any {
	return map[string]any{
		"description": description,
		"content": map[string]any{
			"application/json": map[string]any{
				"schema": map[string]any{
					"$ref": ProblemResponseSchemaRef,
				},
			},
		},
	}
}
