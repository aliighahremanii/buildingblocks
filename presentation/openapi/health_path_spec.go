package openapi

func HealthPathSpec() map[string]any {
	return map[string]any{
		"get": map[string]any{
			"operationId": "healthCheck",
			"tags":        []string{"Operations"},
			"summary":     "Service health check",
			"responses": map[string]any{
				"200": map[string]any{
					"description": "Service is healthy.",
					"content": map[string]any{
						"application/json": map[string]any{
							"schema": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"status": map[string]any{
										"type":    "string",
										"example": "pong",
									},
								},
								"required": []string{"status"},
							},
						},
					},
				},
			},
		},
	}
}
