package openapi

// Shared OpenAPI 3 helper builders used by every module spec. Each module's
// presentation/openapi package keeps only its own schemas and paths; the
// generic builders (refs, parameters, responses, operations) live here.

// Ref returns a JSON $ref into #/components/schemas/{name}.
func Ref(name string) map[string]any {
	return map[string]any{"$ref": "#/components/schemas/" + name}
}

// PathUUID returns a required path parameter typed as a UUID string.
func PathUUID(name string) map[string]any {
	return map[string]any{
		"name":     name,
		"in":       "path",
		"required": true,
		"schema":   map[string]any{"type": "string", "format": "uuid"},
	}
}

// QueryIntParam returns an integer query parameter. desc and def are emitted
// only when non-empty / non-zero so callers that omit them get the minimal
// shape.
func QueryIntParam(name, desc string, def int, required bool) map[string]any {
	schema := map[string]any{"type": "integer"}
	if def != 0 {
		schema["default"] = def
	}
	m := map[string]any{
		"name":     name,
		"in":       "query",
		"required": required,
		"schema":   schema,
	}
	if desc != "" {
		m["description"] = desc
	}
	return m
}

// QueryStringParam returns a string query parameter. desc is emitted only when
// non-empty.
func QueryStringParam(name, desc string) map[string]any {
	m := map[string]any{
		"name":     name,
		"in":       "query",
		"required": false,
		"schema":   map[string]any{"type": "string"},
	}
	if desc != "" {
		m["description"] = desc
	}
	return m
}

// PaginationParams returns the standard page/pageSize/sortBy/sortOrder query
// parameters shared by every search endpoint.
func PaginationParams() []map[string]any {
	return []map[string]any{
		QueryIntParam("page", "", 0, false),
		QueryIntParam("pageSize", "", 0, false),
		QueryStringParam("sortBy", ""),
		QueryStringParam("sortOrder", ""),
	}
}

// JSONContent wraps a schema in an application/json content map.
func JSONContent(schema map[string]any) map[string]any {
	return map[string]any{"application/json": map[string]any{"schema": schema}}
}

// RequestBody wraps a schema as a required JSON request body.
func RequestBody(schema map[string]any) map[string]any {
	return map[string]any{"required": true, "content": JSONContent(schema)}
}

// BodyFromRef returns a required request body referencing a named schema.
func BodyFromRef(name string) map[string]any {
	return RequestBody(Ref(name))
}

// EmptyBody returns a required request body wrapping an empty object schema.
func EmptyBody() map[string]any {
	return RequestBody(map[string]any{"type": "object"})
}

// OKResponse returns a 200-style success response body referencing a named
// schema.
func OKResponse(schema string) map[string]any {
	return map[string]any{"description": "Success.", "content": JSONContent(Ref(schema))}
}

// CreatedResponse returns a created response body referencing a named schema.
func CreatedResponse(schema string) map[string]any {
	return map[string]any{"description": "Created.", "content": JSONContent(Ref(schema))}
}

// NoContentResponse returns a description-only no-content response.
func NoContentResponse() map[string]any {
	return map[string]any{"description": "No content."}
}

// ProblemResponseRef returns a response body referencing the Problem schema.
func ProblemResponseRef(description string) map[string]any {
	return map[string]any{"description": description, "content": JSONContent(Ref("Problem"))}
}

// ReadResponses returns the standard response map for a read endpoint.
func ReadResponses(schema string) map[string]any {
	return map[string]any{
		"200": OKResponse(schema),
		"404": ProblemResponseRef("Not found."),
		"422": ProblemResponseRef("Validation error."),
	}
}

// CreatedResponses returns the standard response map for a create endpoint.
// Includes 409 Conflict because Register-style commands now use
// concurrency.NoStream(), which surfaces as a typed ConcurrencyError (HTTP
// 409) when a stream already exists for the supplied aggregate id — e.g.
// a retried POST whose first attempt already persisted.
func CreatedResponses(schema string) map[string]any {
	return map[string]any{
		"201": CreatedResponse(schema),
		"404": ProblemResponseRef("Not found."),
		"409": ProblemResponseRef("Concurrency conflict: the aggregate was modified by another command. Reload and retry."),
		"422": ProblemResponseRef("Validation error."),
	}
}

// ActionResponses returns the standard 204 response map for an action endpoint.
// Includes 409 Conflict because action commands now use
// concurrency.AtRevision(rev), which surfaces as a typed ConcurrencyError
// (HTTP 409) when another command committed to the same aggregate between
// the load and the save.
func ActionResponses() map[string]any {
	return map[string]any{
		"204": NoContentResponse(),
		"404": ProblemResponseRef("Not found."),
		"409": ProblemResponseRef("Concurrency conflict: the aggregate was modified by another command. Reload and retry."),
		"422": ProblemResponseRef("Validation error."),
	}
}

// ActionOperation builds a POST operation on a path that always carries an {id}
// path parameter. extraParams (e.g. nested {terminalId}) are appended after the
// id parameter. A nil body omits the requestBody field.
func ActionOperation(operationID, tag, summary string, body map[string]any, responses map[string]any, extraParams ...map[string]any) map[string]any {
	params := append([]map[string]any{PathUUID("id")}, extraParams...)
	op := map[string]any{
		"operationId": operationID,
		"tags":        []string{tag},
		"summary":     summary,
		"parameters":  params,
		"responses":   responses,
	}
	if body != nil {
		op["requestBody"] = body
	}
	return op
}

// ProblemSchema returns the canonical Problem response object schema.
func ProblemSchema() map[string]any {
	return map[string]any{
		"type":     "object",
		"required": []string{"code", "message"},
		"properties": map[string]any{
			"code":    map[string]any{"type": "string"},
			"message": map[string]any{"type": "string"},
			"fields":  map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		},
	}
}

// MultilingualNameSchema returns the local/intl multilingual name schema.
func MultilingualNameSchema() map[string]any {
	return map[string]any{
		"type":     "object",
		"required": []string{"local", "intl"},
		"properties": map[string]any{
			"local": map[string]any{"type": "string"},
			"intl":  map[string]any{"type": "string"},
		},
	}
}

// PagedResponse returns the standard paged response wrapper schema referencing
// the named item schema.
func PagedResponse(itemRef string) map[string]any {
	return map[string]any{
		"type":     "object",
		"required": []string{"items", "total", "page", "pageSize", "totalPages"},
		"properties": map[string]any{
			"items":      map[string]any{"type": "array", "items": Ref(itemRef)},
			"total":      map[string]any{"type": "integer", "format": "int64"},
			"page":       map[string]any{"type": "integer"},
			"pageSize":   map[string]any{"type": "integer"},
			"totalPages": map[string]any{"type": "integer"},
		},
	}
}
