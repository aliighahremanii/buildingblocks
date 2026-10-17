package openapi

import "testing"

func TestHealthPathSpec(t *testing.T) {
	t.Parallel()

	spec := HealthPathSpec()

	get, ok := spec["get"].(map[string]any)
	if !ok {
		t.Fatal("expected get operation")
	}
	if get["operationId"] != "healthCheck" {
		t.Fatalf("expected operationId healthCheck, got %#v", get["operationId"])
	}

	responses, ok := get["responses"].(map[string]any)
	if !ok {
		t.Fatal("expected responses")
	}
	okResponse, ok := responses["200"].(map[string]any)
	if !ok {
		t.Fatal("expected 200 response")
	}
	content, ok := okResponse["content"].(map[string]any)
	if !ok {
		t.Fatal("expected response content")
	}
	jsonContent, ok := content["application/json"].(map[string]any)
	if !ok {
		t.Fatal("expected application/json content")
	}
	schema, ok := jsonContent["schema"].(map[string]any)
	if !ok {
		t.Fatal("expected response schema")
	}
	properties, ok := schema["properties"].(map[string]any)
	if !ok {
		t.Fatal("expected schema properties")
	}
	status, ok := properties["status"].(map[string]any)
	if !ok {
		t.Fatal("expected status property")
	}
	if status["example"] != "pong" {
		t.Fatalf("expected status example pong, got %#v", status["example"])
	}
}
