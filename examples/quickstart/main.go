// Command quickstart is a minimal, compiling demonstration of the toolkit's
// core composition: a typed command handler, typed middleware, and the
// validation/concurrency error types a consumer maps to HTTP.
//
// It is deliberately dependency-free (no datastore, no network) so it runs
// anywhere:
//
//	go run ./examples/quickstart
//
// The point it makes is the toolkit's central design choice: commands are
// typed end to end. There is no command bus, no handler registry, and no
// `Dispatch(ctx, any)` — the middleware chain is composed explicitly and the
// compiler checks it.
package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/aliighahremanii/buildingblocks/application/commands"
	apperrors "github.com/aliighahremanii/buildingblocks/application/errors"
)

// Register is a vertical-slice command. Each use case owns its own command and
// result types; nothing is shared through a generic envelope.
type Register struct {
	Name     string
	Quantity int
}

// Validate makes Register satisfy commands.Validatable, so the Validation
// middleware can run it before the handler.
func (c Register) Validate() error {
	var fields []apperrors.FieldError
	if c.Name == "" {
		fields = append(fields, apperrors.Field("name", "name is required"))
	}
	if c.Quantity <= 0 {
		fields = append(fields, apperrors.Field("quantity", "quantity must be greater than zero"))
	}
	if len(fields) == 0 {
		return nil
	}
	return apperrors.NewValidationError(fields...)
}

// RegisterResult is what the caller receives.
type RegisterResult struct {
	ID       string
	Quantity int
}

// registerHandler is the use case itself. In a real service it would load and
// save an event-sourced aggregate through an event-store repository; here it
// stays in memory so the example needs no infrastructure.
type registerHandler struct{}

func (registerHandler) Handle(_ context.Context, cmd Register) (RegisterResult, error) {
	if cmd.Name == "duplicate" {
		// A duplicate registration is an optimistic-concurrency conflict, not a
		// validation failure — the consumer maps this to HTTP 409.
		return RegisterResult{}, apperrors.NewConcurrencyError("widget", "widget-1", 3, 4)
	}
	return RegisterResult{ID: "widget-1", Quantity: cmd.Quantity}, nil
}

func main() {
	// Compose the pipeline explicitly. A real module composes this in its
	// composition root (module.go), never in an HTTP endpoint.
	var handler commands.Handler[Register, RegisterResult] = registerHandler{}
	handler = commands.Chain(handler, commands.Validation[Register, RegisterResult]())

	for _, cmd := range []Register{
		{Name: "widget", Quantity: 5},
		{Name: "", Quantity: 0},
		{Name: "duplicate", Quantity: 1},
	} {
		result, err := handler.Handle(context.Background(), cmd)
		fmt.Printf("command=%+v\n", cmd)
		switch {
		case err == nil:
			fmt.Printf("  ok: %+v\n", result)
		default:
			var validationErr apperrors.ValidationError
			var concurrencyErr apperrors.ConcurrencyError
			switch {
			case errors.As(err, &validationErr):
				fmt.Printf("  validation (-> HTTP 422): %v\n", err)
			case errors.As(err, &concurrencyErr):
				fmt.Printf("  concurrency conflict (-> HTTP 409): %v\n", err)
			default:
				fmt.Printf("  error: %v\n", err)
			}
		}
	}
}
