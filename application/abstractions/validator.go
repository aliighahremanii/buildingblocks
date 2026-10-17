package abstractions

import "context"

type Validator[T any] interface {
	Validate(ctx context.Context, value T) error
}
