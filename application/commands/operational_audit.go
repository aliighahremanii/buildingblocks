package commands

import (
	"context"

	"github.com/aliighahremanii/buildingblocks/application/abstractions"
)

type OperationalAuditDescriptor[Command any] func(command Command) (event string, resource string, resourceID string, metadata map[string]string)

func RequireOperationalAudit[Command any, Result any](
	logger abstractions.OperationalAuditLogger,
	describe OperationalAuditDescriptor[Command],
) Middleware[Command, Result] {
	return func(next Handler[Command, Result]) Handler[Command, Result] {
		return HandlerFunc[Command, Result](func(ctx context.Context, command Command) (Result, error) {
			result, err := next.Handle(ctx, command)
			if err != nil {
				return result, err
			}

			event, resource, resourceID, metadata := describe(command)
			if err := logger.LogOperationalEvent(ctx, event, resource, resourceID, metadata); err != nil {
				var zero Result
				return zero, err
			}

			return result, nil
		})
	}
}
