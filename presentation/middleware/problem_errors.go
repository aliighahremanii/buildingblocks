package middleware

import (
	"errors"
	"log/slog"

	"github.com/aliighahremanii/buildingblocks/application/correlation"
	apperrors "github.com/aliighahremanii/buildingblocks/application/errors"
	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

type ProblemFieldFactory[ProblemField any] func(code string, message string) ProblemField

type ProblemResponseFactory[ProblemResponse any, ProblemField any] func(code string, message string, fields []ProblemField) ProblemResponse

func WriteProblemError[ProblemResponse any, ProblemField any](
	ctx web.Context,
	err error,
	newProblemResponse ProblemResponseFactory[ProblemResponse, ProblemField],
	newProblemField ProblemFieldFactory[ProblemField],
) error {
	var ve apperrors.ValidationError
	if errors.As(err, &ve) {
		fields := make([]ProblemField, len(ve.Errors))
		for i, field := range ve.Errors {
			fields[i] = newProblemField(field.Code, field.Message)
		}

		return ctx.JSON(web.StatusUnprocessableEntity, newProblemResponse(
			apperrors.ValidationErrorCode,
			apperrors.ValidationErrorMessage,
			fields,
		))
	}

	var ue apperrors.UnauthorizedError
	if errors.As(err, &ue) {
		return ctx.JSON(web.StatusUnauthorized, newProblemResponse(ue.Code, ue.Message, nil))
	}

	var fe apperrors.ForbiddenError
	if errors.As(err, &fe) {
		return ctx.JSON(web.StatusForbidden, newProblemResponse(apperrors.ForbiddenCode, fe.Message, nil))
	}

	var nfe apperrors.NotFoundError
	if errors.As(err, &nfe) {
		return ctx.JSON(web.StatusNotFound, newProblemResponse(
			apperrors.NotFoundCode,
			apperrors.NotFoundMessage(nfe.Resource),
			nil,
		))
	}

	var ce apperrors.ConflictError
	if errors.As(err, &ce) {
		return ctx.JSON(web.StatusConflict, newProblemResponse(ce.Code, ce.Message, nil))
	}

	var cce apperrors.ConcurrencyError
	if errors.As(err, &cce) {
		return ctx.JSON(web.StatusConflict, newProblemResponse(cce.Code, cce.Message, nil))
	}

	var tmre apperrors.TooManyRequestsError
	if errors.As(err, &tmre) {
		return ctx.JSON(web.StatusTooManyRequests, newProblemResponse(tmre.Code, tmre.Message, nil))
	}

	var sue apperrors.ServiceUnavailableError
	if errors.As(err, &sue) {
		return ctx.JSON(web.StatusServiceUnavailable, newProblemResponse(sue.Code, sue.Message, nil))
	}

	// JSON binding failures are client errors, not server errors. The shared
	// presentation error wrapper lets all HTTP adapters map them consistently.
	var bindErr *web.BindJSONError
	if errors.As(err, &bindErr) {
		return ctx.JSON(web.StatusBadRequest, newProblemResponse(
			apperrors.BadRequestCode,
			apperrors.InvalidRequestBodyMessage,
			nil,
		))
	}

	// Fallback: an unclassified error is masked as a generic 500 with NO
	// client-visible detail (no leak of internals or sensitive bodies) —
	// which makes it undiagnosable unless the cause is logged HERE, at the
	// masking boundary, with the underlying error and whatever
	// correlation/request metadata the context carries (D127: the production
	// tombstoned-stream incident was invisible in logs for exactly this
	// reason). slog.Default matches the building-blocks logging convention.
	logAttrs := []any{
		"err", err,
		"method", ctx.Method(),
		"path", ctx.Path(),
	}
	if c, ok := correlation.FromContext(ctx.Context()); ok {
		logAttrs = append(logAttrs, "correlation_id", c.CorrelationID, "command_id", c.CommandID)
	}
	slog.Error("unclassified error masked as 500 InternalError", logAttrs...)

	return ctx.JSON(web.StatusInternalServerError, newProblemResponse(
		apperrors.InternalErrorCode,
		apperrors.InternalErrorMessage,
		nil,
	))
}

// DebugError logs the actual error type and message for 500-class errors
// that do not match any known application error. This is a development
// aid; production deployments should rely on structured logging from the
// handler instead.
func DebugError(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}
