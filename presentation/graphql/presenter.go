package graphql

import (
	"context"
	"errors"
	"log/slog"

	gqlgraphql "github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// Stable GraphQL extension codes used by the safe error presenter.
const (
	// CodeBadUserInput marks client-caused errors: the message names only
	// the rejected input, never infrastructure topology.
	CodeBadUserInput = "BAD_USER_INPUT"

	// CodeInternalServerError masks every error that is not provably
	// client-caused or deliberately classified.
	CodeInternalServerError = "INTERNAL_SERVER_ERROR"

	// MaskedInternalErrorMessage is the only message a client sees for a
	// masked internal error. The real error is logged server-side.
	MaskedInternalErrorMessage = "internal server error"
)

// extensionCodeKey is the conventional GraphQL error extension key carrying
// a machine-readable classification.
const extensionCodeKey = "code"

// SafeErrorPresenter returns a gqlgen error presenter that makes the HTTP
// boundary safe for untrusted clients without hiding actionable feedback:
//
//  1. Client-caused resolver errors — errors for which isClientError returns
//     true — keep their message verbatim and gain the stable
//     BAD_USER_INPUT extension code. The classifier is host-supplied
//     (dependency direction: building blocks must not know host error
//     types); the message is safe because the host type guarantees it names
//     only the rejected input.
//
//  2. Errors that already carry an extension code pass through UNTOUCHED.
//     This preserves gqlgen's own protocol errors — GRAPHQL_PARSE_FAILED,
//     GRAPHQL_VALIDATION_FAILED, COMPLEXITY_LIMIT_EXCEEDED — whose detailed
//     messages are client feedback by design, and any error a developer
//     deliberately shaped with a code.
//
//  3. Everything else is treated as untrusted detail: the real error is
//     logged server-side via logger (slog.Default() when nil), and the
//     client receives only "internal server error" with the
//     INTERNAL_SERVER_ERROR code. Internal error messages routinely name
//     hosts, index uids, and transport failures — they must never leak.
//
// Path and locations on the presented error are preserved in all three
// cases so clients still know which field failed.
func SafeErrorPresenter(isClientError func(error) bool, logger *slog.Logger) gqlgraphql.ErrorPresenterFunc {
	if logger == nil {
		logger = slog.Default()
	}
	return func(ctx context.Context, err error) *gqlerror.Error {
		if err == nil {
			return nil
		}
		var gqlErr *gqlerror.Error
		asGqlErr := errors.As(err, &gqlErr)

		if isClientError != nil && isClientError(err) {
			if !asGqlErr {
				gqlErr = gqlerror.WrapPath(gqlgraphql.GetPath(ctx), err)
			}
			setErrorCode(gqlErr, CodeBadUserInput)
			return gqlErr
		}

		if asGqlErr && hasErrorCode(gqlErr) {
			return gqlErr
		}

		logger.Error("graphql: internal server error masked at boundary",
			"error", err.Error(),
			"path", gqlgraphql.GetPath(ctx))
		if !asGqlErr {
			gqlErr = gqlerror.WrapPath(gqlgraphql.GetPath(ctx), err)
		}
		gqlErr.Message = MaskedInternalErrorMessage
		setErrorCode(gqlErr, CodeInternalServerError)
		return gqlErr
	}
}

// hasErrorCode reports whether the error was deliberately classified with
// an extension code.
func hasErrorCode(e *gqlerror.Error) bool {
	_, ok := extensionCode(e)
	return ok
}

// extensionCode returns the string extension code, if present.
func extensionCode(e *gqlerror.Error) (string, bool) {
	if e == nil || e.Extensions == nil {
		return "", false
	}
	code, ok := e.Extensions[extensionCodeKey].(string)
	return code, ok
}

// setErrorCode stamps code onto the error's extensions (creating the map if
// needed), mirroring gqlgen's errcode.Set.
func setErrorCode(e *gqlerror.Error, code string) {
	if e == nil {
		return
	}
	if e.Extensions == nil {
		e.Extensions = map[string]any{}
	}
	e.Extensions[extensionCodeKey] = code
}
