package middleware

import (
	"net/http"

	"github.com/google/uuid"

	"github.com/aliighahremanii/buildingblocks/application/correlation"
	"github.com/aliighahremanii/buildingblocks/presentation/auth"
)

// WithActorMetadata is the host-level net/http middleware that bridges the
// authenticated principal (set by bearer-auth middleware) into the correlation
// context so that events written while serving the request carry the actor's
// UserID in their EventMetadata envelope.
//
// Runtime order (outer → inner) MUST be:
//
//	WithCorrelationID  →  WithBearerAuthentication  →  WithActorMetadata  →  mux
//
// WithCorrelationID runs first so the correlation.Context (carrying
// CorrelationID/CausationID/CommandID) already exists; WithBearerAuthentication
// runs second so the auth principal is available on the request context;
// WithActorMetadata runs third to merge the principal's UserID into the
// correlation.Context before handlers (and the event-store Save path) read it.
//
// When no correlation context is present (defensive — should not happen with the
// canonical host wiring), a fresh one is minted so that an actor-attributed
// request still gets correlation IDs. When the request is unauthenticated the
// UserID is left empty, preserving the existing behaviour for anonymous/worker
// paths; events written by such requests carry an empty UserID, which the
// audit projection treats as a system/anonymous actor.
func WithActorMetadata(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, ok := correlation.FromContext(r.Context())
		if !ok {
			commandID := uuid.New().String()
			c = correlation.Context{
				CorrelationID: uuid.New().String(),
				CausationID:   commandID,
				CommandID:     commandID,
			}
		}
		if userID, found := auth.UserID(r.Context()); found {
			c.UserID = userID
		}
		r = r.WithContext(correlation.WithContext(r.Context(), c))
		next.ServeHTTP(w, r)
	})
}
