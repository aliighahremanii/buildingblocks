package middleware

import (
	"log"
	"time"

	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

func Logging() web.Middleware {
	return func(next web.HandlerFunc) web.HandlerFunc {
		return func(ctx web.Context) error {
			startedAt := time.Now()

			err := next(ctx)

			log.Printf(
				"method=%s path=%s duration=%s",
				ctx.Method(),
				ctx.Path(),
				time.Since(startedAt),
			)

			return err
		}
	}
}
