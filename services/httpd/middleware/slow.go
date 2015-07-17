package middleware

import (
	"time"

	"github.com/gin-gonic/gin"
)

type SlowHandlerFunc func(ctx *gin.Context, duration time.Duration)

func SlowMiddleware(maxTime time.Duration, callback SlowHandlerFunc) gin.HandlerFunc {
	return func(ctx *gin.Context) {

		defer func(startTime time.Time) {
			elapsed := time.Since(startTime)
			if elapsed > maxTime {
				callback(ctx, elapsed)
			}
		}(time.Now())

		ctx.Next()
	}
}
