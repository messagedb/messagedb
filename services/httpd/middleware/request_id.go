package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/satori/go.uuid"
)

const (
	RequestIDHeaderKey = "X-Request-Id"
	ServerHeaderValue  = "MessageDB"
)

func RequestIdMiddleware() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		ctx.Writer.Header().Set("X-Request-Id", uuid.NewV4().String())
		ctx.Writer.Header().Set("Server", ServerHeaderValue)
		ctx.Next()
	}
}
