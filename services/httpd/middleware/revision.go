package middleware

import (
	"io/ioutil"
	"strings"

	"github.com/gin-gonic/gin"
)

func RevisionMiddleware() gin.HandlerFunc {
	// Revision file contents will be only loaded once per process
	data, err := ioutil.ReadFile("REVISION")

	// If we cant read file, just skip to the next request handler
	// This is pretty much a NOOP middleware :)
	if err != nil {
		return func(ctx *gin.Context) {
			ctx.Next()
		}
	}

	// Clean up the value since it could contain line breaks
	revision := strings.TrimSpace(string(data))

	// Set out header value for each response
	return func(ctx *gin.Context) {
		ctx.Writer.Header().Set("X-Revision", revision)
		ctx.Next()
	}
}
