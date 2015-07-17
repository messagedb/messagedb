package middleware

import (
	"mime"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

const ContentTypeHeaderKey = "Content-Type"

func ContentTypeCheckerMiddleware() gin.HandlerFunc {
	return func(ctx *gin.Context) {

		header := ctx.Request.Header.Get(ContentTypeHeaderKey)
		mediatype, params, _ := mime.ParseMediaType(header)
		charset, ok := params["charset"]
		if !ok {
			charset = "UTF-8"
		}

		if ctx.Request.ContentLength > 0 &&
			!(mediatype == "application/json" && strings.ToUpper(charset) == "UTF-8") {

			ctx.String(http.StatusUnsupportedMediaType, "Bad Content-Type or charset, expected 'application/json'\n")
			ctx.Abort()
			return
		}

		ctx.Next()
	}
}
