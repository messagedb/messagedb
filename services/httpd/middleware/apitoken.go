package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

const ApiTokenHeaderKey = "X-MessageDB-Api-Token"

func ApiTokenMiddleware() gin.HandlerFunc {
	return func(ctx *gin.Context) {

		token := ctx.Request.Header.Get(ApiTokenHeaderKey)
		if len(token) == 0 {
			ctx.AbortWithStatus(http.StatusForbidden)
			return
		}

		valid, err := isValidApiToken(token)
		if err != nil {
			ctx.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		if !valid {
			ctx.AbortWithStatus(http.StatusForbidden)
			return
		}

		ctx.Next()
	}
}

func isValidApiToken(token string) (bool, error) {
	//TODO: check if Api Token is a valid one here...
	return true, nil
}
