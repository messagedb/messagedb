package middleware

import (
	"crypto/sha256"
	"fmt"
	"net/http"

	"github.com/messagedb/messagedb/util"

	"github.com/gin-gonic/gin"
)

/*
TravisCI returns a Handler that authenticates via Travis's Authorization for
Webhooks scheme (http://docs.travis-ci.com/user/notifications/#Authorization-for-Webhooks)
Writes a http.StatusUnauthorized if authentication fails
*/
func TravisCIMiddleware(token string) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		providedAuth := ctx.Request.Header.Get("Authorization")

		travisRepoSlug := ctx.Request.Header.Get("Travis-Repo-Slug")
		calculatedAuth := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s%s", travisRepoSlug, token))))

		if !util.SecureCompare(providedAuth, calculatedAuth) {
			ctx.AbortWithStatus(http.StatusUnauthorized)
		}

		ctx.Next()
	}
}
