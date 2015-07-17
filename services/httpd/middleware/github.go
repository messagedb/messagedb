package middleware

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/messagedb/messagedb/util"

	"github.com/gin-gonic/gin"
)

//GithubMiddleware returns a Handler that authenticates via GitHub's Authorization for
//Webhooks scheme (https://developer.github.com/webhooks/securing/#validating-payloads-from-github)
//Writes a http.StatusUnauthorized if authentication fails
//
func GithubMiddleware(secret string) gin.HandlerFunc {

	// Set out header value for each response
	return func(ctx *gin.Context) {

		requestSignature := ctx.Request.Header.Get("X-Hub-Signature")

		body, err := ioutil.ReadAll(ctx.Request.Body)
		if err != nil {
			ctx.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		ctx.Request.Body = ioutil.NopCloser(bytes.NewReader(body))

		mac := hmac.New(sha1.New, []byte(secret))
		mac.Reset()
		mac.Write(body)
		calculatedSignature := fmt.Sprintf("sha1=%x", mac.Sum(nil))

		if !util.SecureCompare(requestSignature, calculatedSignature) {
			ctx.AbortWithStatus(http.StatusUnauthorized)
		} else {
			ctx.Next()
		}

	}
}
