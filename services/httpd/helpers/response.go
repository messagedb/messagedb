package helpers

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

const (
	LocationHeaderKey = "Location"
)

type ResponseError struct {
	Code    int    `json:"code,omitempty"`
	Message string `json:"error,omitempty"`
}

func NewResponseError(code int, message string) *ResponseError {
	return &ResponseError{Code: code, Message: message}
}

func (r *ResponseError) Error() error {
	return fmt.Errorf("Response Error with code %v: %v", r.Code, r.Message)
}

func JSONResponse(ctx *gin.Context, statusCode int, obj ...interface{}) {
	if len(obj) == 1 {
		if gin.Mode() == "debug" {
			ctx.IndentedJSON(statusCode, obj[0])
		} else {
			ctx.JSON(statusCode, obj[0])
		}
	} else {
		if gin.Mode() == "debug" {
			ctx.IndentedJSON(statusCode, obj)
		} else {
			ctx.JSON(statusCode, obj)
		}
	}
}

func JSONResponseObject(ctx *gin.Context, obj interface{}) {
	JSONResponse(ctx, http.StatusOK, obj)
}

func JSONResponseCollection(ctx *gin.Context, obj interface{}) {
	JSONResponse(ctx, http.StatusOK, map[string]interface{}{"results": obj})
}

func JSONResponseOK(ctx *gin.Context, obj ...interface{}) {
	JSONResponse(ctx, http.StatusOK, obj...)
}

func JSONResponseInternalServerError(ctx *gin.Context, obj ...interface{}) {
	JSONResponse(ctx, http.StatusInternalServerError, obj...)
}

func JSONResponseNotImplemented(ctx *gin.Context) {
	JSONResponse(ctx, http.StatusNotImplemented, nil)
}

func JSONResponseBadRequest(ctx *gin.Context, obj ...interface{}) {
	JSONResponse(ctx, http.StatusBadRequest, obj...)
}

func JSONForbidden(ctx *gin.Context, format string, a ...interface{}) {
	JSONErrorf(ctx, http.StatusForbidden, format, a...)
}

func JSONForbiddenCode(ctx *gin.Context, code int, format string, a ...interface{}) {
	JSONErrorCodef(ctx, http.StatusForbidden, code, format, a...)
}

func JSONResponseValidationFailed(ctx *gin.Context, err error) {
	JSONResponse(ctx, http.StatusBadRequest, err.Error())
}

func JSONError(ctx *gin.Context, statusCode int, err error) {
	JSONResponse(ctx, statusCode, &ResponseError{Message: err.Error()})
}

func JSONErrorCode(ctx *gin.Context, statusCode int, code int, err error) {
	JSONResponse(ctx, statusCode, &ResponseError{Code: code, Message: err.Error()})
}

func JSONErrorf(ctx *gin.Context, statusCode int, format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	JSONResponse(ctx, statusCode, &ResponseError{Message: msg})
}

func JSONErrorCodef(ctx *gin.Context, statusCode int, code int, format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	JSONResponse(ctx, statusCode, &ResponseError{Code: code, Message: msg})
}
