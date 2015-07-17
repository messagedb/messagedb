package controllers

import (
	"log"

	"github.com/messagedb/messagedb/meta"
	"github.com/messagedb/messagedb/meta/bindings"
	"github.com/messagedb/messagedb/meta/services"
	"github.com/messagedb/messagedb/services/httpd/helpers"
	"github.com/messagedb/messagedb/services/httpd/presenters"

	"github.com/gin-gonic/gin"
)

// SessionController handles RESTful API requests for an Session resources
type SessionController struct {
	Engine *gin.Engine

	MetaStore interface {
		Database(name string) (*meta.DatabaseInfo, error)
		Authenticate(username, password string) (ui *meta.UserInfo, err error)
		Users() ([]meta.UserInfo, error)
	}

	Logger        *log.Logger
	logginEnabled bool // Log every HTTP access
	WriteTrace    bool // Detail logging of controller handler
}

func NewSessionController(engine *gin.Engine, logginEnabled, writeTrace bool) *SessionController {
	c := &SessionController{
		Engine:        engine,
		logginEnabled: logginEnabled,
		WriteTrace:    writeTrace,
	}
	c.registerRoutes()
	return c
}

func (c *SessionController) registerRoutes() error {

	router := c.Engine
	router.POST("/authorize", c.AuthorizeUser)
	router.POST("/token/refresh", c.RefreshToken)

	return nil
}

// AuthorizeUser performs the authentication for the API user
//
// GET /authorize
//
func (c *SessionController) AuthorizeUser(ctx *gin.Context) {
	var json bindings.AuthorizeUser
	err := ctx.Bind(&json)
	if err != nil {
		// Missing authentication credentials
		helpers.JSONResponseValidationFailed(ctx, err)
		return
	}

	user, err := services.Auth.AuthorizeUser(json)
	if err != nil {
		helpers.JSONForbidden(ctx, "Invalid authentication credentials")
		return
	}

	tokenFields, err := services.Auth.GenerateToken(user)
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	helpers.JSONResponseOK(ctx, gin.H{
		"user":   presenters.UserPresenter(user),
		"tokens": tokenFields,
	})

}

// RefreshToken generates a new set of authentication tokens for the user to consume the API
//
// GET /token/refresh
//
func (c *SessionController) RefreshToken(ctx *gin.Context) {
	var json bindings.RefreshToken
	err := ctx.Bind(&json)
	if err != nil {
		// Missing refresh token
		helpers.JSONResponseValidationFailed(ctx, err)
		return
	}

	//TODO: fix this
	// user, err := services.Auth.ValidateRefreshToken(json.Token)
	// if err != nil {
	// 	helpers.JSONForbidden(ctx, "Unable to validate refresh token")
	// 	return
	// }
	//
	// tokenFields, err := services.Auth.GenerateToken(user)
	// if err != nil {
	// 	helpers.JSONResponseInternalServerError(ctx, err)
	// 	return
	// }
	//
	// helpers.JSONResponseOK(ctx, gin.H{
	// 	"user":   presenters.UserPresenter(user),
	// 	"tokens": tokenFields,
	// })
}
