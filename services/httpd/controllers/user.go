package controllers

import (
	"log"
	"net/http"

	"github.com/messagedb/messagedb/meta"
	"github.com/messagedb/messagedb/meta/bindings"
	"github.com/messagedb/messagedb/meta/services"
	"github.com/messagedb/messagedb/services/httpd/helpers"
	"github.com/messagedb/messagedb/services/httpd/presenters"

	"github.com/gin-gonic/gin"
)

// UsersController handles the request for the API User resources
type UsersController struct {
	Engine *gin.Engine

	MetaStore interface {
		Database(name string) (*meta.DatabaseInfo, error)
		Authenticate(username, password string) (ui *meta.UserInfo, err error)
		Users() ([]meta.UserInfo, error)
	}

	Logger         *log.Logger
	loggingEnabled bool // Log every HTTP access
	WriteTrace     bool // Detail logging of controller handler
}

func NewUsersController(engine *gin.Engine, loggingEnabled, writeTrace bool) *UsersController {
	c := &UsersController{
		Engine:         engine,
		loggingEnabled: loggingEnabled,
		WriteTrace:     writeTrace,
	}
	c.registerRoutes()
	return c
}

func (c *UsersController) registerRoutes() error {

	router := c.Engine
	{
		router.POST("/users", c.RegisterNewUser)

		authRouter := router.Group("", AuthenticatedFilter())
		{
			meRouter := authRouter.Group("/me")
			{
				meRouter.GET("", c.GetMe)
				meRouter.PATCH("", c.UpdateMe)

				meRouter.POST("/change/password", c.ChangePassword)

				meRouter.GET("/emails", c.ListMyEmails)
				meRouter.POST("/emails", c.AddEmail)
				meRouter.DELETE("/emails", c.DeleteEmail)
				meRouter.GET("/orgs", c.ListMyOrganizations)
				meRouter.GET("/memberships/orgs", c.ListMyOrganizationMemberships)
				meRouter.GET("/memberships/orgs/:org_id", c.GetMyOrganizationMembership)
				meRouter.PATCH("/memberships/orgs/:org_id", c.EditMyOrganizationMembership)
				meRouter.GET("/conversations", c.ListMyConversations)
			}

			authRouter.GET("/users", c.ListAllUsers)

			unameRouter := authRouter.Group("/", UsernameFilter())
			{
				unameRouter.GET("/users/:username", c.GetUser)
				unameRouter.GET("/users/:username/orgs", c.ListUserOrganizations)
				unameRouter.GET("/users/:username/conversations", c.ListUserConversations)
			}
		}
	}

	return nil
}

// GetMe gets the authenticated user
//
// GET /user
//
func (c *UsersController) GetMe(ctx *gin.Context) {
	user := getCurrentUser(ctx)
	helpers.JSONResponseObject(ctx, presenters.UserPresenter(user))
}

// UpdateMe updates the authenticated user
//
// PATCH /user
//
func (c *UsersController) UpdateMe(ctx *gin.Context) {
	user := getCurrentUser(ctx)
	//TODO: fix me

	helpers.JSONResponseObject(ctx, presenters.UserPresenter(user))
}

// ChangeUsername changes the username of authenticated user
// POST /change/username
func (c *UsersController) ChangeUsername(ctx *gin.Context) {

	var json bindings.ChangeUsername
	if err := ctx.Bind(&json); err != nil {
		helpers.JSONResponseValidationFailed(ctx, err)
		return
	}

	user := getCurrentUser(ctx)
	accountService, err := services.NewAccountService(user)
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create AccountService for user: %v", user)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	ok, err := accountService.ChangeUsername(json)
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	if !ok {
		helpers.JSONErrorf(ctx, http.StatusBadRequest, "Failed to change username for user: %s", user.Username)
		return
	}

	helpers.JSONResponseOK(ctx)
}

// ChangePassword updates the password for authenticated user
//
// POST /change/password
//
// Params:
//
// type ChangePassword struct {
// 	OldPassword string `json:"old_password" binding:"required"`
// 	NewPassword string `json:"new_password" binding:"required"`
// }
//
func (c *UsersController) ChangePassword(ctx *gin.Context) {

	var json bindings.ChangePassword
	if err := ctx.Bind(&json); err != nil {
		helpers.JSONResponseValidationFailed(ctx, err)
		return
	}

	user := getCurrentUser(ctx)
	accountService, err := services.NewAccountService(user)
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create AccountService for user: %v", user)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	ok, err := accountService.ChangePassword(json)
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	if !ok {
		helpers.JSONErrorf(ctx, http.StatusBadRequest, "Failed to change password for user: %s", user.Username)
		return
	}

	helpers.JSONResponseOK(ctx)

}

// ListMyEmails lists email addresses for current user
//
// GET /user/emails
//
func (c *UsersController) ListMyEmails(ctx *gin.Context) {
	user := getCurrentUser(ctx)
	helpers.JSONResponseOK(ctx, user.ListOfEmails())
}

// AddEmail adds email address for current user
//
// POST /user/emails
//
func (c *UsersController) AddEmail(ctx *gin.Context) {
	var json bindings.UpdateEmail
	err := ctx.Bind(&json)
	if err != nil {
		helpers.JSONResponseValidationFailed(ctx, err)
	}

	user := getCurrentUser(ctx)
	accountService, err := services.NewAccountService(user)
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create AccountService for user: %v", user)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	err = accountService.AddEmailAddress(json)
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	helpers.JSONResponseOK(ctx, user.ListOfEmails())
}

// DeleteEmail deletes email address for current user
//
// DELETE /user/emails
//
func (c *UsersController) DeleteEmail(ctx *gin.Context) {
	var json bindings.UpdateEmail
	err := ctx.Bind(&json)
	if err != nil {
		helpers.JSONResponseValidationFailed(ctx, err)
	}

	user := getCurrentUser(ctx)
	accountService, err := services.NewAccountService(user)
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create AccountService for user: %v", user)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	err = accountService.RemoveEmailAddress(json)
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	helpers.JSONResponseOK(ctx, user.ListOfEmails())
}

// ListMyOrganizations list the active organizations for the authenticated user
//
// GET /user/orgs
//
func (c *UsersController) ListMyOrganizations(ctx *gin.Context) {
	user := getCurrentUser(ctx)

	accountService, err := services.NewAccountService(user)
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create AccountService for user: %v", user)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	orgs, err := accountService.ListMyOrganizations()
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	helpers.JSONResponseObject(ctx, presenters.OrganizationCollectionPresenter(orgs))
}

// ListMyOrganizationMemberships list your organization memberships for the authenticated users
//
// GET /user/memberships/orgs
//
func (c *UsersController) ListMyOrganizationMemberships(ctx *gin.Context) {

	user := getCurrentUser(ctx)

	accountService, err := services.NewAccountService(user)
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create AccountService for user: %v", user)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	members, err := accountService.ListMyMemberships()
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	helpers.JSONResponseObject(ctx, presenters.MemberCollectionPresenter(members))
}

// GetMyOrganizationMembership get the organization membership for the authenticated user
//
// GET /user/memberships/orgs/:org
//
func (c *UsersController) GetMyOrganizationMembership(ctx *gin.Context) {

	user := getCurrentUser(ctx)

	accountService, err := services.NewAccountService(user)
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create AccountService for user: %v", user)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	org := getOrganizationFromContext(ctx)

	member, err := accountService.GetMyMembership(org.ID)
	if err != nil {
		if err == ErrNotFound {
			helpers.JSONErrorf(ctx, http.StatusNotFound, "No membership found for this organization")
			return
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	helpers.JSONResponseObject(ctx, presenters.MemberPresenter(member))
}

// EditMyOrganizationMembership edit your organization membership for the authenticated user
//
// PATCH /user/memberships/orgs/:org
//
func (c *UsersController) EditMyOrganizationMembership(ctx *gin.Context) {
	var json bindings.EditMyMembership
	err := ctx.Bind(&json)
	if err != nil {
		helpers.JSONResponseValidationFailed(ctx, err)
	}

	user := getCurrentUser(ctx)

	accountService, err := services.NewAccountService(user)
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create AccountService for user: %v", user)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	org := getOrganizationFromContext(ctx)

	member, err := accountService.EditMyMembership(org.ID, json)
	if err != nil {
		if err == ErrNotFound {
			helpers.JSONErrorf(ctx, http.StatusNotFound, "No membership found for this organization")
			return
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	helpers.JSONResponseObject(ctx, presenters.MemberPresenter(member))
}

// ListMyConversations lists the conversations that the authenticated user participates in
//
// GET /user/conversations
//
func (c *UsersController) ListMyConversations(ctx *gin.Context) {
	// TODO: implement this
	helpers.JSONResponseNotImplemented(ctx)
}

///// USERS Resource /////

// ListAllUsers returns all the users
//
// GET /users
//
func (c *UsersController) ListAllUsers(ctx *gin.Context) {
	users, err := services.ListAllUsers()
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}
	helpers.JSONResponseCollection(ctx, presenters.UserCollectionPresenter(users))
}

// RegisterNewUser creates a new user account
//
// POST /users
//
func (c *UsersController) RegisterNewUser(ctx *gin.Context) {
	var json bindings.RegisterNewUser
	err := ctx.Bind(&json)
	if err != nil {
		helpers.JSONResponseValidationFailed(ctx, err)
		return
	}

	user, err := services.RegisterNewUser(json)
	if err != nil {
		if err == services.ErrNamespaceAlreadyExists {
			helpers.JSONErrorf(ctx, http.StatusBadRequest, "Username already exists")
		} else {
			helpers.JSONResponseInternalServerError(ctx, err)
		}
		return
	}

	helpers.JSONResponseObject(ctx, presenters.UserPresenter(user))
}

// GetUser returns a user
//
// GET /users/:username
//
func (c *UsersController) GetUser(ctx *gin.Context) {
	user := getUserFromContext(ctx)
	helpers.JSONResponseObject(ctx, presenters.UserPresenter(user))
}

// ListUserOrganizations lists all organizations for a specific user
//
// GET /users/:username/orgs
//
func (c *UsersController) ListUserOrganizations(ctx *gin.Context) {
	user := getCurrentUser(ctx)

	userService, err := services.NewUserService(user)
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create UserService for user: %v", user)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	orgs, err := userService.ListOrganizations()
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	helpers.JSONResponseObject(ctx, presenters.OrganizationCollectionPresenter(orgs))
}

// ListUserConversations lists all conversations for a specific user
//
// GET /users/:username/conversations
//
func (c *UsersController) ListUserConversations(ctx *gin.Context) {

	user := getCurrentUser(ctx)

	userService, err := services.NewUserService(user)
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create UserService for user: %v", user)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	members, err := userService.ListMemberships()
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	helpers.JSONResponseObject(ctx, presenters.MemberCollectionPresenter(members))
}
