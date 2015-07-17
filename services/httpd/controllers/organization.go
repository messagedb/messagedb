package controllers

import (
	"log"
	"net/http"

	"github.com/messagedb/messagedb/meta"
	"github.com/messagedb/messagedb/meta/bindings"
	"github.com/messagedb/messagedb/meta/schema"
	"github.com/messagedb/messagedb/meta/services"
	"github.com/messagedb/messagedb/services/httpd/helpers"
	"github.com/messagedb/messagedb/services/httpd/presenters"

	"github.com/gin-gonic/gin"
)

// OrganizationsController handles RESTful API requests for an Organization resources
type OrganizationsController struct {
	Engine *gin.Engine

	MetaStore interface {
		Database(name string) (*meta.DatabaseInfo, error)
		Authenticate(username, password string) (ui *meta.UserInfo, err error)
		Users() ([]meta.UserInfo, error)
		// Organizations() ([]meta.OrganizationInfo, error)
	}

	Logger         *log.Logger
	loggingEnabled bool // Log every HTTP access
	WriteTrace     bool // Detail logging of controller handler
}

func NewOrganizationsController(engine *gin.Engine, loggingEnabled, writeTrace bool) *OrganizationsController {
	c := &OrganizationsController{
		Engine:         engine,
		loggingEnabled: loggingEnabled,
		WriteTrace:     writeTrace,
	}
	c.registerRoutes()
	return c
}

func (c *OrganizationsController) registerRoutes() error {

	authRouter := c.Engine.Group("", AuthenticatedFilter())
	{
		authRouter.GET("/orgs", c.ListOrganizations)
		authRouter.POST("/orgs", c.CreateOrganization)

		orgRouter := authRouter.Group("/")
		orgRouter.Use(OrganizationFilter())
		{
			orgRouter.GET("/orgs/:org", c.GetOrganization)
			orgRouter.PATCH("/orgs/:org", c.EditOrganization)

			orgRouter.GET("/orgs/:org/members", c.ListMembers)

			orgRouter.GET("/orgs/:org/members/:username", UsernameFilter(), c.CheckMembership)

			orgRouter.GET("/orgs/:org/memberships/:username", UsernameFilter(), c.GetMembership)
			orgRouter.PUT("/orgs/:org/memberships/:username", UsernameFilter(), c.AddOrUpdateMembership)
			orgRouter.DELETE("/orgs/:org/memberships/:username", UsernameFilter(), c.RemoveMembership)

			orgRouter.GET("/orgs/:org/conversations", c.ListConversations)
			orgRouter.POST("/orgs/:org/conversations", c.CreateConversation)

			orgRouter.GET("/orgs/:org/public_conversations", c.ListPublicConversations)

			orgRouter.GET("/orgs/:org/public_members", c.ListPublicMembers)
			orgRouter.GET("/orgs/:org/public_members/:username", UsernameFilter(), c.CheckPublicMembership)
			orgRouter.PUT("/orgs/:org/public_members/:username", UsernameFilter(), c.PublicizeMembership)
			orgRouter.DELETE("/orgs/:org/public_members/:username", UsernameFilter(), c.ConcealMembership)
		}
	}

	return nil
}

// ListOrganizations returns all available organizations
//
// GET /orgs
//
func (c *OrganizationsController) ListOrganizations(ctx *gin.Context) {
	orgs := []*schema.Organization{}

	//TODO: add pagination support via Skip and Limit query methods

	// err := models.Organization.Find(nil).All(&orgs)
	// if err != nil {
	// 	helpers.JSONResponseInternalServerError(ctx, err)
	// 	return
	// }

	helpers.JSONResponseCollection(ctx, presenters.OrganizationCollectionPresenter(orgs))
}

// CreateOrganization creates a new Organization and makes the current authenticated user the owner
//
// POST /orgs
//
func (c *OrganizationsController) CreateOrganization(ctx *gin.Context) {
	var json bindings.CreateOrganization
	err := ctx.Bind(&json)
	if err != nil {
		helpers.JSONResponseValidationFailed(ctx, err)
		return
	}

	// creates the organization and set the current user as part of Owners Team
	org, err := services.CreateOrganization(json, getCurrentUser(ctx))
	if err != nil {
		if err == services.ErrNamespaceAlreadyExists {
			helpers.JSONErrorf(ctx, http.StatusBadRequest, "Organization name already exists")
		} else {
			helpers.JSONResponseInternalServerError(ctx, err)
		}
		return
	}

	helpers.JSONResponseObject(ctx, presenters.OrganizationPresenter(org))
}

// GetOrganization returns an organization record
//
// GET /orgs/:org
//
func (c *OrganizationsController) GetOrganization(ctx *gin.Context) {
	org := getOrganizationFromContext(ctx)
	helpers.JSONResponseObject(ctx, presenters.OrganizationPresenter(org))
}

// EditOrganization modifes an organization record
//
// PATCH /orgs/:org
//
func (c *OrganizationsController) EditOrganization(ctx *gin.Context) {

	var json bindings.UpdateOrganization
	err := ctx.Bind(&json)
	if err != nil {
		helpers.JSONResponseValidationFailed(ctx, err)
		return
	}

	org := getOrganizationFromContext(ctx)
	orgService, err := services.NewOrganizationService(org, getCurrentUser(ctx))
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create OrganizationService for org: %v", org)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	org, err = orgService.UpdateOrganization(json)
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	helpers.JSONResponseObject(ctx, presenters.OrganizationPresenter(org))
}

// ListMembers return the list of all members that are part of the organization. This will return all members even the ones
// that are still pending acceptance of their invitation
//
// GET /orgs/:org/members
//
func (c *OrganizationsController) ListMembers(ctx *gin.Context) {

	org := getOrganizationFromContext(ctx)
	orgService, err := services.NewOrganizationService(org, getCurrentUser(ctx))
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create OrganizationService for org: %v", org)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	members, err := orgService.GetMembers()
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	helpers.JSONResponseCollection(ctx, presenters.MemberCollectionPresenter(members))
}

// CheckMembership checks if a user is a member of the organization
//
// GET /orgs/:org/members/:username
//
func (c *OrganizationsController) CheckMembership(ctx *gin.Context) {

	org := getOrganizationFromContext(ctx)
	orgService, err := services.NewOrganizationService(org, getCurrentUser(ctx))
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create OrganizationService for org: %v", org)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	// retrieves the user that was found based on username parameter
	user := getUserFromContext(ctx)

	check, err := orgService.CheckMembership(user)
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	if check {
		ctx.JSON(http.StatusNoContent, nil)
	} else {
		ctx.JSON(http.StatusNotFound, nil)
	}
}

// ListPublicMembers retusn the list of all public members. Each user can control the visibility of their membership. This will
// only includes the members that have chosen to do so.
//
// GET /orgs/:org/public_members
//
func (c *OrganizationsController) ListPublicMembers(ctx *gin.Context) {

	org := getOrganizationFromContext(ctx)
	orgService, err := services.NewOrganizationService(org, getCurrentUser(ctx))
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create OrganizationService for org: %v", org)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	members, err := orgService.GetPublicMembers()
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	helpers.JSONResponseCollection(ctx, presenters.MemberCollectionPresenter(members))
}

// CheckPublicMembership checks if a user is a public member of the organization
//
// GET /orgs/:org/public_members/:username
//
func (c *OrganizationsController) CheckPublicMembership(ctx *gin.Context) {

	org := getOrganizationFromContext(ctx)
	orgService, err := services.NewOrganizationService(org, getCurrentUser(ctx))
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create OrganizationService for org: %v", org)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	// retrieves the user that was found based on username parameter
	user := getUserFromContext(ctx)

	check, err := orgService.CheckPublicMembership(user)
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	if check {
		ctx.JSON(http.StatusNoContent, nil)
	} else {
		ctx.JSON(http.StatusNotFound, nil)
	}
}

// PublicizeMembership makes the authenticated user organization membership public
//
// PUT /orgs/:org/public_members/:username
//
func (c *OrganizationsController) PublicizeMembership(ctx *gin.Context) {

	org := getOrganizationFromContext(ctx)
	orgService, err := services.NewOrganizationService(org, getCurrentUser(ctx))
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create OrganizationService for org: %v", org)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	// retrieves the user that was found based on username parameter
	user := getUserFromContext(ctx)

	err = orgService.PublicizeMembership(user)
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	ctx.JSON(http.StatusNoContent, nil)
}

// ConcealMembership makes the authenticated user organization membership private. User's membership will not appear
// in the roster of members of the organization
//
// DELETE /orgs/:org/public_members/:username
//
func (c *OrganizationsController) ConcealMembership(ctx *gin.Context) {

	org := getOrganizationFromContext(ctx)
	orgService, err := services.NewOrganizationService(org, getCurrentUser(ctx))
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create OrganizationService for org: %v", org)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	// retrieves the user that was found based on username parameter
	user := getUserFromContext(ctx)

	err = orgService.ConcealMembership(user)
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	ctx.JSON(http.StatusNoContent, nil)

}

// GetMembership returns the user's membership to the organization. The authenticated user must be an organization owner
//
// GET /orgs/:org/memberships/:username
//
func (c *OrganizationsController) GetMembership(ctx *gin.Context) {

	org := getOrganizationFromContext(ctx)
	orgService, err := services.NewOrganizationService(org, getCurrentUser(ctx))
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create OrganizationService for org: %v", org)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	// retrieves the user that was found based on username parameter
	user := getUserFromContext(ctx)

	member, err := orgService.GetMembership(user)
	if err != nil {
		if err == services.ErrNotAnOrganizationOwner {
			helpers.JSONForbidden(ctx, err.Error())
		} else {
			helpers.JSONResponseInternalServerError(ctx, err)
		}
		return
	}

	helpers.JSONResponseObject(ctx, presenters.MemberPresenter(member))
}

// AddOrUpdateMembership adds the user as member to the organization. The authenticated user must be an organization owner.
// If the user is not yet a member of the organization, the membership will be pending until the user accepts the invitation. Otherwise, if
// the user is already a member of the organization, this method will update the role of the member within the organization.
//
// PUT /orgs/:org/memberships/:username
//
func (c *OrganizationsController) AddOrUpdateMembership(ctx *gin.Context) {

	var json bindings.AddUpdateMembership
	err := ctx.Bind(&json)
	if err != nil {
		helpers.JSONResponseValidationFailed(ctx, err)
		return
	}

	org := getOrganizationFromContext(ctx)
	orgService, err := services.NewOrganizationService(org, getCurrentUser(ctx))
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create OrganizationService for org: %v", org)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	// retrieves the user that was found based on username parameter
	user := getUserFromContext(ctx)

	member, err := orgService.AddOrUpdateMembership(user, json)
	if err != nil {
		if err == services.ErrNotAnOrganizationOwner {
			helpers.JSONForbidden(ctx, err.Error())
		} else {
			helpers.JSONResponseInternalServerError(ctx, err)
		}
		return
	}

	helpers.JSONResponseObject(ctx, presenters.MemberPresenter(member))
}

// RemoveMembership removes the user's membership from the organization. The authenticated user must be an organization owner.
//
// DELETE /orgs/:org/memberships/:username
//
func (c *OrganizationsController) RemoveMembership(ctx *gin.Context) {

	org := getOrganizationFromContext(ctx)
	orgService, err := services.NewOrganizationService(org, getCurrentUser(ctx))
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create OrganizationService for org: %v", org)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	// retrieves the user that was found based on username parameter
	user := getUserFromContext(ctx)

	err = orgService.RemoveMembership(user)
	if err != nil {
		if err == services.ErrNotAnOrganizationOwner {
			helpers.JSONForbidden(ctx, err.Error())
		} else {
			helpers.JSONResponseInternalServerError(ctx, err)
		}
		return
	}

	ctx.JSON(http.StatusNoContent, nil)
}

// ListConversations returns all conversations that are part of the Organization
//
// GET /orgs/:org/conversations
//
func (c *OrganizationsController) ListConversations(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// CreateConversation creates a new conversation in the Organization
//
// POST /orgs/:org/conversations
//
func (c *OrganizationsController) CreateConversation(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// ListPublicConversations returns all conversations that are marked as public in the Organization.
//
// GET /orgs/:org/public_conversations
//
func (c *OrganizationsController) ListPublicConversations(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}
