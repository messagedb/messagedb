package controllers

// import (
// 	"net/http"

// 	"github.com/messagedb/messagedb/services/httpd/presenters"
// 	"github.com/messagedb/messagedb/meta/bindings"
// 	"github.com/messagedb/messagedb/meta/models"

// 	"github.com/gin-gonic/gin"
// )

// type TeamController struct {
// 	Engine *gin.Engine
// }

// func newTeamController(engine *gin.Engine) Controller {
// 	return &TeamController{Engine: engine}
// }

// func (c *TeamController) registerRoutes() error {

// 	authRouter := c.Engine.Group("")
// 	authRouter.Use(filters.AuthenticatedFilter())
// 	authRouter.Use(filters.TeamFilter())
// 	{
// 		memberRouter := authRouter.Group("", filters.CheckOrgMembershipFilter())
// 		ownerRouter := authRouter.Group("", filters.CheckOrgOwnershipFilter())
// 		teamMemberRouter := memberRouter.Group("", filters.CheckTeamMembershipFilter())

// 		memberRouter.GET("/teams/:team_id", c.GetTeam)

// 		ownerRouter.PATCH("/teams/:team_id", c.EditTeam)
// 		ownerRouter.DELETE("/teams/:team_id", c.DeleteTeam)

// 		memberRouter.GET("/teams/:team_id/members", c.ListMembers)

// 		ownerRouter.GET("/teams/:team_id/membership/:username", c.GetTeamMembership)
// 		ownerRouter.PUT("/teams/:team_id/membership/:username", c.AddTeamMembership)
// 		ownerRouter.DELETE("/teams/:team_id/membership/:username", c.RemoveTeamMembership)

// 		memberRouter.GET("/teams/:team_id/conversations", c.ListTeamConversations)

// 		memberRouter.GET("/teams/:team_id/conversations/:conversation_id", c.CheckIfTeamManagesConversation)
// 		teamMemberRouter.PUT("/teams/:team_id/conversations/:conversation_id", c.AddTeamConversation)
// 		teamMemberRouter.DELETE("/teams/:team_id/conversations/:conversation_id", c.RemoveTeamConversation)
// 	}

// 	return nil
// }

// // GET /teams/:id
// // Get a Team
// func (c *TeamController) GetTeam(ctx *gin.Context) {
// 	team := GetTeamFromContext(ctx)
// 	helpers.JSONResponseObject(ctx, presenters.TeamPresenter(team))
// }

// // PATCH /teams/:id
// // Edit a team
// func (c *TeamController) EditTeam(ctx *gin.Context) {
// 	org := getOrganizationFromContext(ctx)
// 	team := GetTeamFromContext(ctx)

// 	var json bindings.CreateUpdateTeam
// 	err := ctx.Bind(&json)
// 	if err != nil {
// 		helpers.JSONResponseValidationFailed(ctx, err)
// 		return
// 	}

// 	team, err = services.Team.UpdateTeam(org, team, json)
// 	if err != nil {
// 		if err == services.ErrTeamNameAlreadyExists {
// 			helpers.JSONErrorf(ctx, http.StatusBadRequest, "Team name already exists in this organization")
// 		} else if err == services.ErrForbiddenNotOrganizationOwner {
// 			helpers.JSONErrorf(ctx, http.StatusForbidden, "User cannot manage this organization")
// 		} else {
// 			helpers.JSONResponseInternalServerError(ctx, err)
// 		}
// 		return
// 	}

// 	helpers.JSONResponseObject(ctx, presenters.TeamPresenter(team))
// }

// // DELETE /teams/:id
// // DELETE a team
// func (c *TeamController) DeleteTeam(ctx *gin.Context) {
// 	org := getOrganizationFromContext(ctx)
// 	team := GetTeamFromContext(ctx)

// 	err := services.Team.DeleteTeam(org, team)
// 	if err != nil {
// 		if err == services.ErrTeamHasMembers {
// 			helpers.JSONErrorf(ctx, http.StatusForbidden, "Unable to delete Team because it has members or pending invitations")
// 		} else if err == services.ErrForbiddenToDeleteOwnersTeam {
// 			helpers.JSONErrorf(ctx, http.StatusForbidden, "Organization Owner's team cannot be deleted")
// 		} else {
// 			helpers.JSONResponseInternalServerError(ctx, err)
// 		}
// 		return
// 	}
// 	helpers.JSONResponseOK(ctx)
// }

// // GET /teams/:id/members
// // List team members
// func (c *TeamController) ListMembers(ctx *gin.Context) {
// 	team := GetTeamFromContext(ctx)

// 	members, err := models.Member.FindByTeam(team)
// 	if err != nil {
// 		helpers.JSONResponseInternalServerError(ctx, err)
// 		return
// 	}

// 	helpers.JSONResponseCollection(ctx, presenters.MemberCollectionPresenter(members))
// }

// // GET /teams/:id/membership/:username
// // Get team membership
// func (c *TeamController) GetTeamMembership(ctx *gin.Context) {
// 	_ = GetTeamFromContext(ctx)
// 	helpers.JSONResponseNotImplemented(ctx)
// }

// // PUT /teams/:id/membership/:username
// // Add team membership
// func (c *TeamController) AddTeamMembership(ctx *gin.Context) {
// 	_ = GetTeamFromContext(ctx)
// 	helpers.JSONResponseNotImplemented(ctx)
// }

// // DELETE /teams/:id/membership/:username
// // Remove team membership
// func (c *TeamController) RemoveTeamMembership(ctx *gin.Context) {
// 	_ = GetTeamFromContext(ctx)
// 	helpers.JSONResponseNotImplemented(ctx)
// }

// // GET /teams/:id/conversations
// // List team conversations
// func (c *TeamController) ListTeamConversations(ctx *gin.Context) {
// 	_ = GetTeamFromContext(ctx)
// 	helpers.JSONResponseNotImplemented(ctx)
// }

// // GET /teams/:id/conversations/:owner/:conversation
// // Check if a team manages a conversation
// func (c *TeamController) CheckIfTeamManagesConversation(ctx *gin.Context) {
// 	_ = GetTeamFromContext(ctx)
// 	helpers.JSONResponseNotImplemented(ctx)
// }

// // PUT /teams/:id/conversations/:owner/:conversation
// // Add team conversations
// func (c *TeamController) AddTeamConversation(ctx *gin.Context) {
// 	_ = GetTeamFromContext(ctx)
// 	helpers.JSONResponseNotImplemented(ctx)
// }

// // DELETE /teams/:id/conversations/:owner/:conversation
// // Remove team conversations
// func (c *TeamController) RemoveTeamConversation(ctx *gin.Context) {
// 	_ = GetTeamFromContext(ctx)
// 	helpers.JSONResponseNotImplemented(ctx)
// }
