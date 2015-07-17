package controllers

import (
	"net/http"
	"strings"

	"github.com/messagedb/messagedb/meta/services"

	"github.com/gin-gonic/gin"
)

// AuthenticatedFilter is a middleware that ensure there is an authentication token in the HTTP headers, only allowing
// the request to proceeed if the token is present and valid
func AuthenticatedFilter() gin.HandlerFunc {
	return func(ctx *gin.Context) {

		var token string
		auth := ctx.Request.Header.Get("Authorization")
		if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			token = auth[7:]
		} else {
			ctx.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		user, err := services.Auth.ValidateAccessToken(token)
		if err != nil || user == nil {
			ctx.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		ctx.Set("currentUser", user)
		ctx.Next()
	}
}

// UsernameFilter is a middleware that retrieves the username from the URL and attempts to load a user model
func UsernameFilter() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		paramUname := ctx.Param("username")

		if len(paramUname) == 0 {
			ctx.AbortWithStatus(http.StatusBadRequest)
		}

		// var user *schema.User
		// var err error
		// if utils.IsObjectId(paramUname) {
		// 	user, err = models.User.FindById(paramUname)
		// } else {
		// 	user, err = models.User.FindByUsername(paramUname)
		// }
		//
		// if err != nil {
		// 	if err == utils.ErrInvalidObjectId || err == ErrNotFound {
		// 		ctx.AbortWithStatus(http.StatusNotFound)
		// 	} else {
		// 		ctx.String(http.StatusBadRequest, err.Error())
		// 	}
		// 	return
		// }
		//
		// if user == nil {
		// 	ctx.AbortWithStatus(http.StatusNotFound)
		// 	return
		// }
		//
		// //TODO: verify if current user has ability to access this team
		//
		// // add to the context so we can reuse in the handlers
		// ctx.Set("user", user)
		ctx.Next()
	}
}

// func TeamFilter() gin.HandlerFunc {
// 	return func(ctx *gin.Context) {
// 		paramTeam := ctx.Param("team_id")

// 		if len(paramTeam) == 0 {
// 			ctx.AbortWithStatus(http.StatusBadRequest)
// 		}

// 		team, err := models.Team.FindById(paramTeam)
// 		if err != nil {
// 			if err == utils.ErrInvalidObjectId || err == storage.ErrNotFound {
// 				ctx.AbortWithStatus(http.StatusNotFound)
// 			} else {
// 				ctx.AbortWithStatus(http.StatusInternalServerError)
// 			}
// 			return
// 		}

// 		if team == nil {
// 			ctx.AbortWithStatus(http.StatusNotFound)
// 			return
// 		}

// 		organization, err := models.Organization.FindById(team.OrganizationId)
// 		if err != nil || organization == nil {
// 			ctx.AbortWithStatus(http.StatusInternalServerError)
// 			return
// 		}

// 		//TODO: verify if current user has ability to access this team

// 		// add to the context so we can reuse in the handlers
// 		ctx.Set("team", team)
// 		ctx.Set("organization", organization)
// 		ctx.Next()
// 	}
// }

// OrganizationFilter is middleware that attempts to load the organization based on the URL parameters
func OrganizationFilter() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		paramOrg := ctx.Param("org")

		if len(paramOrg) == 0 {
			ctx.AbortWithStatus(http.StatusBadRequest)
		}

		// organization, err := models.Organization.FindByID(paramOrg)
		// if err != nil {
		// 	if err == utils.ErrInvalidObjectId || err == storage.ErrNotFound {
		// 		ctx.AbortWithStatus(http.StatusNotFound)
		// 	} else {
		// 		ctx.AbortWithStatus(http.StatusInternalServerError)
		// 	}
		// 	return
		// }
		//
		// if organization == nil {
		// 	ctx.AbortWithStatus(http.StatusNotFound)
		// 	return
		// }
		//
		// //TODO: verify if current user has ability to access this organization
		//
		// // add to the context so we can reuse in the handlers
		// ctx.Set("organization", organization)
		ctx.Next()
	}
}

// func CheckOrgOwnershipFilter() gin.HandlerFunc {
// 	return func(ctx *gin.Context) {

// 		org, ok := ctx.MustGet("organization").(*schema.Organization)
// 		if !ok {
// 			ctx.AbortWithStatus(http.StatusInternalServerError)
// 		}

// 		user, ok := ctx.MustGet("user").(*schema.User)
// 		if !ok {
// 			ctx.AbortWithStatus(http.StatusInternalServerError)
// 		}

// 		isOwner, err := models.Member.IsOrganizationOwner(org, user)
// 		if err != nil {
// 			ctx.AbortWithStatus(http.StatusInternalServerError)
// 		}

// 		if !isOwner {
// 			helpers.JSONForbidden(ctx, "Action not authorized for authenticated user")
// 			return
// 		}

// 		ctx.Next()
// 	}
// }

// func CheckOrgMembershipFilter() gin.HandlerFunc {
// 	return func(ctx *gin.Context) {

// 		org, ok := ctx.MustGet("organization").(*schema.Organization)
// 		if !ok {
// 			ctx.AbortWithStatus(http.StatusInternalServerError)
// 		}

// 		user, ok := ctx.MustGet("user").(*schema.User)
// 		if !ok {
// 			ctx.AbortWithStatus(http.StatusInternalServerError)
// 		}

// 		isMember, err := models.Member.IsOrganizationMember(org, user)
// 		if err != nil {
// 			ctx.AbortWithStatus(http.StatusInternalServerError)
// 		}

// 		if !isMember {
// 			helpers.JSONForbidden(ctx, "Action not authorized for authenticated user")
// 			return
// 		}

// 		ctx.Next()
// 	}
// }

// func CheckTeamMembershipFilter() gin.HandlerFunc {
// 	return func(ctx *gin.Context) {

// 		team, ok := ctx.MustGet("team").(*schema.Team)
// 		if !ok {
// 			ctx.AbortWithStatus(http.StatusInternalServerError)
// 		}

// 		user, ok := ctx.MustGet("user").(*schema.User)
// 		if !ok {
// 			ctx.AbortWithStatus(http.StatusInternalServerError)
// 		}

// 		isTeamMember, err := models.Member.IsTeamMember(team, user)
// 		if err != nil {
// 			ctx.AbortWithStatus(http.StatusInternalServerError)
// 		}

// 		if !isTeamMember {
// 			helpers.JSONForbidden(ctx, "Action not authorized for authenticated user")
// 			return
// 		}

// 		ctx.Next()
// 	}
// }

// MessageFilter is a middleware that attemps to load a Message based on the provided URL parameters
func MessageFilter() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		paramMsgID := ctx.Param("message_id")

		if len(paramMsgID) == 0 {
			ctx.AbortWithStatus(http.StatusBadRequest)
		}

		// message, err := models.Message.FindById(paramMsgID)
		// if err != nil {
		// 	if err == utils.ErrInvalidObjectId || err == storage.ErrNotFound {
		// 		ctx.AbortWithStatus(http.StatusNotFound)
		// 	} else {
		// 		ctx.AbortWithStatus(http.StatusInternalServerError)
		// 	}
		// 	return
		// }
		//
		// if message == nil {
		// 	ctx.AbortWithStatus(http.StatusNotFound)
		// 	return
		// }
		//
		// //TODO: verify if current user has ability to access this conversation
		//
		// // add to the context so we can reuse in the handlers
		// ctx.Set("message", message)
		ctx.Next()
	}
}

// DeviceFilter is a middleware that attemps to load a Device based on the provided URL parameters
func DeviceFilter() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		paramID := ctx.Param("id")

		if len(paramID) == 0 {
			ctx.AbortWithStatus(http.StatusBadRequest)
		}

		// device, err := models.Device.FindByID(paramID)
		// if err != nil {
		// 	if err == utils.ErrInvalidObjectId || err == storage.ErrNotFound {
		// 		ctx.AbortWithStatus(http.StatusNotFound)
		// 	} else {
		// 		ctx.AbortWithStatus(http.StatusInternalServerError)
		// 	}
		// 	return
		// }
		//
		// if device == nil {
		// 	ctx.AbortWithStatus(http.StatusNotFound)
		// 	return
		// }
		//
		// //TODO: verify if current user has ability to access this team
		//
		// // add to the context so we can reuse in the handlers
		// ctx.Set("device", device)
		ctx.Next()
	}
}

// ConversationFilter is a middleware that attemps to load a Conversation based on the provided URL parameters
func ConversationFilter() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		paramID := ctx.Param("conversation_id")

		if len(paramID) == 0 {
			ctx.AbortWithStatus(http.StatusBadRequest)
		}

		// conversation, err := models.Conversation.FindByID(paramID)
		// if err != nil {
		// 	if err == utils.ErrInvalidObjectId || err == storage.ErrNotFound {
		// 		ctx.AbortWithStatus(http.StatusNotFound)
		// 	} else {
		// 		ctx.AbortWithStatus(http.StatusInternalServerError)
		// 	}
		// 	return
		// }
		//
		// if conversation == nil {
		// 	ctx.AbortWithStatus(http.StatusNotFound)
		// 	return
		// }
		//
		// //TODO: verify if current user has ability to access this conversation
		//
		// // add to the context so we can reuse in the handlers
		// ctx.Set("conversation", conversation)
		ctx.Next()
	}
}

// IntegrationFilter is a middleware that attemps to load a Integration based on the provided URL parameters
func IntegrationFilter() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		paramName := ctx.Param("name")

		if len(paramName) == 0 {
			ctx.AbortWithStatus(http.StatusBadRequest)
		}

		// integration, err := models.Integration.FindByName(paramName)
		// if err != nil {
		// 	if err == storage.ErrNotFound {
		// 		ctx.AbortWithStatus(http.StatusNotFound)
		// 	} else {
		// 		ctx.AbortWithStatus(http.StatusInternalServerError)
		// 	}
		// 	return
		// }
		//
		// if integration == nil {
		// 	ctx.AbortWithStatus(http.StatusNotFound)
		// 	return
		// }
		//
		// //TODO: verify if current user has ability to access this conversation
		//
		// // add to the context so we can reuse in the handlers
		// ctx.Set("integration", integration)
		ctx.Next()
	}
}
