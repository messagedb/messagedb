package controllers

import (
	"errors"

	"github.com/messagedb/messagedb/meta/schema"

	"github.com/gin-gonic/gin"
)

var (
	ErrNotFound = errors.New("Not Found")
)

func getCurrentUser(ctx *gin.Context) *schema.User {
	user, ok := ctx.MustGet("currentUser").(*schema.User)
	if !ok {
		panic("CurrentUser has wrong type of object")
	}
	return user
}

func getUserFromContext(ctx *gin.Context) *schema.User {
	user, ok := ctx.MustGet("user").(*schema.User)
	if !ok {
		panic("User has wrong type of object")
	}
	return user
}

func getOrganizationFromContext(ctx *gin.Context) *schema.Organization {
	org, ok := ctx.MustGet("organization").(*schema.Organization)
	if !ok {
		panic("Organization has wrong type of object")
	}
	return org
}

func getConversationFromContext(ctx *gin.Context) *schema.Conversation {
	conv, ok := ctx.MustGet("conversation").(*schema.Conversation)
	if !ok {
		panic("Conversation has wrong type of object")
	}
	return conv
}
