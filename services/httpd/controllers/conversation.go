package controllers

import (
	"log"

	"github.com/messagedb/messagedb/meta"
	"github.com/messagedb/messagedb/meta/services"
	"github.com/messagedb/messagedb/services/httpd/helpers"
	"github.com/messagedb/messagedb/services/httpd/presenters"

	"github.com/gin-gonic/gin"
)

// ConversationsController handles RESTful API requests for Conversation resources
type ConversationsController struct {
	Engine *gin.Engine

	MetaStore interface {
		Database(name string) (*meta.DatabaseInfo, error)
		Authenticate(username, password string) (ui *meta.UserInfo, err error)
		Users() ([]meta.UserInfo, error)
		// Conversations() ([]meta.ConversationInfo, error)
	}

	Logger         *log.Logger
	loggingEnabled bool // Log every HTTP access
	WriteTrace     bool // Detail logging of controller handler
}

func NewConversationsController(engine *gin.Engine, loggingEnabled, writeTrace bool) *ConversationsController {
	c := &ConversationsController{
		Engine:         engine,
		loggingEnabled: loggingEnabled,
		WriteTrace:     writeTrace,
	}
	c.registerRoutes()
	return c
}

func (c *ConversationsController) registerRoutes() error {

	router := c.Engine
	{
		router.GET("/conversations", c.ListPublicConversations)

		convRouter := router.Group("/")
		convRouter.Use(ConversationFilter())

		convRouter.GET("/conversations/:conversation_id", c.GetConversation)
		convRouter.PATCH("/conversations/:conversation_id", c.EditConversation)
		convRouter.DELETE("/conversations/:conversation_id", c.DeleteConversation)

		convRouter.GET("/conversations/:conversation_id/attachments", c.ListAttachments)
		convRouter.POST("/conversations/:conversation_id/attachments", c.AddAttachment)

		convRouter.GET("/conversations/:conversation_id/links", c.ListLinks)
		convRouter.GET("/conversations/:conversation_id/snippets", c.ListSnippets)

		convRouter.GET("/conversations/:conversation_id/participants", c.ListParticipants)
		convRouter.GET("/conversations/:conversation_id/participants/:username", UsernameFilter(), c.CheckParticipant)
		convRouter.PUT("/conversations/:conversation_id/participants/:username", UsernameFilter(), c.AddParticipant)
		convRouter.DELETE("/conversations/:conversation_id/participants/:username", UsernameFilter(), c.RemoveParticipant)

		convRouter.POST("/conversations/:conversation_id/pulses", c.AddPulse)

		convRouter.GET("/conversations/:conversation_id/integrations", c.ListIntegrations)

		intRouter := convRouter.Group("/")
		intRouter.Use(IntegrationFilter())
		{
			intRouter.GET("/conversations/:conversation_id/integrations/:name", c.AddIntegration)
			intRouter.PATCH("/conversations/:conversation_id/integrations/:name", c.EditIntegration)
			intRouter.DELETE("/conversations/:conversation_id/integrations/:name", c.RemoveIntegration)
		}

	}

	return nil
}

// ListPublicConversations returns the list of all public conversations
//
// GET /conversations
//
func (c *ConversationsController) ListPublicConversations(ctx *gin.Context) {

	conversations, err := services.ListPublicConversations(nil)
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	helpers.JSONResponseObject(ctx, presenters.ConversationCollectionPresenter(conversations))
}

// GetConversation returns a specific conversation
//
// GET /conversations/:id
//
func (c *ConversationsController) GetConversation(ctx *gin.Context) {

	ctxConversation := getConversationFromContext(ctx)

	conversationService, err := services.NewConversationService(ctxConversation, getCurrentUser(ctx))
	if err != nil {
		if c.WriteTrace {
			c.Logger.Printf("Failed to create ConversationService for org: %v", ctxConversation)
		}
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	conversation, err := conversationService.GetConversation()
	if err != nil {
		helpers.JSONResponseInternalServerError(ctx, err)
		return
	}

	helpers.JSONResponseObject(ctx, presenters.ConversationPresenter(conversation))
}

// EditConversation updates a specific conversation
//
// PATCH /conversations/:id
//
func (c *ConversationsController) EditConversation(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// DeleteConversation method deletes permanently a specificconversation
//
// DELETE /conversations/:id
//
func (c *ConversationsController) DeleteConversation(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// ListAttachments lists all conversation attachments
//
// GET /conversations/:id/attachments
//
func (c *ConversationsController) ListAttachments(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// AddAttachment adds attachment to conversation
//
// POST /conversations/:id/attachments
//
func (c *ConversationsController) AddAttachment(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// ListLinks lists all conversation links
//
// GET /conversations/:id/links
//
func (c *ConversationsController) ListLinks(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// ListPosts lists all conversation posts
//
// GET /conversations/:id/posts
//
func (c *ConversationsController) ListPosts(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// ListSnippets lists all conversation snippets
//
// GET /conversations/:id/snippets
//
func (c *ConversationsController) ListSnippets(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// ListParticipants lists all conversation snippets
//
// GET /conversations/:id/participants
//
func (c *ConversationsController) ListParticipants(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// CheckParticipant checks if user is a participant of a conversation
//
// GET /conversations/:id/participants/:username
//
func (c *ConversationsController) CheckParticipant(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// AddParticipant adds user as a participant in this conversation
//
// PUT /conversations/:id/participants/:username
//
func (c *ConversationsController) AddParticipant(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// RemoveParticipant removes user as a participant from this conversation
//
// DELETE /conversations/:id/participants/:username
//
func (c *ConversationsController) RemoveParticipant(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// AddPulse pulses the current user in the conversation so we can track activity
//
// POST /conversations/:id/pulses
//
func (c *ConversationsController) AddPulse(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// ListIntegrations lists all conversation integrations
//
// GET /conversations/:id/integrations
//
func (c *ConversationsController) ListIntegrations(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// AddIntegration lists all conversation integrations
//
// PUT /conversations/:id/integrations/:integration_name
//
func (c *ConversationsController) AddIntegration(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// EditIntegration edits a conversation's integration
//
// PATCH /conversations/:id/integrations/:integration_name
//
func (c *ConversationsController) EditIntegration(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// RemoveIntegration removes a conversation's integration
//
// DELETE /conversations/:id/integrations/:integration_name
//
func (c *ConversationsController) RemoveIntegration(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}
