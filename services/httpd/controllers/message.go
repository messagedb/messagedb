package controllers

import (
	"log"

	"github.com/messagedb/messagedb/cluster"
	"github.com/messagedb/messagedb/db"
	"github.com/messagedb/messagedb/sql"
	"github.com/messagedb/messagedb/meta"
	"github.com/messagedb/messagedb/services/httpd/helpers"

	"github.com/gin-gonic/gin"
)

// MessagesController handles RESTful API requests for an Message resources
type MessagesController struct {
	Engine *gin.Engine

	MetaStore interface {
		Database(name string) (*meta.DatabaseInfo, error)
		Authenticate(username, password string) (ui *meta.UserInfo, err error)
		Users() ([]meta.UserInfo, error)
	}

	QueryExecutor interface {
		ExecuteQuery(q *sql.Query, db string, chunkSize int) (<-chan *sql.Result, error)
	}

	DataStore interface {
		CreateMapper(shardID uint64, query string, chunkSize int) (db.Mapper, error)
	}

	MessagesWriter interface {
		WriteMessages(p *cluster.WriteMessagesRequest) error
	}

	Logger        *log.Logger
	logginEnabled bool // Log every HTTP access
	WriteTrace    bool // Detail logging of controller handler
}

func NewMessagesController(engine *gin.Engine, logginEnabled, writeTrace bool) *MessagesController {
	c := &MessagesController{
		Engine:        engine,
		logginEnabled: logginEnabled,
		WriteTrace:    writeTrace,
	}
	c.registerRoutes()
	return c
}

func (c *MessagesController) registerRoutes() error {

	router := c.Engine
	{
		convRouter := router.Group("/conversations/:conversation_id")
		convRouter.Use(ConversationFilter(), MessageFilter())
		{
			convRouter.GET("/messages/:message_id", c.GetMessage)
			convRouter.PATCH("/messages/:message_id", c.EditMessage)
			convRouter.DELETE("/messages/:message_id", c.DeleteMessage)
		}
	}

	return nil
}

// GetMessage returns a message in a Conversation
//
// GET /conversations/:conversation_id/messages/:message_id
//
func (c *MessagesController) GetMessage(ctx *gin.Context) {
	_ = ctx.MustGet("conversation")
	helpers.JSONResponseNotImplemented(ctx)
}

// EditMessage edits a message in a Conversation
//
// PATCH /conversations/:conversation_id/messages/:message_id
//
func (c *MessagesController) EditMessage(ctx *gin.Context) {
	_ = ctx.MustGet("conversation")
	helpers.JSONResponseNotImplemented(ctx)
}

// DeleteMessage deletes a message from a Conversation
//
// DELETE /conversations/:conversation_id/messages/:message_id
//
func (c *MessagesController) DeleteMessage(ctx *gin.Context) {
	_ = ctx.MustGet("conversation")
	helpers.JSONResponseNotImplemented(ctx)
}
