package controllers

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

// PingController handles RESTful API requests for an Ping resources
type PingController struct {
	Engine *gin.Engine

	Logger         *log.Logger
	loggingEnabled bool // Log every HTTP access
	WriteTrace     bool // Detail logging of controller handler
}

// NewPingController returns an instance of the PingController
func NewPingController(engine *gin.Engine, loggingEnabled, writeTrace bool) *PingController {
	c := &PingController{
		Engine:         engine,
		loggingEnabled: loggingEnabled,
		WriteTrace:     writeTrace,
	}

	c.Engine.GET("/ping", c.PingHandler)

	return c
}

// PingHandler responds to the ping method
//
// GET /ping
//
func (p *PingController) PingHandler(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{"result": "pong"})
}
