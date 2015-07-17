package controllers

import (
	"log"

	"github.com/messagedb/messagedb/meta"
	"github.com/messagedb/messagedb/services/httpd/helpers"

	"github.com/gin-gonic/gin"
)

// DeviceController handles RESTful API requests for an Device resources
type DevicesController struct {
	Engine *gin.Engine

	MetaStore interface {
		Database(name string) (*meta.DatabaseInfo, error)
		Authenticate(username, password string) (ui *meta.UserInfo, err error)
		Users() ([]meta.UserInfo, error)
		// Devices() ([]meta.DeviceInfo, error)
	}

	Logger        *log.Logger
	logginEnabled bool // Log every HTTP access
	WriteTrace    bool // Detail logging of controller handler
}

func NewDevicesController(engine *gin.Engine, logginEnabled, writeTrace bool) *DevicesController {
	c := &DevicesController{
		Engine:        engine,
		logginEnabled: logginEnabled,
		WriteTrace:    writeTrace,
	}

	c.registerRoutes()

	return c
}

func (c *DevicesController) registerRoutes() error {

	router := c.Engine
	router.GET("/devices", c.ListDevices)
	router.POST("/devices", c.AddDevice)

	devicesRouter := router.Group("/")
	devicesRouter.Use(DeviceFilter())
	{
		devicesRouter.GET("/devices/:id", c.GetDevice)
		devicesRouter.PATCH("/devices/:id", c.EditDevice)
		devicesRouter.DELETE("/devices/:id", c.DeleteDevice)
	}

	return nil
}

// ListDevices returns the list of client devices associated with the authenticated user
//
// GET /devices
//
func (c *DevicesController) ListDevices(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// AddDevice registers a new client device to the authenticated user
//
// POST /devices
//
func (c *DevicesController) AddDevice(ctx *gin.Context) {
	helpers.JSONResponseNotImplemented(ctx)
}

// GetDevice returns a client device
//
// GET /devices/:id
//
func (c *DevicesController) GetDevice(ctx *gin.Context) {
	_ = ctx.MustGet("device")
	helpers.JSONResponseNotImplemented(ctx)
}

// EditDevice modifies the client device
//
// PATCH /devices/:id
//
func (c *DevicesController) EditDevice(ctx *gin.Context) {
	_ = ctx.MustGet("device")
	helpers.JSONResponseNotImplemented(ctx)
}

// DeleteDevice removes a client device from the authenticated users' account
//
// DELETE /devices/:id
//
func (c *DevicesController) DeleteDevice(ctx *gin.Context) {
	_ = ctx.MustGet("device")
	helpers.JSONResponseNotImplemented(ctx)
}
