package httpd

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/messagedb/messagedb/cluster"
	"github.com/messagedb/messagedb/db"
	"github.com/messagedb/messagedb/meta"
	"github.com/messagedb/messagedb/services/httpd/controllers"
	"github.com/messagedb/messagedb/services/httpd/middleware"

	"github.com/gin-gonic/contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/itsjamie/gin-cors"
	"golang.org/x/net/netutil"
)

// Service manages the listener and handler for an HTTP endpoint.
type Service struct {
	listener net.Listener
	addr     string
	err      chan error
	maxConn  int
	router   *gin.Engine
	Version  string

	PingController          *controllers.PingController
	SessionController       *controllers.SessionController
	UsersController         *controllers.UsersController
	DevicesController       *controllers.DevicesController
	OrganizationsController *controllers.OrganizationsController
	ConversationsController *controllers.ConversationsController
	MessagesController      *controllers.MessagesController

	Logger *log.Logger
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	s := &Service{
		addr:    c.BindAddress,
		err:     make(chan error),
		router:  NewRouter(),
		maxConn: c.MaxConnections,
		Logger:  log.New(os.Stderr, "[httpd] ", log.LstdFlags),
	}

	s.PingController = s.setupPingController(c)
	s.SessionController = s.setupSessionController(c)
	s.UsersController = s.setupUsersController(c)
	s.DevicesController = s.setupDevicesController(c)
	s.OrganizationsController = s.setupOrganizationsController(c)
	s.ConversationsController = s.setupConversationsController(c)
	s.MessagesController = s.setupMessagesController(c)

	return s
}

func (s *Service) SetMetaStore(metaStore *meta.Store) {
	s.SessionController.MetaStore = metaStore
	s.UsersController.MetaStore = metaStore
	s.DevicesController.MetaStore = metaStore
	s.OrganizationsController.MetaStore = metaStore
	s.ConversationsController.MetaStore = metaStore
	s.MessagesController.MetaStore = metaStore
}

func (s *Service) SetDataStore(dataStore *db.Store) {
	s.MessagesController.DataStore = dataStore
}

func (s *Service) SetQueryExecutor(executor *db.QueryExecutor) {
	s.MessagesController.QueryExecutor = executor
}

func (s *Service) SetMessagesWriter(writer *cluster.MessagesWriter) {
	s.MessagesController.MessagesWriter = writer
}

func (s *Service) setupPingController(config Config) *controllers.PingController {
	c := controllers.NewPingController(s.router, config.LogEnabled, config.WriteTracing)
	c.Logger = s.Logger
	return c
}

func (s *Service) setupSessionController(config Config) *controllers.SessionController {
	c := controllers.NewSessionController(s.router, config.LogEnabled, config.WriteTracing)
	c.Logger = s.Logger
	return c
}

func (s *Service) setupUsersController(config Config) *controllers.UsersController {
	c := controllers.NewUsersController(s.router, config.LogEnabled, config.WriteTracing)
	c.Logger = s.Logger
	return c
}

func (s *Service) setupDevicesController(config Config) *controllers.DevicesController {
	c := controllers.NewDevicesController(s.router, config.LogEnabled, config.WriteTracing)
	c.Logger = s.Logger
	return c
}

func (s *Service) setupOrganizationsController(config Config) *controllers.OrganizationsController {
	c := controllers.NewOrganizationsController(s.router, config.LogEnabled, config.WriteTracing)
	c.Logger = s.Logger
	return c
}

func (s *Service) setupConversationsController(config Config) *controllers.ConversationsController {
	c := controllers.NewConversationsController(s.router, config.LogEnabled, config.WriteTracing)
	c.Logger = s.Logger
	return c
}

func (s *Service) setupMessagesController(config Config) *controllers.MessagesController {
	c := controllers.NewMessagesController(s.router, config.LogEnabled, config.WriteTracing)
	c.Logger = s.Logger
	return c
}

// Open starts the service
func (s *Service) Open() error {
	// Open listener.
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.listener = netutil.LimitListener(listener, s.maxConn)

	s.Logger.Println("listening on HTTP:", listener.Addr().String())

	// Begin listening for requests in a separate goroutine.
	go s.serve()
	return nil
}

// Close closes the underlying listener.
func (s *Service) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.Logger = l
}

// Err returns a channel for fatal errors that occur on the listener.
func (s *Service) Err() <-chan error { return s.err }

// Addr returns the listener's address. Returns nil if listener is closed.
func (s *Service) Addr() net.Addr {
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// serve serves the handler from the listener.
func (s *Service) serve() {
	// The listener was closed so exit
	// See https://github.com/golang/go/issues/4373
	err := http.Serve(s.listener, s.router)
	if err != nil && !strings.Contains(err.Error(), "closed") {
		s.err <- fmt.Errorf("listener failed: addr=%s, err=%s", s.Addr(), err)
	}
}

func NewRouter() *gin.Engine {

	gin.SetMode(gin.ReleaseMode)

	router := gin.Default()
	router.RedirectTrailingSlash = true
	router.RedirectFixedPath = true

	router.Use(gzip.Gzip(gzip.DefaultCompression))
	router.Use(middleware.ContentTypeCheckerMiddleware())
	router.Use(middleware.RequestIdMiddleware())
	router.Use(middleware.RevisionMiddleware())

	router.Use(cors.Middleware(cors.Config{
		Origins:         "*",
		Methods:         "GET, PUT, POST, DELETE, PATCH, OPTIONS",
		RequestHeaders:  "Origin, Authorization, Content-Type",
		ExposedHeaders:  "",
		MaxAge:          1728000,
		Credentials:     true,
		ValidateHeaders: false,
	}))

	return router
}
