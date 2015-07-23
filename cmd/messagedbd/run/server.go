package run

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"
	"runtime"
	"time"

	"github.com/messagedb/messagedb/cluster"
	"github.com/messagedb/messagedb/db"
	"github.com/messagedb/messagedb/meta"
	"github.com/messagedb/messagedb/services/admin"
	"github.com/messagedb/messagedb/services/hh"
	"github.com/messagedb/messagedb/services/httpd"
	"github.com/messagedb/messagedb/services/retention"
	"github.com/messagedb/messagedb/services/snapshotter"
	"github.com/messagedb/messagedb/tcp"
)

// Server represents a container for the metadata and storage data and services.
// It is build using a Config and it manages the startup and shutdown of all
// services in the proper order. It also handle the multiplexing of all errors received from all services.
type Server struct {
	version string // Build version

	err     chan error
	closing chan struct{}

	Hostname    string
	BindAddress string
	Listener    net.Listener

	MetaStore      *meta.Store
	DataStore      *db.Store
	QueryExecutor  *db.QueryExecutor
	MessagesWriter *cluster.MessagesWriter
	ShardWriter    *cluster.ShardWriter
	ShardMapper    *cluster.ShardMapper
	HintedHandoff  *hh.Service

	Services []Service

	ClusterService     *cluster.Service
	SnapshotterService *snapshotter.Service

	// Server reporting
	reportingDisabled bool

	CPUProfile string
	MemProfile string
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config, version string) (*Server, error) {

	s := &Server{
		version: version,
		err:     make(chan error),
		closing: make(chan struct{}),

		Hostname:    c.Meta.Hostname,
		BindAddress: c.Meta.BindAddress,

		MetaStore: meta.NewStore(c.Meta),
		DataStore: db.NewStore(c.Data.Dir),

		reportingDisabled: c.ReportingDisabled,
	}

	// Copy TSDB configuration.
	s.DataStore.MaxWALSize = c.Data.MaxWALSize
	s.DataStore.WALFlushInterval = time.Duration(c.Data.WALFlushInterval)
	s.DataStore.WALPartitionFlushDelay = time.Duration(c.Data.WALPartitionFlushDelay)

	// Set the shard mapper
	s.ShardMapper = cluster.NewShardMapper(time.Duration(c.Cluster.ShardMapperTimeout))
	s.ShardMapper.ForceRemoteMapping = c.Cluster.ForceRemoteShardMapping
	s.ShardMapper.MetaStore = s.MetaStore
	s.ShardMapper.DataStore = s.DataStore

	// Initialize query executor.
	s.QueryExecutor = db.NewQueryExecutor(s.DataStore)
	s.QueryExecutor.MetaStore = s.MetaStore
	s.QueryExecutor.MetaStatementExecutor = &meta.StatementExecutor{Store: s.MetaStore}

	// Set the shard writer
	s.ShardWriter = cluster.NewShardWriter(time.Duration(c.Cluster.ShardWriterTimeout))
	s.ShardWriter.MetaStore = s.MetaStore

	// Create the hinted handoff service
	s.HintedHandoff = hh.NewService(c.HintedHandoff, s.ShardWriter)

	// Initialize points writer.
	s.MessagesWriter = cluster.NewMessagesWriter()
	s.MessagesWriter.WriteTimeout = time.Duration(c.Cluster.WriteTimeout)
	s.MessagesWriter.MetaStore = s.MetaStore
	s.MessagesWriter.DataStore = s.DataStore
	s.MessagesWriter.ShardWriter = s.ShardWriter
	s.MessagesWriter.HintedHandoff = s.HintedHandoff

	// Append services.
	s.appendClusterService(c.Cluster)
	s.appendSnapshotterService()
	s.appendAdminService(c.Admin)
	s.appendHTTPDService(c.HTTPD)
	s.appendRetentionPolicyService(c.Retention)

	return s, nil
}

func (s *Server) appendClusterService(c cluster.Config) {
	srv := cluster.NewService(c)
	srv.MetaStore = s.MetaStore
	srv.DataStore = s.DataStore
	s.Services = append(s.Services, srv)
	s.ClusterService = srv
}

func (s *Server) appendSnapshotterService() {
	srv := snapshotter.NewService()
	srv.MetaStore = s.MetaStore
	srv.DataStore = s.DataStore
	s.Services = append(s.Services, srv)
	s.SnapshotterService = srv
}

func (s *Server) appendRetentionPolicyService(c retention.Config) {
	if !c.Enabled {
		return
	}
	srv := retention.NewService(c)
	srv.MetaStore = s.MetaStore
	srv.DataStore = s.DataStore
	s.Services = append(s.Services, srv)
}

func (s *Server) appendAdminService(c admin.Config) {
	if !c.Enabled {
		return
	}
	srv := admin.NewService(c)
	s.Services = append(s.Services, srv)
}

func (s *Server) appendHTTPDService(c httpd.Config) {
	if !c.Enabled {
		return
	}
	srv := httpd.NewService(c)

	srv.SetMetaStore(s.MetaStore)
	srv.SetDataStore(s.DataStore)
	srv.SetQueryExecutor(s.QueryExecutor)
	srv.SetMessagesWriter(s.MessagesWriter)
	srv.Version = s.version

	s.Services = append(s.Services, srv)
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

// Open opens the meta and data store and all services.
func (s *Server) Open() error {
	if err := func() error {
		// Start profiling, if set.
		startProfile(s.CPUProfile, s.MemProfile)

		// Resolve host to address.
		_, port, err := net.SplitHostPort(s.BindAddress)
		if err != nil {
			return fmt.Errorf("split bind address: %s", err)
		}
		hostport := net.JoinHostPort(s.Hostname, port)
		addr, err := net.ResolveTCPAddr("tcp", hostport)
		if err != nil {
			return fmt.Errorf("resolve tcp: addr=%s, err=%s", hostport, err)
		}
		s.MetaStore.Addr = addr

		// Open shared TCP connection.
		ln, err := net.Listen("tcp", s.BindAddress)
		if err != nil {
			return fmt.Errorf("listen: %s", err)
		}
		s.Listener = ln

		// Multiplex listener.
		mux := tcp.NewMux()
		s.MetaStore.RaftListener = mux.Listen(meta.MuxRaftHeader)
		s.MetaStore.ExecListener = mux.Listen(meta.MuxExecHeader)
		s.ClusterService.Listener = mux.Listen(cluster.MuxHeader)
		s.SnapshotterService.Listener = mux.Listen(snapshotter.MuxHeader)
		go mux.Serve(ln)

		// Open meta store.
		if err := s.MetaStore.Open(); err != nil {
			return fmt.Errorf("open meta store: %s", err)
		}
		go s.monitorErrorChan(s.MetaStore.Err())

		// Wait for the store to initialize.
		<-s.MetaStore.Ready()

		// Open DB store.
		if err := s.DataStore.Open(); err != nil {
			return fmt.Errorf("open data store: %s", err)
		}

		// Open the hinted handoff service
		if err := s.HintedHandoff.Open(); err != nil {
			return fmt.Errorf("open hinted handoff: %s", err)
		}

		for _, service := range s.Services {
			if err := service.Open(); err != nil {
				return fmt.Errorf("open service: %s", err)
			}
		}

		// Start the reporting service, if not disabled.
		if !s.reportingDisabled {
			go s.startServerReporting()
		}

		return nil

	}(); err != nil {
		s.Close()
		return err
	}

	return nil
}

// Close shuts down the meta and data stores and all services.
func (s *Server) Close() error {
	stopProfile()

	if s.Listener != nil {
		s.Listener.Close()
	}
	if s.MetaStore != nil {
		s.MetaStore.Close()
	}
	if s.DataStore != nil {
		s.DataStore.Close()
	}
	if s.HintedHandoff != nil {
		s.HintedHandoff.Close()
	}
	// closes all the services
	for _, service := range s.Services {
		service.Close()
	}
	close(s.closing)
	return nil
}

// startServerReporting starts periodic server reporting.
func (s *Server) startServerReporting() {
	for {
		if err := s.MetaStore.WaitForLeader(30 * time.Second); err != nil {
			log.Printf("no leader available for reporting: %s", err.Error())
			continue
		}
		s.reportServer()
		<-time.After(24 * time.Hour)
	}
}

// reportServer reports anonymous statistics about the system.
func (s *Server) reportServer() {
	dis, err := s.MetaStore.Databases()
	if err != nil {
		log.Printf("failed to retrieve databases for reporting: %s", err.Error())
		return
	}
	numDatabases := len(dis)

	numOrgs := 0
	numUsers := 0
	numConversations := 0

	// TODO: Implement reporter

	for _, di := range dis {
		d := s.DataStore.DatabaseIndex(di.Name)
		if d == nil {
			// No data in this store for this database.
			continue
		}

		c := d.ConversationsCount()
		numConversations += c
	}

	clusterID, err := s.MetaStore.ClusterID()
	if err != nil {
		log.Printf("failed to retrieve cluster ID for reporting: %s", err.Error())
		return
	}

	json := fmt.Sprintf(`[{
    "name":"reports",
    "columns":["os", "arch", "version", "server_id", "cluster_id", "num_databases", "num_organizations", "num_users", "num_conversations"],
    "points":[["%s", "%s", "%s", "%x", "%x", "%d", "%d", "%d", "%d"]]
  }]`, runtime.GOOS, runtime.GOARCH, s.version, s.MetaStore.NodeID(), clusterID, numDatabases, numOrgs, numUsers, numConversations)

	data := bytes.NewBufferString(json)

	log.Printf("Sending anonymous usage statistics to m.messagedb.com")

	client := http.Client{Timeout: time.Duration(5 * time.Second)}
	go client.Post("http://m.messagedb.com:8086/db/reporting/series?u=reporter&p=messagedb", "application/json", data)
}

// monitorErrorChan reads an error channel and resends it through the server.
func (s *Server) monitorErrorChan(ch <-chan error) {
	for {
		select {
		case err, ok := <-ch:
			if !ok {
				return
			}
			s.err <- err
		case <-s.closing:
			return
		}
	}
}

// Service represents a service attached to the server.
type Service interface {
	Open() error
	Close() error
}
