package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/messagedb/messagedb/db"
	"github.com/messagedb/messagedb/meta"

	"gopkg.in/fatih/pool.v2"
)

const (
	MAX_MAP_RESPONSE_SIZE = 1024 * 1024 * 1024
)

// ShardMapper is responsible for providing mappers for requested shards. It is
// responsible for creating those mappers from the local store, or reaching
// out to another node on the cluster.
type ShardMapper struct {
	ForceRemoteMapping bool // All shards treated as remote. Useful for testing.

	MetaStore interface {
		NodeID() uint64
		Node(id uint64) (ni *meta.NodeInfo, err error)
	}

	DataStore interface {
		CreateMapper(shardID uint64, query string, chunkSize int) (db.Mapper, error)
	}

	timeout time.Duration
	pool    *clientPool
}

// NewShardMapper returns a mapper of local and remote shards.
func NewShardMapper(timeout time.Duration) *ShardMapper {
	return &ShardMapper{
		pool:    newClientPool(),
		timeout: timeout,
	}
}

// CreateMapper returns a Mapper for the given shard ID.
func (s *ShardMapper) CreateMapper(sh meta.ShardInfo, stmt string, chunkSize int) (db.Mapper, error) {
	var err error
	var m db.Mapper
	if sh.OwnedBy(s.MetaStore.NodeID()) && !s.ForceRemoteMapping {
		m, err = s.DataStore.CreateMapper(sh.ID, stmt, chunkSize)
		if err != nil {
			return nil, err
		}
	} else {
		// Pick a node in a pseudo-random manner.
		conn, err := s.dial(sh.OwnerIDs[rand.Intn(len(sh.OwnerIDs))])
		if err != nil {
			return nil, err
		}
		conn.SetDeadline(time.Now().Add(s.timeout))

		rm := NewRemoteMapper(conn.(*pool.PoolConn), sh.ID, stmt, chunkSize)
		m = rm
	}

	return m, nil
}

func (s *ShardMapper) dial(nodeID uint64) (net.Conn, error) {
	// If we don't have a connection pool for that addr yet, create one
	_, ok := s.pool.getPool(nodeID)
	if !ok {
		factory := &connFactory{nodeID: nodeID, clientPool: s.pool, timeout: s.timeout}
		factory.metaStore = s.MetaStore

		p, err := pool.NewChannelPool(1, 3, factory.dial)
		if err != nil {
			return nil, err
		}
		s.pool.setPool(nodeID, p)
	}
	return s.pool.conn(nodeID)
}

type remoteShardConn interface {
	io.ReadWriter
	Close() error
	MarkUnusable()
}

// RemoteMapper implements the db.Mapper interface. It connects to a remote node,
// sends a query, and interprets the stream of data that comes back.
type RemoteMapper struct {
	nodeID    uint64
	shardID   uint64
	stmt      string
	chunkSize int

	tagsets []string

	resp    *http.Response
	decoder *json.Decoder

	conn remoteShardConn

	bufferedResponse *MapShardResponse
}

// NewRemoteMapper returns a new remote mapper using the given connection.
func NewRemoteMapper(c remoteShardConn, shardID uint64, stmt string, chunkSize int) *RemoteMapper {
	return &RemoteMapper{
		conn:      c,
		shardID:   shardID,
		stmt:      stmt,
		chunkSize: chunkSize,
	}
}

// Open connects to the remote node and starts receiving data.
func (r *RemoteMapper) Open() (err error) {
	defer func() {
		if err != nil {
			r.conn.Close()
		}
	}()
	// Build Map request.
	var request MapShardRequest
	request.SetShardID(r.shardID)
	request.SetQuery(r.stmt)
	request.SetChunkSize(int32(r.chunkSize))

	// Marshal into protocol buffers.
	buf, err := request.MarshalBinary()
	if err != nil {
		return err
	}

	// Write request.
	if err := WriteTLV(r.conn, mapShardRequestMessage, buf); err != nil {
		r.conn.MarkUnusable()
		return err
	}

	// Read the response.
	_, buf, err = ReadTLV(r.conn)
	if err != nil {
		r.conn.MarkUnusable()
		return err
	}

	// Unmarshal response.
	r.bufferedResponse = &MapShardResponse{}
	if err := r.bufferedResponse.UnmarshalBinary(buf); err != nil {
		return err
	}

	if r.bufferedResponse.Code() != 0 {
		return fmt.Errorf("error code %d: %s", r.bufferedResponse.Code(), r.bufferedResponse.Message())
	}

	return nil
}

// NextChunk returns the next chunk read from the remote node to the client.
func (r *RemoteMapper) NextChunk() (chunk interface{}, err error) {
	output := &db.MapperOutput{}
	var response *MapShardResponse

	if r.bufferedResponse != nil {
		response = r.bufferedResponse
		r.bufferedResponse = nil
	} else {
		response = &MapShardResponse{}

		// Read the response.
		_, buf, err := ReadTLV(r.conn)
		if err != nil {
			r.conn.MarkUnusable()
			return nil, err
		}

		// Unmarshal response.
		if err := response.UnmarshalBinary(buf); err != nil {
			return nil, err
		}

		if response.Code() != 0 {
			return nil, fmt.Errorf("error code %d: %s", response.Code(), response.Message())
		}
	}

	if response.Data() == nil {
		return nil, nil
	}
	err = json.Unmarshal(response.Data(), output)
	return output, err
}

// Close the Mapper
func (r *RemoteMapper) Close() {
	r.conn.Close()
}
