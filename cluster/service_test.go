package cluster_test

import (
	"fmt"
	"net"
	"time"

	"github.com/messagedb/messagedb/cluster"
	"github.com/messagedb/messagedb/db"
	"github.com/messagedb/messagedb/meta"
	"github.com/messagedb/messagedb/tcp"
)

type metaStore struct {
	host string
}

func (m *metaStore) Node(nodeID uint64) (*meta.NodeInfo, error) {
	return &meta.NodeInfo{
		ID:   nodeID,
		Host: m.host,
	}, nil
}

type testService struct {
	nodeID           uint64
	ln               net.Listener
	muxln            net.Listener
	writeShardFunc   func(shardID uint64, messages []db.Message) error
	createShardFunc  func(database, policy string, shardID uint64) error
	createMapperFunc func(shardID uint64, query string, chunkSize int) (db.Mapper, error)
}

func newTestWriteService(f func(shardID uint64, messages []db.Message) error) testService {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	mux := tcp.NewMux()
	muxln := mux.Listen(cluster.MuxHeader)
	go mux.Serve(ln)

	return testService{
		writeShardFunc: f,
		ln:             ln,
		muxln:          muxln,
	}
}

func (ts *testService) Close() {
	if ts.ln != nil {
		ts.ln.Close()
	}
}

type serviceResponses []serviceResponse
type serviceResponse struct {
	shardID  uint64
	ownerID  uint64
	messages []db.Message
}

func (t testService) WriteToShard(shardID uint64, messages []db.Message) error {
	return t.writeShardFunc(shardID, messages)
}

func (t testService) CreateShard(database, policy string, shardID uint64) error {
	return t.createShardFunc(database, policy, shardID)
}

func (t testService) CreateMapper(shardID uint64, query string, chunkSize int) (db.Mapper, error) {
	return t.createMapperFunc(shardID, query, chunkSize)
}

func writeShardSuccess(shardID uint64, messages []db.Message) error {
	responses <- &serviceResponse{
		shardID:  shardID,
		messages: messages,
	}
	return nil
}

func writeShardFail(shardID uint64, messages []db.Message) error {
	return fmt.Errorf("failed to write")
}

var responses = make(chan *serviceResponse, 1024)

func (testService) ResponseN(n int) ([]*serviceResponse, error) {
	var a []*serviceResponse
	for {
		select {
		case r := <-responses:
			a = append(a, r)
			if len(a) == n {
				return a, nil
			}
		case <-time.After(time.Second):
			return a, fmt.Errorf("unexpected response count: expected: %d, actual: %d", n, len(a))
		}
	}
}
