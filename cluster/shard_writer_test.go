package cluster_test

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/messagedb/messagedb/cluster"
	"github.com/messagedb/messagedb/db"
)

// Ensure the shard writer can successful write a single request.
func TestShardWriter_WriteShard_Success(t *testing.T) {
	ts := newTestWriteService(writeShardSuccess)
	s := cluster.NewService(cluster.Config{})
	s.Listener = ts.muxln
	s.DataStore = ts
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer ts.Close()

	w := cluster.NewShardWriter(time.Minute)
	w.MetaStore = &metaStore{host: ts.ln.Addr().String()}

	// Build a single point.
	now := time.Now()
	var messages []db.Message
	messages = append(messages, db.NewMessage(now))

	// Write to shard and close.
	if err := w.WriteShard(1, 2, messages); err != nil {
		t.Fatal(err)
	} else if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Validate response.
	responses, err := ts.ResponseN(1)
	if err != nil {
		t.Fatal(err)
	} else if responses[0].shardID != 1 {
		t.Fatalf("unexpected shard id: %d", responses[0].shardID)
	}

	// Validate message.
	m := responses[0].messages[0]
	if m.Time().UnixNano() != now.UnixNano() {
		t.Fatalf("unexpected time: %s", m.Time())
	}
}

// Ensure the shard writer can successful write a multiple requests.
func TestShardWriter_WriteShard_Multiple(t *testing.T) {
	ts := newTestWriteService(writeShardSuccess)
	s := cluster.NewService(cluster.Config{})
	s.Listener = ts.muxln
	s.DataStore = ts
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer ts.Close()

	w := cluster.NewShardWriter(time.Minute)
	w.MetaStore = &metaStore{host: ts.ln.Addr().String()}

	// Build a single point.
	now := time.Now()
	var messages []db.Message
	messages = append(messages, db.NewMessage(now))

	// Write to shard twice and close.
	if err := w.WriteShard(1, 2, messages); err != nil {
		t.Fatal(err)
	} else if err := w.WriteShard(1, 2, messages); err != nil {
		t.Fatal(err)
	} else if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Validate response.
	responses, err := ts.ResponseN(1)
	if err != nil {
		t.Fatal(err)
	} else if responses[0].shardID != 1 {
		t.Fatalf("unexpected shard id: %d", responses[0].shardID)
	}

	// Validate message.
	m := responses[0].messages[0]
	if m.Time().UnixNano() != now.UnixNano() {
		t.Fatalf("unexpected time: %s", m.Time())
	}
}

// Ensure the shard writer returns an error when the server fails to accept the write.
func TestShardWriter_WriteShard_Error(t *testing.T) {
	ts := newTestWriteService(writeShardFail)
	s := cluster.NewService(cluster.Config{})
	s.Listener = ts.muxln
	s.DataStore = ts
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer ts.Close()

	w := cluster.NewShardWriter(time.Minute)
	w.MetaStore = &metaStore{host: ts.ln.Addr().String()}
	now := time.Now()

	shardID := uint64(1)
	ownerID := uint64(2)
	var messages []db.Message
	messages = append(messages, db.NewMessage(now))

	if err := w.WriteShard(shardID, ownerID, messages); err == nil || err.Error() != "error code 1: write shard 1: failed to write" {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Ensure the shard writer returns an error when dialing times out.
func TestShardWriter_Write_ErrDialTimeout(t *testing.T) {
	ts := newTestWriteService(writeShardSuccess)
	s := cluster.NewService(cluster.Config{})
	s.Listener = ts.muxln
	s.DataStore = ts
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer ts.Close()

	w := cluster.NewShardWriter(time.Nanosecond)
	w.MetaStore = &metaStore{host: ts.ln.Addr().String()}
	now := time.Now()

	shardID := uint64(1)
	ownerID := uint64(2)
	var messages []db.Message
	messages = append(messages, db.NewMessage(now))

	if err, exp := w.WriteShard(shardID, ownerID, messages), "i/o timeout"; err == nil || !strings.Contains(err.Error(), exp) {
		t.Fatalf("expected error %v, to contain %s", err, exp)
	}
}

// Ensure the shard writer returns an error when reading times out.
func TestShardWriter_Write_ErrReadTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	w := cluster.NewShardWriter(time.Millisecond)
	w.MetaStore = &metaStore{host: ln.Addr().String()}
	now := time.Now()

	shardID := uint64(1)
	ownerID := uint64(2)
	var messages []db.Message
	messages = append(messages, db.NewMessage(now))

	if err := w.WriteShard(shardID, ownerID, messages); err == nil || !strings.Contains(err.Error(), "i/o timeout") {
		t.Fatalf("unexpected error: %s", err)
	}
}
