package cluster

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/messagedb/messagedb/db"
)

// remoteShardResponder implements the remoteShardConn interface.
type remoteShardResponder struct {
	t       *testing.T
	rxBytes []byte

	buffer *bytes.Buffer
}

func newRemoteShardResponder(outputs []*db.MapperOutput, tagsets []string) *remoteShardResponder {
	r := &remoteShardResponder{}
	a := make([]byte, 0, 1024)
	r.buffer = bytes.NewBuffer(a)

	// Pump the outputs in the buffer for later reading.
	for _, o := range outputs {
		resp := &MapShardResponse{}
		resp.SetCode(0)
		if o != nil {
			d, _ := json.Marshal(o)
			resp.SetData(d)
		}

		g, _ := resp.MarshalBinary()
		WriteTLV(r.buffer, mapShardResponseMessage, g)
	}

	return r
}

func (r remoteShardResponder) MarkUnusable() { return }
func (r remoteShardResponder) Close() error  { return nil }
func (r remoteShardResponder) Read(p []byte) (n int, err error) {
	return io.ReadFull(r.buffer, p)
}

func (r remoteShardResponder) Write(p []byte) (n int, err error) {
	if r.rxBytes == nil {
		r.rxBytes = make([]byte, 0)
	}
	r.rxBytes = append(r.rxBytes, p...)
	return len(p), nil
}

// Ensure a RemoteMapper can process valid responses from a remote shard.
func TestShardWriter_RemoteMapper_Success(t *testing.T) {
	expTagSets := []string{"tagsetA"}
	expOutput := &db.MapperOutput{
		Name: "cpu",
	}

	c := newRemoteShardResponder([]*db.MapperOutput{expOutput, nil}, expTagSets)

	r := NewRemoteMapper(c, 1234, "SELECT * FROM CPU", 10)
	if err := r.Open(); err != nil {
		t.Fatalf("failed to open remote mapper: %s", err.Error())
	}

	// Get first chunk from mapper.
	chunk, err := r.NextChunk()
	if err != nil {
		t.Fatalf("failed to get next chunk from mapper: %s", err.Error())
	}
	output, ok := chunk.(*db.MapperOutput)
	if !ok {
		t.Fatal("chunk is not of expected type")
	}
	if output.Name != "cpu" {
		t.Fatalf("received output incorrect, exp: %v, got %v", expOutput, output)
	}

	// Next chunk should be nil, indicating no more data.
	chunk, err = r.NextChunk()
	if err != nil {
		t.Fatalf("failed to get next chunk from mapper: %s", err.Error())
	}
	if chunk != nil {
		t.Fatal("received more chunks when none expected")
	}
}
