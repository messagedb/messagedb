package cluster

import (
	"time"

	"github.com/messagedb/messagedb/cluster/internal"
	"github.com/messagedb/messagedb/db"

	"github.com/gogo/protobuf/proto"
)

//go:generate protoc --gogo_out=. internal/data.proto

// MapShardRequest represents the request to map a remote shard for a query.
type MapShardRequest struct {
	pb internal.MapShardRequest
}

func (m *MapShardRequest) ShardID() uint64  { return m.pb.GetShardID() }
func (m *MapShardRequest) Query() string    { return m.pb.GetQuery() }
func (m *MapShardRequest) ChunkSize() int32 { return m.pb.GetChunkSize() }

func (m *MapShardRequest) SetShardID(id uint64)         { m.pb.ShardID = &id }
func (m *MapShardRequest) SetQuery(query string)        { m.pb.Query = &query }
func (m *MapShardRequest) SetChunkSize(chunkSize int32) { m.pb.ChunkSize = &chunkSize }

// MarshalBinary encodes the object to a binary format.
func (m *MapShardRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&m.pb)
}

// UnmarshalBinary populates MapShardRequest from a binary format.
func (m *MapShardRequest) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &m.pb); err != nil {
		return err
	}
	return nil
}

// MapShardResponse represents the response returned from a remote MapShardRequest call
type MapShardResponse struct {
	pb internal.MapShardResponse
}

func NewMapShardResponse(code int, message string) *MapShardResponse {
	m := &MapShardResponse{}
	m.SetCode(code)
	m.SetMessage(message)
	return m
}

func (r *MapShardResponse) Code() int       { return int(r.pb.GetCode()) }
func (r *MapShardResponse) Message() string { return r.pb.GetMessage() }
func (r *MapShardResponse) Data() []byte    { return r.pb.GetData() }

func (r *MapShardResponse) SetCode(code int)          { r.pb.Code = proto.Int32(int32(code)) }
func (r *MapShardResponse) SetMessage(message string) { r.pb.Message = &message }
func (r *MapShardResponse) SetData(data []byte)       { r.pb.Data = data }

// MarshalBinary encodes the object to a binary format.
func (r *MapShardResponse) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&r.pb)
}

// UnmarshalBinary populates WritePointRequest from a binary format.
func (r *MapShardResponse) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &r.pb); err != nil {
		return err
	}
	return nil
}

// WriteMessagesRequest represents a request to write message data to the cluster
type WriteMessagesRequest struct {
	Database         string
	RetentionPolicy  string
	ConsistencyLevel ConsistencyLevel
	Messages         []db.Message
}

// AddMessage adds a message to the WriteMessagesRequest with field name 'value'
func (w *WriteMessagesRequest) AddMessage(name string, value interface{}, timestamp time.Time, tags map[string]string) {
	w.Messages = append(w.Messages, db.NewMessage(timestamp))
}

// WriteShardRequest represents the a request to write a slice of messages to a shard
type WriteShardRequest struct {
	pb internal.WriteShardRequest
}

func (w *WriteShardRequest) SetShardID(id uint64) { w.pb.ShardID = &id }
func (w *WriteShardRequest) ShardID() uint64      { return w.pb.GetShardID() }

func (w *WriteShardRequest) Messages() []db.Message { return w.unmarshalMessages() }

func (w *WriteShardRequest) AddMessage(name string, value interface{}, timestamp time.Time, tags map[string]string) {
	// w.AddMessages([]db.Message{db.NewMessage(
	// 	name, tags, map[string]interface{}{"value": value}, timestamp,
	// )})
	//TODO: fix this
	w.AddMessages([]db.Message{db.NewMessage(timestamp)})
}

func (w *WriteShardRequest) AddMessages(messages []db.Message) {
	w.pb.Messages = append(w.pb.Messages, w.marshalMessages(messages)...)
}

// MarshalBinary encodes the object to a binary format.
func (w *WriteShardRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&w.pb)
}

// UnmarshalBinary populates WriteMessagesRequest from a binary format.
func (w *WriteShardRequest) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &w.pb); err != nil {
		return err
	}
	return nil
}

func (w *WriteShardRequest) marshalMessages(messages []db.Message) []*internal.Message {
	msgs := make([]*internal.Message, len(messages))
	for i, p := range messages {
		// fields := []*internal.Field{}
		// for k, v := range p.Fields() {
		// 	name := k
		// 	f := &internal.Field{
		// 		Name: &name,
		// 	}
		// 	switch t := v.(type) {
		// 	case int:
		// 		f.Int64 = proto.Int64(int64(t))
		// 	case int32:
		// 		f.Int32 = proto.Int32(t)
		// 	case int64:
		// 		f.Int64 = proto.Int64(t)
		// 	case float64:
		// 		f.Float64 = proto.Float64(t)
		// 	case bool:
		// 		f.Bool = proto.Bool(t)
		// 	case string:
		// 		f.String_ = proto.String(t)
		// 	case []byte:
		// 		f.Bytes = t
		// 	}
		// 	fields = append(fields, f)
		// }

		// tags := []*internal.Tag{}
		// for k, v := range p.Tags() {
		// 	key := k
		// 	value := v
		// 	tags = append(tags, &internal.Tag{
		// 		Key:   &key,
		// 		Value: &value,
		// 	})
		// }
		// name := p.Name()
		msgs[i] = &internal.Message{
			// Name: &name,
			Time: proto.Int64(p.Time().UnixNano()),
			// Fields: fields,
			// Tags:   tags,
		}

	}
	return msgs
}

func (w *WriteShardRequest) unmarshalMessages() []db.Message {
	messages := make([]db.Message, len(w.pb.GetMessages()))
	for i, m := range w.pb.GetMessages() {
		// msg := db.NewMessage(
		// 	m.GetName(), map[string]string{},
		// 	map[string]interface{}{}, time.Unix(0, m.GetTime()))

		msg := db.NewMessage(time.Unix(0, m.GetTime()))

		// for _, f := range m.GetFields() {
		// 	n := f.GetName()
		// 	if f.Int32 != nil {
		// 		msg.AddField(n, f.GetInt32())
		// 	} else if f.Int64 != nil {
		// 		msg.AddField(n, f.GetInt64())
		// 	} else if f.Float64 != nil {
		// 		msg.AddField(n, f.GetFloat64())
		// 	} else if f.Bool != nil {
		// 		msg.AddField(n, f.GetBool())
		// 	} else if f.String_ != nil {
		// 		msg.AddField(n, f.GetString_())
		// 	} else {
		// 		msg.AddField(n, f.GetBytes())
		// 	}
		// }

		// tags := db.Tags{}
		// for _, t := range m.GetTags() {
		// 	tags[t.GetKey()] = t.GetValue()
		// }
		// msg.SetTags(tags)
		messages[i] = msg
	}
	return messages
}

func (w *WriteShardResponse) SetCode(code int)          { w.pb.Code = proto.Int32(int32(code)) }
func (w *WriteShardResponse) SetMessage(message string) { w.pb.Message = &message }

// WriteShardResponse represents the response returned from a remote WriteShardRequest call
type WriteShardResponse struct {
	pb internal.WriteShardResponse
}

func (w *WriteShardResponse) Code() int       { return int(w.pb.GetCode()) }
func (w *WriteShardResponse) Message() string { return w.pb.GetMessage() }

// MarshalBinary encodes the object to a binary format.
func (w *WriteShardResponse) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&w.pb)
}

// UnmarshalBinary populates WriteMessagesRequest from a binary format.
func (w *WriteShardResponse) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &w.pb); err != nil {
		return err
	}
	return nil
}
