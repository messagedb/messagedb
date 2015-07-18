package db

import (
	"sort"
	"sync"

	"github.com/messagedb/messagedb/db/internal"

	"github.com/gogo/protobuf/proto"
)

//go:generate protoc --gogo_out=. internal/meta.proto

const (
	maxStringLength = 64 * 1024
)

// Conversation represent unique series messages in a database
type Conversation struct {
	mu         sync.RWMutex
	Name       string `json:"name,omitempty"`
	Key        string
	fieldNames map[string]struct{}

	Tags  map[string]string
	index *DatabaseIndex

	id uint64
}

// HasField returns true if the measurement has a field by the given name
func (c *Conversation) HasField(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// 	_, hasField := c.fieldNames[name]
	// 	return hasField
	return true //TODO: fix this
}

// HasTagKey returns true if at least one series in this measurement has written a value for the passed in tag key
func (c *Conversation) HasTagKey(k string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// _, hasTag := m.seriesByTagKeyValue[k]
	// return hasTag
	return false // TODO: Fix this
}

// MarshalBinary encodes the object to a binary format.
func (c *Conversation) MarshalBinary() ([]byte, error) {
	var pb internal.Conversation
	pb.Key = &c.Key
	for k, v := range c.Tags {
		key := k
		value := v
		pb.Tags = append(pb.Tags, &internal.Tag{Key: &key, Value: &value})
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes the object from a binary format.
func (c *Conversation) UnmarshalBinary(buf []byte) error {
	var pb internal.Conversation
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	c.Key = pb.GetKey()
	c.Tags = make(map[string]string)
	for _, t := range pb.Tags {
		c.Tags[t.GetKey()] = t.GetValue()
	}
	return nil
}

// match returns true if all tags match the series' tags.
func (c *Conversation) match(tags map[string]string) bool {
	for k, v := range tags {
		if c.Tags[k] != v {
			return false
		}
	}
	return true
}

// Conversations represents a list of *Conversation.
type Conversations []*Conversation

func (a Conversations) Len() int           { return len(a) }
func (a Conversations) Less(i, j int) bool { return a[i].Name < a[j].Name }
func (a Conversations) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// used to convert the tag set to bytes for use as a lookup key
func marshalTags(tags map[string]string) []byte {
	// Empty maps marshal to empty bytes.
	if len(tags) == 0 {
		return nil
	}

	// Extract keys and determine final size.
	sz := (len(tags) * 2) - 1 // separators
	keys := make([]string, 0, len(tags))
	for k, v := range tags {
		keys = append(keys, k)
		sz += len(k) + len(v)
	}
	sort.Strings(keys)

	// Generate marshaled bytes.
	b := make([]byte, sz)
	buf := b
	for _, k := range keys {
		copy(buf, k)
		buf[len(k)] = '|'
		buf = buf[len(k)+1:]
	}
	for i, k := range keys {
		v := tags[k]
		copy(buf, v)
		if i < len(keys)-1 {
			buf[len(v)] = '|'
			buf = buf[len(v)+1:]
		}
	}
	return b
}
