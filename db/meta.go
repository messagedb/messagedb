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

// tagSets returns the unique tag sets that exist for the given tag keys. This is used to determine
// what composite series will be created by a group by. i.e. "group by region" should return:
// {"region":"uswest"}, {"region":"useast"}
// or region, service returns
// {"region": "uswest", "service": "redis"}, {"region": "uswest", "service": "mysql"}, etc...
// This will also populate the TagSet objects with the series IDs that match each tagset and any
// influx filter expression that goes with the series
// TODO: this shouldn't be exported. However, until tx.go and the engine get refactored into tsdb, we need it.
// func (c *Conversation) TagSets(stmt *sql.SelectStatement, dimensions []string) ([]*sql.TagSet, error) {
// 	c.index.mu.RLock()
// 	defer c.index.mu.RUnlock()
// 	c.mu.RLock()
// 	defer c.mu.RUnlock()
//
// 	// get the unique set of series ids and the filters that should be applied to each
// 	filters, err := c.filters(stmt)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	// For every series, get the tag values for the requested tag keys i.e. dimensions. This is the
// 	// TagSet for that series. Series with the same TagSet are then grouped together, because for the
// 	// purpose of GROUP BY they are part of the same composite series.
// 	tagSets := make(map[string]*sql.TagSet)
// 	for id, filter := range filters {
// 		s := c.conversationsByID[id]
// 		tags := make(map[string]string)
//
// 		// Build the TagSet for this series.
// 		for _, dim := range dimensions {
// 			tags[dim] = s.Tags[dim]
// 		}
//
// 		// Convert the TagSet to a string, so it can be added to a map allowing TagSets to be handled
// 		// as a set.
// 		tagsAsKey := string(marshalTags(tags))
// 		tagSet, ok := tagSets[tagsAsKey]
// 		if !ok {
// 			// This TagSet is new, create a new entry for it.
// 			tagSet = &sql.TagSet{}
// 			tagsForSet := make(map[string]string)
// 			for k, v := range tags {
// 				tagsForSet[k] = v
// 			}
// 			tagSet.Tags = tagsForSet
// 			tagSet.Key = marshalTags(tagsForSet)
// 		}
//
// 		// Associate the series and filter with the Tagset.
// 		tagSet.AddFilter(c.conversationsByID[id].Key, filter)
//
// 		// Ensure it's back in the map.
// 		tagSets[tagsAsKey] = tagSet
// 	}
//
// 	// The TagSets have been created, as a map of TagSets. Just send
// 	// the values back as a slice, sorting for consistency.
// 	sortedTagSetKeys := make([]string, 0, len(tagSets))
// 	for k, _ := range tagSets {
// 		sortedTagSetKeys = append(sortedTagSetKeys, k)
// 	}
// 	sort.Strings(sortedTagSetKeys)
//
// 	sortedTagsSets := make([]*sql.TagSet, 0, len(sortedTagSetKeys))
// 	for _, k := range sortedTagSetKeys {
// 		sortedTagsSets = append(sortedTagsSets, tagSets[k])
// 	}
//
// 	return sortedTagsSets, nil
// }

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

// stringSet represents a set of strings.
type stringSet map[string]struct{}

// newStringSet returns an empty stringSet.
func newStringSet() stringSet {
	return make(map[string]struct{})
}

// add adds strings to the set.
func (s stringSet) add(ss ...string) {
	for _, n := range ss {
		s[n] = struct{}{}
	}
}

// contains returns whether the set contains the given string.
func (s stringSet) contains(ss string) bool {
	_, ok := s[ss]
	return ok
}

// list returns the current elements in the set, in sorted order.
func (s stringSet) list() []string {
	l := make([]string, 0, len(s))
	for k := range s {
		l = append(l, k)
	}
	sort.Strings(l)
	return l
}

// union returns the union of this set and another.
func (s stringSet) union(o stringSet) stringSet {
	ns := newStringSet()
	for k := range s {
		ns[k] = struct{}{}
	}
	for k := range o {
		ns[k] = struct{}{}
	}
	return ns
}

// union returns the intersection of this set and another.
func (s stringSet) intersect(o stringSet) stringSet {
	ns := newStringSet()
	for k := range s {
		if _, ok := o[k]; ok {
			ns[k] = struct{}{}
		}
	}
	for k := range o {
		if _, ok := s[k]; ok {
			ns[k] = struct{}{}
		}
	}
	return ns
}
