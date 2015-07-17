package db

import (
	"regexp"
	"sort"
	"sync"

	"github.com/messagedb/messagedb/db/internal"
	"github.com/messagedb/messagedb/messageql"

	"github.com/gogo/protobuf/proto"
)

//go:generate protoc --gogo_out=. internal/meta.proto

const (
	maxStringLength = 64 * 1024
)

// DatabaseIndex is the in memory index of a collection of conversations, and their tags.
// Exported functions are goroutine safe while un-exported functions assume the caller will use the appropriate locks
type DatabaseIndex struct {
	// in memory metadata index, built on load and updated when new series come in
	mu            sync.RWMutex
	conversations map[string]*Conversation // map conversations key to the Conversations object
	names         []string                 // sorted list of the conversations names
	lastID        uint64                   // last used conversations ID. They're in memory only for this shard
}

// NewDatabaseIndex creates the in memory index
func NewDatabaseIndex() *DatabaseIndex {
	return &DatabaseIndex{
		conversations: make(map[string]*Conversation),
		names:         make([]string, 0),
	}
}

// Conversation returns the measurement object from the index by the name
func (db *DatabaseIndex) Conversation(name string) *Conversation {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.conversations[name]
}

// ConversationsCount returns the number of conversations currently indexed by the database.
// Useful for reporting and monitoring.
func (db *DatabaseIndex) ConversationsCount() (nConversations int) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	nConversations = len(db.conversations)
	return
}

// createSeriesIndexIfNotExists adds the series for the given measurement to the index and sets its ID or returns the existing series object
func (db *DatabaseIndex) createConversationIndexIfNotExists(name string, conversation *Conversation) *Conversation {
	// if there is a measurement for this id, it's already been added
	cc := db.conversations[conversation.Key]
	if cc != nil {
		return cc
	}

	// set the in memory ID for query processing on this shard
	conversation.id = db.lastID + 1
	db.lastID++

	db.conversations[conversation.Key] = conversation

	return conversation
}

// conversationsByExpr takes and expression containing only tags and returns
// a list of matching *Measurement.
// func (db *DatabaseIndex) conversationsByExpr(expr messageql.Expr) (Conversations, error) {
// 	switch e := expr.(type) {
// 	case *messageql.BinaryExpr:
// 		switch e.Op {
// 		case messageql.EQ, messageql.NEQ, messageql.EQREGEX, messageql.NEQREGEX:
// 			tag, ok := e.LHS.(*messageql.VarRef)
// 			if !ok {
// 				return nil, fmt.Errorf("left side of '%s' must be a tag name", e.Op.String())
// 			}
//
// 			tf := &TagFilter{
// 				Op:  e.Op,
// 				Key: tag.Val,
// 			}
//
// 			if messageql.IsRegexOp(e.Op) {
// 				re, ok := e.RHS.(*messageql.RegexLiteral)
// 				if !ok {
// 					return nil, fmt.Errorf("right side of '%s' must be a regular expression", e.Op.String())
// 				}
// 				tf.Regex = re.Val
// 			} else {
// 				s, ok := e.RHS.(*messageql.StringLiteral)
// 				if !ok {
// 					return nil, fmt.Errorf("right side of '%s' must be a tag value string", e.Op.String())
// 				}
// 				tf.Value = s.Val
// 			}
//
// 			return db.conversationsByTagFilters([]*TagFilter{tf}), nil
// 		case messageql.OR, messageql.AND:
// 			lhsIDs, err := db.conversationsByExpr(e.LHS)
// 			if err != nil {
// 				return nil, err
// 			}
//
// 			rhsIDs, err := db.conversationsByExpr(e.RHS)
// 			if err != nil {
// 				return nil, err
// 			}
//
// 			if e.Op == messageql.OR {
// 				return lhsIDs.union(rhsIDs), nil
// 			}
//
// 			return lhsIDs.intersect(rhsIDs), nil
// 		default:
// 			return nil, fmt.Errorf("invalid operator")
// 		}
// 	case *messageql.ParenExpr:
// 		return db.conversationsByExpr(e.Expr)
// 	}
// 	return nil, fmt.Errorf("%#v", expr)
// }

// conversationsByTagFilters returns the conversations matching the filters on tag values.
// func (db *DatabaseIndex) conversationsByTagFilters(filters []*TagFilter) Conversations {
// 	// If no filters, then return all conversations.
// 	if len(filters) == 0 {
// 		conversations := make(Conversations, 0, len(db.conversations))
// 		for _, m := range db.conversations {
// 			conversations = append(conversations, m)
// 		}
// 		return conversations
// 	}
//
// 	// Build a list of conversations matching the filters.
// 	var conversations Conversations
// 	var tagMatch bool
//
// 	// Iterate through all conversations in the database.
// 	for _, m := range db.conversations {
// 		// Iterate filters seeing if the conversation has a matching tag.
// 		for _, f := range filters {
// 			tagVals, ok := m.seriesByTagKeyValue[f.Key]
// 			if !ok {
// 				continue
// 			}
//
// 			tagMatch = false
//
// 			// If the operator is non-regex, only check the specified value.
// 			if f.Op == messageql.EQ || f.Op == messageql.NEQ {
// 				if _, ok := tagVals[f.Value]; ok {
// 					tagMatch = true
// 				}
// 			} else {
// 				// Else, the operator is regex and we have to check all tag
// 				// values against the regular expression.
// 				for tagVal := range tagVals {
// 					if f.Regex.MatchString(tagVal) {
// 						tagMatch = true
// 						break
// 					}
// 				}
// 			}
//
// 			isEQ := (f.Op == messageql.EQ || f.Op == messageql.EQREGEX)
//
// 			// tags match | operation is EQ | measurement matches
// 			// --------------------------------------------------
// 			//     True   |       True      |      True
// 			//     True   |       False     |      False
// 			//     False  |       True      |      False
// 			//     False  |       False     |      True
//
// 			if tagMatch == isEQ {
// 				conversations = append(conversations, m)
// 				break
// 			}
// 		}
// 	}
//
// 	return conversations
// }

// conversationsByRegex returns the conversations that match the regex.
func (db *DatabaseIndex) conversationsByRegex(re *regexp.Regexp) Conversations {
	var matches Conversations
	for _, c := range db.conversations {
		if re.MatchString(c.Name) {
			matches = append(matches, c)
		}
	}
	return matches
}

// Conversations returns a list of all conversations.
func (db *DatabaseIndex) Conversations() Conversations {
	conversations := make(Conversations, 0, len(db.conversations))
	for _, m := range db.conversations {
		conversations = append(conversations, m)
	}
	return conversations
}

func (db *DatabaseIndex) DropConversation(name string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	c := db.conversations[name]
	if c == nil {
		return
	}

	delete(db.conversations, name)

	var names []string
	for _, n := range db.names {
		if n != name {
			names = append(names, n)
		}
	}
	db.names = names
}

// Conversations represents a list of *Conversation.
type Conversations []*Conversation

func (a Conversations) Len() int           { return len(a) }
func (a Conversations) Less(i, j int) bool { return a[i].Name < a[j].Name }
func (a Conversations) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (a Conversations) intersect(other Conversations) Conversations {
	l := a
	r := other

	// we want to iterate through the shortest one and stop
	if len(other) < len(a) {
		l = other
		r = a
	}

	// they're in sorted order so advance the counter as needed.
	// That is, don't run comparisons against lower values that we've already passed
	var i, j int

	result := make(Conversations, 0, len(l))
	for i < len(l) && j < len(r) {
		if l[i].Name == r[j].Name {
			result = append(result, l[i])
			i++
			j++
		} else if l[i].Name < r[j].Name {
			i++
		} else {
			j++
		}
	}

	return result
}

func (a Conversations) union(other Conversations) Conversations {
	result := make(Conversations, 0, len(a)+len(other))
	var i, j int
	for i < len(a) && j < len(other) {
		if a[i].Name == other[j].Name {
			result = append(result, a[i])
			i++
			j++
		} else if a[i].Name < other[j].Name {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, other[j])
			j++
		}
	}

	// now append the remainder
	if i < len(a) {
		result = append(result, a[i:]...)
	} else if j < len(other) {
		result = append(result, other[j:]...)
	}

	return result
}

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

// conversationIDs is a convenience type for sorting, checking equality, and doing
// union and intersection of collections of series ids.
type conversationIDs []uint64

func (a conversationIDs) Len() int           { return len(a) }
func (a conversationIDs) Less(i, j int) bool { return a[i] < a[j] }
func (a conversationIDs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// equals assumes that both are sorted.
func (a conversationIDs) equals(other conversationIDs) bool {
	if len(a) != len(other) {
		return false
	}
	for i, s := range other {
		if a[i] != s {
			return false
		}
	}
	return true
}

// intersect returns a new collection of series ids in sorted order that is the intersection of the two.
// The two collections must already be sorted.
func (a conversationIDs) intersect(other conversationIDs) conversationIDs {
	l := a
	r := other

	// we want to iterate through the shortest one and stop
	if len(other) < len(a) {
		l = other
		r = a
	}

	// they're in sorted order so advance the counter as needed.
	// That is, don't run comparisons against lower values that we've already passed
	var i, j int

	ids := make([]uint64, 0, len(l))
	for i < len(l) && j < len(r) {
		if l[i] == r[j] {
			ids = append(ids, l[i])
			i++
			j++
		} else if l[i] < r[j] {
			i++
		} else {
			j++
		}
	}

	return conversationIDs(ids)
}

// union returns a new collection of series ids in sorted order that is the union of the two.
// The two collections must already be sorted.
func (a conversationIDs) union(other conversationIDs) conversationIDs {
	l := a
	r := other
	ids := make([]uint64, 0, len(l)+len(r))
	var i, j int
	for i < len(l) && j < len(r) {
		if l[i] == r[j] {
			ids = append(ids, l[i])
			i++
			j++
		} else if l[i] < r[j] {
			ids = append(ids, l[i])
			i++
		} else {
			ids = append(ids, r[j])
			j++
		}
	}

	// now append the remainder
	if i < len(l) {
		ids = append(ids, l[i:]...)
	} else if j < len(r) {
		ids = append(ids, r[j:]...)
	}

	return ids
}

// reject returns a new collection of series ids in sorted order with the passed in set removed from the original.
// This is useful for the NOT operator. The two collections must already be sorted.
func (a conversationIDs) reject(other conversationIDs) conversationIDs {
	l := a
	r := other
	var i, j int

	ids := make([]uint64, 0, len(l))
	for i < len(l) && j < len(r) {
		if l[i] == r[j] {
			i++
			j++
		} else if l[i] < r[j] {
			ids = append(ids, l[i])
			i++
		} else {
			j++
		}
	}

	// Append the remainder
	if i < len(l) {
		ids = append(ids, l[i:]...)
	}

	return conversationIDs(ids)
}

// TagFilter represents a tag filter when looking up other tags or measurements.
type TagFilter struct {
	Op    messageql.Token
	Key   string
	Value string
	Regex *regexp.Regexp
}

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

// TagKeys returns a list of the measurement's tag names.
// func (c *Conversation) TagKeys() []string {
// 	c.mu.RLock()
// 	defer c.mu.RUnlock()
// 	keys := make([]string, 0, len(c.conversationsByTagKeyValue))
// 	for k := range c.conversationsByTagKeyValue {
// 		keys = append(keys, k)
// 	}
// 	sort.Strings(keys)
// 	return keys
// }

// func (c *Conversation) tagValuesByKeyAndConversationsID(tagKeys []string, ids conversationIDs) map[string]stringSet {
// 	// If no tag keys were passed, get all tag keys for the measurement.
// 	if len(tagKeys) == 0 {
// 		for k := range c.conversationsByTagKeyValue {
// 			tagKeys = append(tagKeys, k)
// 		}
// 	}
//
// 	// Mapping between tag keys to all existing tag values.
// 	tagValues := make(map[string]stringSet, 0)
//
// 	// Iterate all conversations to collect tag values.
// 	for _, id := range ids {
//
// 		// Iterate the tag keys we're interested in and collect values
// 		// from this series, if they exist.
// 		for _, tagKey := range tagKeys {
// 			if tagVal, ok := c.Tags[tagKey]; ok {
// 				if _, ok = tagValues[tagKey]; !ok {
// 					tagValues[tagKey] = newStringSet()
// 				}
// 				tagValues[tagKey].add(tagVal)
// 			}
// 		}
// 	}
//
// 	return tagValues
// }

// stringSet represents a set of strings.
type stringSet map[string]struct{}

// newStringSet returns an empty stringSet.
func newStringSet() stringSet {
	return make(map[string]struct{})
}

// add adds a string to the set.
func (s stringSet) add(ss string) {
	s[ss] = struct{}{}
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
