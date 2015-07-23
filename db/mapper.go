package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/messagedb/messagedb/sql"
)

// mapperValue is a complex type, which can encapsulate data from both raw and aggregate
// mappers. This currently allows marshalling and network system to remain simpler. For
// aggregate output Time is ignored, and actual Time-Value pairs are contained soley
// within the Value field.
type mapperValue struct {
	Time  int64       `json:"time,omitempty"`  // Ignored for aggregate output.
	Value interface{} `json:"value,omitempty"` // For aggregate, contains interval time multiple values.
}

type mapperValues []*mapperValue

func (a mapperValues) Len() int           { return len(a) }
func (a mapperValues) Less(i, j int) bool { return a[i].Time < a[j].Time }
func (a mapperValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type MapperOutput struct {
	Name   string         `json:"name,omitempty"`
	Values []*mapperValue `json:"values,omitempty"` // For aggregates contains a single value at [0]
}

func (mo *MapperOutput) key() string {
	return mo.Name
}

// LocalMapper is for retrieving data for a query, from a given shard.
type LocalMapper struct {
	shard           *Shard
	stmt            sql.Statement
	selectStmt      *sql.SelectStatement
	rawMode         bool
	chunkSize       int
	tx              *bolt.Tx              // Read transaction for this shard.
	queryTMin       int64                 // Minimum time of the query.
	queryTMax       int64                 // Maximum time of the query.
	whereFields     []string              // field names that occur in the where clause
	selectFields    []string              // field names that occur in the select clause
	selectTags      []string              // tag keys that occur in the select clause
	cursors         []*conversationCursor // Cursors per tag sets.
	currCursorIndex int                   // Current tagset cursor being drained.

	// The following attributes are only used when mappers are for aggregate queries.

	queryTMinWindow int64         // Minimum time of the query floored to start of interval.
	intervalSize    int64         // Size of each interval.
	numIntervals    int           // Maximum number of intervals to return.
	currInterval    int           // Current interval for which data is being fetched.
	mapFuncs        []sql.MapFunc // The mapping functions.
	fieldNames      []string      // the field name being read for mapping.
}

// NewLocalMapper returns a mapper for the given shard, which will return data for the SELECT statement.
func NewLocalMapper(shard *Shard, stmt sql.Statement, chunkSize int) *LocalMapper {
	m := &LocalMapper{
		shard:     shard,
		stmt:      stmt,
		chunkSize: chunkSize,
		cursors:   make([]*conversationCursor, 0),
	}

	if s, ok := stmt.(*sql.SelectStatement); ok {
		m.selectStmt = s
		// m.rawMode = (s.IsRawQuery && !s.HasDistinct()) || s.IsSimpleDerivative()
		m.rawMode = true
	}
	return m
}

// openMeta opens the mapper for a meta query.
func (lm *LocalMapper) openMeta() error {
	return errors.New("not implemented")
}

// Open opens the local mapper.
func (lm *LocalMapper) Open() error {
	var err error

	// Get a read-only transaction.
	tx, err := lm.shard.DB().Begin(false)
	if err != nil {
		return err
	}
	lm.tx = tx

	if lm.selectStmt == nil {
		return lm.openMeta()
	}

	// Set all time-related parameters on the mapper.
	lm.queryTMin, lm.queryTMax = sql.TimeRangeAsEpochNano(lm.selectStmt.Condition)

	whereFields := newStringSet()

	// Create the TagSet cursors for the Mapper.
	for _, src := range lm.selectStmt.Sources {
		mm, ok := src.(*sql.Conversation)
		if !ok {
			return fmt.Errorf("invalid source type: %#v", src)
		}

		c := lm.shard.index.Conversation(mm.Name)
		if c == nil {
			// This shard have never received data for the measurement. No Mapper
			// required.
			return nil
		}

		wfs := newStringSet()
		for _, n := range lm.selectStmt.NamesInWhere() {
			if n == "time" {
				continue
			}
			if c.HasField(n) {
				wfs.add(n)
				continue
			}
		}
		whereFields.add(wfs.list()...)

		shardCursor := createCursorForConversation(lm.tx, lm.shard, mm.Name)
		if shardCursor == nil {
			// No data exists for this key.
			continue
		}

		convCursor := newConversationCursor(shardCursor, nil)
		convCursor.SeekTo(lm.queryTMin)
		lm.cursors = append(lm.cursors, convCursor)

		sort.Sort(conversationCursors(lm.cursors))
	}

	lm.whereFields = whereFields.list()

	return nil
}

// NextChunk returns the next chunk of data
func (lm *LocalMapper) NextChunk() (interface{}, error) {
	var output *MapperOutput
	for {
		if lm.currCursorIndex == len(lm.cursors) {
			// All tagset cursors processed. NextChunk'ing complete.
			return nil, nil
		}
		cursor := lm.cursors[lm.currCursorIndex]

		k, v := cursor.Next()
		if v == nil {
			// Tagset cursor is empty, move to next one.
			lm.currCursorIndex++
			if output != nil {
				// There is data, so return it and continue when next called.
				return output, nil
			} else {
				// Just go straight to the next cursor.
				continue
			}
		}

		if output == nil {
			output = &MapperOutput{
				Name: cursor.conversation,
			}
		}
		value := &mapperValue{Time: k, Value: v}
		output.Values = append(output.Values, value)
		if len(output.Values) == lm.chunkSize {
			return output, nil
		}
	}
}

// Close closes the mapper.
func (lm *LocalMapper) Close() {
	if lm != nil && lm.tx != nil {
		_ = lm.tx.Rollback()
	}
}

// conversationCursor is a cursor that walks a single conversation. It provides lookahead functionality.
type conversationCursor struct {
	conversation string       // Measurement name
	cursor       *shardCursor // BoltDB cursor for a series
	filter       sql.Expr
	keyBuffer    int64  // The current timestamp key for the cursor
	valueBuffer  []byte // The current value for the cursor
}

// conversationCursors represents a sortable slice of conversationCursors.
type conversationCursors []*conversationCursor

func (a conversationCursors) Len() int           { return len(a) }
func (a conversationCursors) Less(i, j int) bool { return a[i].key() < a[j].key() }
func (a conversationCursors) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (a conversationCursors) Keys() []string {
	keys := []string{}
	for i := range a {
		keys = append(keys, a[i].key())
	}
	sort.Strings(keys)
	return keys
}

// newSeriesCursor returns a new instance of a series cursor.
func newConversationCursor(b *shardCursor, filter sql.Expr) *conversationCursor {
	return &conversationCursor{
		cursor:    b,
		filter:    filter,
		keyBuffer: -1, // Nothing buffered.
	}
}

func (cc *conversationCursor) key() string {
	return cc.conversation
}

// Peek returns the next timestamp and value, without changing what will be
// be returned by a call to Next()
func (cc *conversationCursor) Peek() (key int64, value []byte) {
	if cc.keyBuffer == -1 {
		k, v := cc.cursor.Next()
		if k == nil {
			cc.keyBuffer = 0
		} else {
			cc.keyBuffer = int64(btou64(k))
			cc.valueBuffer = v
		}
	}

	key, value = cc.keyBuffer, cc.valueBuffer
	return
}

// SeekTo positions the cursor at the key, such that Next() will return
// the key and value at key.
func (cc *conversationCursor) SeekTo(key int64) {
	k, v := cc.cursor.Seek(u64tob(uint64(key)))
	if k == nil {
		cc.keyBuffer = 0
	} else {
		cc.keyBuffer, cc.valueBuffer = int64(btou64(k)), v
	}
}

// Next returns the next timestamp and value from the cursor.
func (cc *conversationCursor) Next() (key int64, value []byte) {
	if cc.keyBuffer != -1 {
		key, value = cc.keyBuffer, cc.valueBuffer
		cc.keyBuffer, cc.valueBuffer = -1, nil
	} else {
		k, v := cc.cursor.Next()
		if k == nil {
			key = 0
		} else {
			key, value = int64(btou64(k)), v
		}
	}
	return
}

// createCursorForConversation creates a cursor for walking the given series key. The cursor
// consolidates both the Bolt store and any WAL cache.
func createCursorForConversation(tx *bolt.Tx, shard *Shard, key string) *shardCursor {
	// Retrieve key bucket.
	b := tx.Bucket([]byte(key))

	// Ignore if there is no bucket or points in the cache.
	partitionID := WALPartition([]byte(key))
	if b == nil && len(shard.cache[partitionID][key]) == 0 {
		return nil
	}

	// Retrieve a copy of the in-cache points for the key.
	cache := make([][]byte, len(shard.cache[partitionID][key]))
	copy(cache, shard.cache[partitionID][key])

	// Build a cursor that merges the bucket and cache together.
	cur := &shardCursor{cache: cache}
	if b != nil {
		cur.cursor = b.Cursor()
	}
	return cur
}

type tagSetsAndFields struct {
	tagSets      []*sql.TagSet
	selectFields []string
	selectTags   []string
	whereFields  []string
}

// createTagSetsAndFields returns the tagsets and various fields given a measurement and
// SELECT statement.
func createTagSetsAndFields(c *Conversation, stmt *sql.SelectStatement) (*tagSetsAndFields, error) {
	_, tagKeys, err := stmt.Dimensions.Normalize()
	if err != nil {
		return nil, err
	}

	sfs := newStringSet()
	sts := newStringSet()
	wfs := newStringSet()

	// Validate the fields and tags asked for exist and keep track of which are in the select vs the where
	for _, n := range stmt.NamesInSelect() {
		if c.HasField(n) {
			sfs.add(n)
			continue
		}
		if c.HasTagKey(n) {
			sts.add(n)
			tagKeys = append(tagKeys, n)
		}
	}
	for _, n := range stmt.NamesInWhere() {
		if n == "time" {
			continue
		}
		if c.HasField(n) {
			wfs.add(n)
			continue
		}
	}

	// Get the sorted unique tag sets for this statement.
	// tagSets, err := c.TagSets(stmt, tagKeys)
	// if err != nil {
	// 	return nil, err
	// }

	return &tagSetsAndFields{
		// tagSets:      tagSets,
		selectFields: sfs.list(),
		selectTags:   sts.list(),
		whereFields:  wfs.list(),
	}, nil
}

// matchesFilter returns true if the value matches the where clause
func matchesWhere(f sql.Expr, fields map[string]interface{}) bool {
	if ok, _ := sql.Eval(f, fields).(bool); !ok {
		return false
	}
	return true
}

func formMeasurementTagSetKey(name string, tags map[string]string) string {
	if len(tags) == 0 {
		return name
	}
	return strings.Join([]string{name, string(marshalTags(tags))}, "|")
}

// btou64 converts an 8-byte slice into an uint64.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }
