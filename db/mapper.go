package db

import (
	"encoding/binary"
	"sort"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/messagedb/messagedb/messageql"
)

type rawMapperValue struct {
	Time  int64       `json:"time,omitempty"`
	Value interface{} `json:"value,omitempty"`
}

type rawMapperValues []*rawMapperValue

func (a rawMapperValues) Len() int           { return len(a) }
func (a rawMapperValues) Less(i, j int) bool { return a[i].Time < a[j].Time }
func (a rawMapperValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type rawMapperOutput struct {
	Name   string            `json:"name,omitempty"`
	Tags   map[string]string `json:"tags,omitempty"`
	Values rawMapperValues   `json:"values,omitempty"`
}

func (mo *rawMapperOutput) key() string {
	return formMeasurementTagSetKey(mo.Name, mo.Tags)
}

// RawMapper is for retrieving data, for a raw query, for a single shard.
type RawMapper struct {
	shard     *Shard
	stmt      *messageql.SelectStatement
	chunkSize int

	tx        *bolt.Tx // Read transaction for this shard.
	queryTMin int64
	queryTMax int64

	whereFields  []string               // field names that occur in the where clause
	selectFields []string               // field names that occur in the select clause
	selectTags   []string               // tag keys that occur in the select clause
	fieldName    string                 // the field name being read.
	decoders     map[string]*FieldCodec // byte decoder per measurement

	cursors         []*tagSetCursor // Cursors per tag sets.
	currCursorIndex int             // Current tagset cursor being drained.
}

// NewRawMapper returns a mapper for the given shard, which will return data for the SELECT statement.
func NewRawMapper(shard *Shard, stmt *messageql.SelectStatement, chunkSize int) *RawMapper {
	return &RawMapper{
		shard:     shard,
		stmt:      stmt,
		chunkSize: chunkSize,
		cursors:   make([]*tagSetCursor, 0),
	}
}

// Open opens the raw mapper.
func (rm *RawMapper) Open() error {
	// Get a read-only transaction.
	// tx, err := rm.shard.DB().Begin(false)
	// if err != nil {
	// 	return err
	// }
	// rm.tx = tx
	//
	// // Set all time-related parameters on the mapper.
	// rm.queryTMin, rm.queryTMax = messageql.TimeRangeAsEpochNano(rm.stmt.Condition)
	//
	// // Create the TagSet cursors for the Mapper.
	// for _, src := range rm.stmt.Sources {
	// 	mm, ok := src.(*messageql.Conversation)
	// 	if !ok {
	// 		return fmt.Errorf("invalid source type: %#v", src)
	// 	}
	//
	// 	m := rm.shard.index.Conversation(mm.Name)
	// 	if m == nil {
	// 		// This shard have never received data for the measurement. No Mapper
	// 		// required.
	// 		return nil
	// 	}
	//
	// 	// Create tagset cursors and determine various field types within SELECT statement.
	// 	tsf, err := createTagSetsAndFields(m, rm.stmt)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	tagSets := tsf.tagSets
	// 	rm.selectFields = tsf.selectFields
	// 	rm.selectTags = tsf.selectTags
	// 	rm.whereFields = tsf.whereFields
	//
	// 	if len(rm.selectFields) == 0 {
	// 		return fmt.Errorf("select statement must include at least one field")
	// 	}
	//
	// 	// // SLIMIT and SOFFSET the unique series
	// 	// if rm.stmt.SLimit > 0 || rm.stmt.SOffset > 0 {
	// 	// 	if rm.stmt.SOffset > len(tagSets) {
	// 	// 		tagSets = nil
	// 	// 	} else {
	// 	// 		if rm.stmt.SOffset+rm.stmt.SLimit > len(tagSets) {
	// 	// 			rm.stmt.SLimit = len(tagSets) - rm.stmt.SOffset
	// 	// 		}
	// 	//
	// 	// 		tagSets = tagSets[rm.stmt.SOffset : rm.stmt.SOffset+rm.stmt.SLimit]
	// 	// 	}
	// 	// }
	//
	// 	// Create all cursors for reading the data from this shard.
	// 	for _, t := range tagSets {
	// 		cursors := []*seriesCursor{}
	//
	// 		for i, key := range t.SeriesKeys {
	// 			c := createCursorForSeries(rm.tx, rm.shard, key)
	// 			if c == nil {
	// 				// No data exists for this key.
	// 				continue
	// 			}
	// 			cm := newSeriesCursor(c, t.Filters[i])
	// 			cm.SeekTo(rm.queryTMin)
	// 			cursors = append(cursors, cm)
	// 		}
	// 		// tsc := newTagSetCursor(m.Name, t.Tags, cursors, rm.shard.FieldCodec(m.Name))
	// 		//(marcio): replace nil here since we have no FieldCode in messageDB
	// 		tsc := newTagSetCursor(m.Name, t.Tags, cursors, nil)
	// 		rm.cursors = append(rm.cursors, tsc)
	// 	}
	// 	sort.Sort(tagSetCursors(rm.cursors))
	// }

	return nil
}

// TagSets returns the list of TagSets for which this mapper has data.
func (rm *RawMapper) TagSets() []string {
	return tagSetCursors(rm.cursors).Keys()
}

// NextChunk returns the next chunk of data. Data comes in the same order as the
// tags return by TagSets. A chunk never contains data for more than 1 tagset.
// If there is no more data for any tagset, nil will be returned.
func (rm *RawMapper) NextChunk() (interface{}, error) {
	var output *rawMapperOutput
	for {
		if rm.currCursorIndex == len(rm.cursors) {
			// All tagset cursors processed. NextChunk'ing complete.
			return nil, nil
		}
		cursor := rm.cursors[rm.currCursorIndex]

		_, k, v := cursor.Next(rm.queryTMin, rm.queryTMax, rm.selectFields, rm.whereFields)
		if v == nil {
			// Tagset cursor is empty, move to next one.
			rm.currCursorIndex++
			if output != nil {
				// There is data, so return it and continue when next called.
				return output, nil
			} else {
				// Just go straight to the next cursor.
				continue
			}
		}

		if output == nil {
			output = &rawMapperOutput{
				Name: cursor.measurement,
				Tags: cursor.tags,
			}
		}
		value := &rawMapperValue{Time: k, Value: v}
		output.Values = append(output.Values, value)
		if len(output.Values) == rm.chunkSize {
			return output, nil
		}
	}
}

// Close closes the mapper.
func (rm *RawMapper) Close() {
	if rm != nil && rm.tx != nil {
		_ = rm.tx.Rollback()
	}
}

// tagSetCursor is virtual cursor that iterates over mutiple series cursors, as though it were
// a single series.
type tagSetCursor struct {
	measurement string            // Measurement name
	tags        map[string]string // Tag key-value pairs
	cursors     []*seriesCursor   // Underlying series cursors.
	decoder     *FieldCodec       // decoder for the raw data bytes
}

// tagSetCursors represents a sortable slice of tagSetCursors.
type tagSetCursors []*tagSetCursor

func (a tagSetCursors) Len() int           { return len(a) }
func (a tagSetCursors) Less(i, j int) bool { return a[i].key() < a[j].key() }
func (a tagSetCursors) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (a tagSetCursors) Keys() []string {
	keys := []string{}
	for i := range a {
		keys = append(keys, a[i].key())
	}
	sort.Strings(keys)
	return keys
}

// newTagSetCursor returns a tagSetCursor
func newTagSetCursor(m string, t map[string]string, c []*seriesCursor, d *FieldCodec) *tagSetCursor {
	return &tagSetCursor{
		measurement: m,
		tags:        t,
		cursors:     c,
		decoder:     d,
	}
}

func (tsc *tagSetCursor) key() string {
	return formMeasurementTagSetKey(tsc.measurement, tsc.tags)
}

// Next returns the next matching series-key, timestamp and byte slice for the tagset. Filtering
// is enforced on the values. If there is no matching value, then a nil result is returned.
func (tsc *tagSetCursor) Next(tmin, tmax int64, selectFields, whereFields []string) (string, int64, interface{}) {
	for {
		// Find the cursor with the lowest timestamp, as that is the one to be read next.
		minCursor := tsc.nextCursor(tmin, tmax)
		if minCursor == nil {
			// No cursor of this tagset has any matching data.
			return "", 0, nil
		}
		timestamp, bytes := minCursor.Next()

		var value interface{}
		if len(selectFields) > 1 {
			if fieldsWithNames, err := tsc.decoder.DecodeFieldsWithNames(bytes); err == nil {
				value = fieldsWithNames

				// if there's a where clause, make sure we don't need to filter this value
				if minCursor.filter != nil && !matchesWhere(minCursor.filter, fieldsWithNames) {
					value = nil
				}
			}
		} else {
			// With only 1 field SELECTed, decoding all fields may be avoidable, which is faster.
			var err error
			value, err = tsc.decoder.DecodeByName(selectFields[0], bytes)
			if err != nil {
				continue
			}

			// If there's a WHERE clase, see if we need to filter
			if minCursor.filter != nil {
				// See if the WHERE is only on this field or on one or more other fields.
				// If the latter, we'll have to decode everything
				if len(whereFields) == 1 && whereFields[0] == selectFields[0] {
					if !matchesWhere(minCursor.filter, map[string]interface{}{selectFields[0]: value}) {
						value = nil
					}
				} else { // Decode everything
					fieldsWithNames, err := tsc.decoder.DecodeFieldsWithNames(bytes)
					if err != nil || !matchesWhere(minCursor.filter, fieldsWithNames) {
						value = nil
					}
				}
			}
		}

		// Value didn't match, look for the next one.
		if value == nil {
			continue
		}

		return "", timestamp, value
	}
}

// SeekTo seeks each underlying cursor to the specified key.
func (tsc *tagSetCursor) SeekTo(key int64) {
	for _, c := range tsc.cursors {
		c.SeekTo(key)
	}
}

// IsEmpty returns whether the tagsetCursor has any more data for the given interval.
func (tsc *tagSetCursor) IsEmptyForInterval(tmin, tmax int64) bool {
	for _, c := range tsc.cursors {
		k, _ := c.Peek()
		if k != 0 && k >= tmin && k <= tmax {
			return false
		}
	}
	return true
}

// nextCursor returns the series cursor with the lowest next timestamp, within in the specified
// range. If none exists, nil is returned.
func (tsc *tagSetCursor) nextCursor(tmin, tmax int64) *seriesCursor {
	var minCursor *seriesCursor
	var timestamp int64
	for _, c := range tsc.cursors {
		timestamp, _ = c.Peek()
		if timestamp != 0 && ((timestamp == tmin) || (timestamp >= tmin && timestamp < tmax)) {
			if minCursor == nil {
				minCursor = c
			} else {
				if currMinTimestamp, _ := minCursor.Peek(); timestamp < currMinTimestamp {
					minCursor = c
				}
			}
		}
	}
	return minCursor
}

// seriesCursor is a cursor that walks a single series. It provides lookahead functionality.
type seriesCursor struct {
	cursor      *shardCursor // BoltDB cursor for a series
	filter      messageql.Expr
	keyBuffer   int64  // The current timestamp key for the cursor
	valueBuffer []byte // The current value for the cursor
}

// newSeriesCursor returns a new instance of a series cursor.
func newSeriesCursor(b *shardCursor, filter messageql.Expr) *seriesCursor {
	return &seriesCursor{
		cursor:    b,
		filter:    filter,
		keyBuffer: -1, // Nothing buffered.
	}
}

// Peek returns the next timestamp and value, without changing what will be
// be returned by a call to Next()
func (mc *seriesCursor) Peek() (key int64, value []byte) {
	if mc.keyBuffer == -1 {
		k, v := mc.cursor.Next()
		if k == nil {
			mc.keyBuffer = 0
		} else {
			mc.keyBuffer = int64(btou64(k))
			mc.valueBuffer = v
		}
	}

	key, value = mc.keyBuffer, mc.valueBuffer
	return
}

// SeekTo positions the cursor at the key, such that Next() will return
// the key and value at key.
func (mc *seriesCursor) SeekTo(key int64) {
	k, v := mc.cursor.Seek(u64tob(uint64(key)))
	if k == nil {
		mc.keyBuffer = 0
	} else {
		mc.keyBuffer, mc.valueBuffer = int64(btou64(k)), v
	}
}

// Next returns the next timestamp and value from the cursor.
func (mc *seriesCursor) Next() (key int64, value []byte) {
	if mc.keyBuffer != -1 {
		key, value = mc.keyBuffer, mc.valueBuffer
		mc.keyBuffer, mc.valueBuffer = -1, nil
	} else {
		k, v := mc.cursor.Next()
		if k == nil {
			key = 0
		} else {
			key, value = int64(btou64(k)), v
		}
	}
	return
}

// createCursorForSeries creates a cursor for walking the given series key. The cursor
// consolidates both the Bolt store and any WAL cache.
func createCursorForSeries(tx *bolt.Tx, shard *Shard, key string) *shardCursor {
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
	tagSets      []*messageql.TagSet
	selectFields []string
	selectTags   []string
	whereFields  []string
}

// // createTagSetsAndFields returns the tagsets and various fields given a measurement and
// // SELECT statement. It also ensures that the fields and tags exist.
// func createTagSetsAndFields(m *Conversation, stmt *messageql.SelectStatement) (*tagSetsAndFields, error) {
// 	_, tagKeys, err := stmt.Dimensions.Normalize()
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	sfs := newStringSet()
// 	sts := newStringSet()
// 	wfs := newStringSet()
//
// 	// Validate the fields and tags asked for exist and keep track of which are in the select vs the where
// 	for _, n := range stmt.NamesInSelect() {
// 		if m.HasField(n) {
// 			sfs.add(n)
// 			continue
// 		}
// 		if !m.HasTagKey(n) {
// 			return nil, fmt.Errorf("unknown field or tag name in select clause: %s", n)
// 		}
// 		sts.add(n)
// 		tagKeys = append(tagKeys, n)
// 	}
// 	for _, n := range stmt.NamesInWhere() {
// 		if n == "time" {
// 			continue
// 		}
// 		if m.HasField(n) {
// 			wfs.add(n)
// 			continue
// 		}
// 		if !m.HasTagKey(n) {
// 			return nil, fmt.Errorf("unknown field or tag name in where clause: %s", n)
// 		}
// 	}
//
// 	// Get the sorted unique tag sets for this statement.
// 	tagSets, err := m.TagSets(stmt, tagKeys)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	return &tagSetsAndFields{
// 		tagSets:      tagSets,
// 		selectFields: sfs.list(),
// 		selectTags:   sts.list(),
// 		whereFields:  wfs.list(),
// 	}, nil
// }

// matchesFilter returns true if the value matches the where clause
func matchesWhere(f messageql.Expr, fields map[string]interface{}) bool {
	if ok, _ := messageql.Eval(f, fields).(bool); !ok {
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
