package db

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/messagedb/messagedb/messageql"
)

const (
	// Return an error if the user is trying to select more than this number of points in a group by statement.
	// Most likely they specified a group by interval without time boundaries.
	MaxGroupByPoints = 100000

	// Since time is always selected, the column count when selecting only a single other value will be 2
	SelectColumnCountWithOneValue = 2

	// IgnoredChunkSize is what gets passed into Mapper.Begin for aggregate queries as they don't chunk points out
	IgnoredChunkSize = 0
)

// MapperResponse is the structure responses from mappers take over the network. Tagsets
// is only set with the first response. Data will be nil when the Mapper has no more data.
type MapperResponse struct {
	TagSets []string `json:"tagSets,omitempty"`
	Data    []byte   `json:"data"`
}

// Mapper is the interface all Mapper types must implement.
type Mapper interface {
	Open() error
	TagSets() []string
	NextChunk() (interface{}, error)
	Close()
}

// Executor is the interface all Executor types must implement.
type Executor interface {
	Execute() <-chan *messageql.Row
}

// StatefulRawMapper encapsulates a RawMapper and some state that the executor needs to
// track for that mapper.
type StatefulRawMapper struct {
	Mapper
	bufferedChunk *rawMapperOutput // Last read chunk.
	drained       bool
}

// NextChunk wraps a RawMapper and some state.
func (srm *StatefulRawMapper) NextChunk() (*rawMapperOutput, error) {
	c, err := srm.Mapper.NextChunk()
	if err != nil {
		return nil, err
	}
	chunk, ok := c.(*rawMapperOutput)
	if !ok {
		if chunk == interface{}(nil) {
			return nil, nil
		}
	}
	return chunk, nil
}

// RawExecutor is an executor for RawMappers.
type RawExecutor struct {
	stmt           *messageql.SelectStatement
	mappers        []*StatefulRawMapper
	chunkSize      int
	limitedTagSets map[string]struct{} // Set tagsets for which data has reached the LIMIT.
}

// NewRawExecutor returns a new RawExecutor.
func NewRawExecutor(stmt *messageql.SelectStatement, mappers []Mapper, chunkSize int) *RawExecutor {
	a := []*StatefulRawMapper{}
	for _, m := range mappers {
		a = append(a, &StatefulRawMapper{m, nil, false})
	}
	return &RawExecutor{
		stmt:           stmt,
		mappers:        a,
		chunkSize:      chunkSize,
		limitedTagSets: make(map[string]struct{}),
	}
}

// Execute begins execution of the query and returns a channel to receive rows.
func (re *RawExecutor) Execute() <-chan *messageql.Row {
	// Create output channel and stream data in a separate goroutine.
	out := make(chan *messageql.Row, 0)
	go re.execute(out)
	return out
}

func (re *RawExecutor) execute(out chan *messageql.Row) {
	// It's important that all resources are released when execution completes.
	defer re.close()

	// Open the mappers.
	for _, m := range re.mappers {
		if err := m.Open(); err != nil {
			out <- &messageql.Row{Err: err}
			return
		}
	}

	// Used to read ahead chunks from mappers.
	var rowWriter *limitedRowWriter
	var currTagset string

	// Keep looping until all mappers drained.
	var err error
	for {
		// Get the next chunk from each Mapper.
		for _, m := range re.mappers {
			if m.drained {
				continue
			}

			// Set the next buffered chunk on the mapper, or mark it drained.
			for {
				if m.bufferedChunk == nil {
					m.bufferedChunk, err = m.NextChunk()
					if err != nil {
						out <- &messageql.Row{Err: err}
						return
					}
					if m.bufferedChunk == nil {
						// Mapper can do no more for us.
						m.drained = true
						break
					}
				}

				if re.tagSetIsLimited(m.bufferedChunk.Name) {
					// chunk's tagset is limited, so no good. Try again.
					m.bufferedChunk = nil
					continue
				}
				// This mapper has a chunk available, and it is not limited.
				break
			}
		}

		// All Mappers done?
		if re.mappersDrained() {
			rowWriter.Flush()
			break
		}

		// Send out data for the next alphabetically-lowest tagset. All Mappers emit data in this order,
		// so by always continuing with the lowest tagset until it is finished, we process all data in
		// the required order, and don't "miss" any.
		tagset := re.nextMapperTagSet()
		if tagset != currTagset {
			currTagset = tagset
			// Tagset has changed, time for a new rowWriter. Be sure to kick out any residual values.
			rowWriter.Flush()
			rowWriter = nil
		}

		// Process the mapper outputs. We can send out everything up to the min of the last time
		// of the chunks for the next tagset.
		minTime := re.nextMapperLowestTime(tagset)

		// Now empty out all the chunks up to the min time. Create new output struct for this data.
		var chunkedOutput *rawMapperOutput
		for _, m := range re.mappers {
			if m.drained {
				continue
			}

			// This mapper's next chunk is not for the next tagset, or the very first value of
			// the chunk is at a higher acceptable timestamp. Skip it.
			if m.bufferedChunk.key() != tagset || m.bufferedChunk.Values[0].Time > minTime {
				continue
			}

			// Find the index of the point up to the min.
			ind := len(m.bufferedChunk.Values)
			for i, mo := range m.bufferedChunk.Values {
				if mo.Time > minTime {
					ind = i
					break
				}
			}

			// Add up to the index to the values
			if chunkedOutput == nil {
				chunkedOutput = &rawMapperOutput{
					Name: m.bufferedChunk.Name,
					Tags: m.bufferedChunk.Tags,
				}
				chunkedOutput.Values = m.bufferedChunk.Values[:ind]
			} else {
				chunkedOutput.Values = append(chunkedOutput.Values, m.bufferedChunk.Values[:ind]...)
			}

			// Clear out the values being sent out, keep the remainder.
			m.bufferedChunk.Values = m.bufferedChunk.Values[ind:]

			// If we emptied out all the values, clear the mapper's buffered chunk.
			if len(m.bufferedChunk.Values) == 0 {
				m.bufferedChunk = nil
			}
		}

		// Sort the values by time first so we can then handle offset and limit
		sort.Sort(rawMapperValues(chunkedOutput.Values))

		// Now that we have full name and tag details, initialize the rowWriter.
		// The Name and Tags will be the same for all mappers.
		if rowWriter == nil {
			rowWriter = &limitedRowWriter{
				limit:       re.stmt.Limit,
				offset:      re.stmt.Offset,
				chunkSize:   re.chunkSize,
				name:        chunkedOutput.Name,
				tags:        chunkedOutput.Tags,
				selectNames: re.stmt.NamesInSelect(),
				fields:      re.stmt.Fields,
				c:           out,
			}
		}

		// Emit the data via the limiter.
		if limited := rowWriter.Add(chunkedOutput.Values); limited {
			// Limit for this tagset was reached, mark it and start draining a new tagset.
			re.limitTagSet(chunkedOutput.key())
			continue
		}
	}

	close(out)
}

// mappersDrained returns whether all the executors Mappers have been drained of data.
func (re *RawExecutor) mappersDrained() bool {
	for _, m := range re.mappers {
		if !m.drained {
			return false
		}
	}
	return true
}

// nextMapperTagset returns the alphabetically lowest tagset across all Mappers.
func (re *RawExecutor) nextMapperTagSet() string {
	tagset := ""
	for _, m := range re.mappers {
		if m.bufferedChunk != nil {
			if tagset == "" {
				tagset = m.bufferedChunk.key()
			} else if m.bufferedChunk.key() < tagset {
				tagset = m.bufferedChunk.key()
			}
		}
	}
	return tagset
}

// nextMapperLowestTime returns the lowest minimum time across all Mappers, for the given tagset.
func (re *RawExecutor) nextMapperLowestTime(tagset string) int64 {
	minTime := int64(math.MaxInt64)
	for _, m := range re.mappers {
		if !m.drained && m.bufferedChunk != nil {
			if m.bufferedChunk.key() != tagset {
				continue
			}
			t := m.bufferedChunk.Values[len(m.bufferedChunk.Values)-1].Time
			if t < minTime {
				minTime = t
			}
		}
	}
	return minTime
}

// tagSetIsLimited returns whether data for the given tagset has been LIMITed.
func (re *RawExecutor) tagSetIsLimited(tagset string) bool {
	_, ok := re.limitedTagSets[tagset]
	return ok
}

// limitTagSet marks the given taset as LIMITed.
func (re *RawExecutor) limitTagSet(tagset string) {
	re.limitedTagSets[tagset] = struct{}{}
}

// Close closes the executor such that all resources are released. Once closed,
// an executor may not be re-used.
func (re *RawExecutor) close() {
	if re != nil {
		for _, m := range re.mappers {
			m.Close()
		}
	}
}

// limitedRowWriter accepts raw mapper values, and will emit those values as rows in chunks
// of the given size. If the chunk size is 0, no chunking will be performed. In addiiton if
// limit is reached, outstanding values will be emitted. If limit is zero, no limit is enforced.
type limitedRowWriter struct {
	chunkSize   int
	limit       int
	offset      int
	name        string
	tags        map[string]string
	selectNames []string
	fields      messageql.Fields
	c           chan *messageql.Row

	currValues  []*rawMapperValue
	totalOffSet int
	totalSent   int

	transformer interface {
		process(input []*rawMapperValue) []*rawMapperValue
	}
}

// Add accepts a slice of values, and will emit those values as per chunking requirements.
// If limited is returned as true, the limit was also reached and no more values should be
// added. In that case only up the limit of values are emitted.
func (r *limitedRowWriter) Add(values []*rawMapperValue) (limited bool) {
	if r.currValues == nil {
		r.currValues = make([]*rawMapperValue, 0, r.chunkSize)
	}

	// Enforce offset.
	if r.totalOffSet < r.offset {
		// Still some offsetting to do.
		offsetRequired := r.offset - r.totalOffSet
		if offsetRequired >= len(values) {
			r.totalOffSet += len(values)
			return false
		} else {
			// Drop leading values and keep going.
			values = values[offsetRequired:]
			r.totalOffSet += offsetRequired
		}
	}
	r.currValues = append(r.currValues, values...)

	// Check limit.
	limitReached := r.limit > 0 && r.totalSent+len(r.currValues) >= r.limit
	if limitReached {
		// Limit will be satified with current values. Truncate 'em.
		r.currValues = r.currValues[:r.limit-r.totalSent]
	}

	// Is chunking in effect?
	if r.chunkSize != IgnoredChunkSize {
		// Chunking level reached?
		for len(r.currValues) >= r.chunkSize {
			index := len(r.currValues) - (len(r.currValues) - r.chunkSize)
			r.c <- r.processValues(r.currValues[:index])
			r.currValues = r.currValues[index:]
		}

		// After values have been sent out by chunking, there may still be some
		// values left, if the remainder is less than the chunk size. But if the
		// limit has been reached, kick them out.
		if len(r.currValues) > 0 && limitReached {
			r.c <- r.processValues(r.currValues)
			r.currValues = nil
		}
	} else if limitReached {
		// No chunking in effect, but the limit has been reached.
		r.c <- r.processValues(r.currValues)
		r.currValues = nil
	}

	return limitReached
}

// Flush instructs the limitedRowWriter to emit any pending values as a single row,
// adhering to any limits. Chunking is not enforced.
func (r *limitedRowWriter) Flush() {
	if r == nil {
		return
	}

	// If at least some rows were sent, and no values are pending, then don't
	// emit anything, since at least 1 row was previously emitted. This ensures
	// that if no rows were ever sent, at least 1 will be emitted, even an empty row.
	if r.totalSent != 0 && len(r.currValues) == 0 {
		return
	}

	if r.limit > 0 && len(r.currValues) > r.limit {
		r.currValues = r.currValues[:r.limit]
	}
	r.c <- r.processValues(r.currValues)
	r.currValues = nil
}

// processValues emits the given values in a single row.
func (r *limitedRowWriter) processValues(values []*rawMapperValue) *messageql.Row {
	defer func() {
		r.totalSent += len(values)
	}()

	selectNames := r.selectNames

	if r.transformer != nil {
		values = r.transformer.process(values)
	}

	// ensure that time is in the select names and in the first position
	hasTime := false
	for i, n := range selectNames {
		if n == "time" {
			// Swap time to the first argument for names
			if i != 0 {
				selectNames[0], selectNames[i] = selectNames[i], selectNames[0]
			}
			hasTime = true
			break
		}
	}

	// time should always be in the list of names they get back
	if !hasTime {
		selectNames = append([]string{"time"}, selectNames...)
	}

	// since selectNames can contain tags, we need to strip them out
	selectFields := make([]string, 0, len(selectNames))

	for _, n := range selectNames {
		if _, found := r.tags[n]; !found {
			selectFields = append(selectFields, n)
		}
	}

	row := &messageql.Row{
		Name:    r.name,
		Tags:    r.tags,
		Columns: selectFields,
	}

	// Kick out an empty row it no results available.
	if len(values) == 0 {
		return row
	}

	// if they've selected only a single value we have to handle things a little differently
	singleValue := len(selectFields) == SelectColumnCountWithOneValue

	// the results will have all of the raw mapper results, convert into the row
	for _, v := range values {
		vals := make([]interface{}, len(selectFields))

		if singleValue {
			vals[0] = time.Unix(0, v.Time).UTC()
			vals[1] = v.Value.(interface{})
		} else {
			fields := v.Value.(map[string]interface{})

			// time is always the first value
			vals[0] = time.Unix(0, v.Time).UTC()

			// populate the other values
			for i := 1; i < len(selectFields); i++ {
				vals[i] = fields[selectFields[i]]
			}
		}

		row.Values = append(row.Values, vals)
	}

	// Perform any mathematical post-processing.
	row.Values = processForMath(r.fields, row.Values)

	return row
}

// processForMath will apply any math that was specified in the select statement
// against the passed in results
func processForMath(fields messageql.Fields, results [][]interface{}) [][]interface{} {
	hasMath := false
	for _, f := range fields {
		if _, ok := f.Expr.(*messageql.BinaryExpr); ok {
			hasMath = true
		} else if _, ok := f.Expr.(*messageql.ParenExpr); ok {
			hasMath = true
		}
	}

	if !hasMath {
		return results
	}

	processors := make([]messageql.Processor, len(fields))
	startIndex := 1
	for i, f := range fields {
		processors[i], startIndex = messageql.GetProcessor(f.Expr, startIndex)
	}

	mathResults := make([][]interface{}, len(results))
	for i, _ := range mathResults {
		mathResults[i] = make([]interface{}, len(fields)+1)
		// put the time in
		mathResults[i][0] = results[i][0]
		for j, p := range processors {
			mathResults[i][j+1] = p(results[i])
		}
	}

	return mathResults
}

// processAggregateDerivative returns the derivatives of an aggregate result set
func processAggregateDerivative(results [][]interface{}, isNonNegative bool, interval time.Duration) [][]interface{} {
	// Return early if we can't calculate derivatives
	if len(results) == 0 {
		return results
	}

	// If we only have 1 value, then the value did not change, so return
	// a single row w/ 0.0
	if len(results) == 1 {
		return [][]interface{}{
			[]interface{}{results[0][0], 0.0},
		}
	}

	// Otherwise calculate the derivatives as the difference between consecutive
	// points divided by the elapsed time.  Then normalize to the requested
	// interval.
	derivatives := [][]interface{}{}
	for i := 1; i < len(results); i++ {
		prev := results[i-1]
		cur := results[i]

		if cur[1] == nil || prev[1] == nil {
			continue
		}

		elapsed := cur[0].(time.Time).Sub(prev[0].(time.Time))
		diff := int64toFloat64(cur[1]) - int64toFloat64(prev[1])
		value := 0.0
		if elapsed > 0 {
			value = float64(diff) / (float64(elapsed) / float64(interval))
		}

		// Drop negative values for non-negative derivatives
		if isNonNegative && diff < 0 {
			continue
		}

		val := []interface{}{
			cur[0],
			value,
		}
		derivatives = append(derivatives, val)
	}

	return derivatives
}

// resultsEmpty will return true if the all the result values are empty or contain only nulls
func resultsEmpty(resultValues [][]interface{}) bool {
	for _, vals := range resultValues {
		// start the loop at 1 because we want to skip over the time value
		for i := 1; i < len(vals); i++ {
			if vals[i] != nil {
				return false
			}
		}
	}
	return true
}

func int64toFloat64(v interface{}) float64 {
	switch v.(type) {
	case int64:
		return float64(v.(int64))
	case float64:
		return v.(float64)
	}
	panic(fmt.Sprintf("expected either int64 or float64, got %v", v))
}

type int64arr []int64

func (a int64arr) Len() int           { return len(a) }
func (a int64arr) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64arr) Less(i, j int) bool { return a[i] < a[j] }
