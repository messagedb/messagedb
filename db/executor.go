package db

import (
	"fmt"
	"math"
	"sort"

	"github.com/messagedb/messagedb/sql"
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

// Mapper is the interface all Mapper types must implement.
type Mapper interface {
	Open() error
	NextChunk() (interface{}, error)
	Close()
}

// StatefulMapper encapsulates a Mapper and some state that the executor needs to
// track for that mapper.
type StatefulMapper struct {
	Mapper
	bufferedChunk *MapperOutput // Last read chunk.
	drained       bool
}

// NextChunk wraps a RawMapper and some state.
func (sm *StatefulMapper) NextChunk() (*MapperOutput, error) {
	c, err := sm.Mapper.NextChunk()
	if err != nil {
		return nil, err
	}
	chunk, ok := c.(*MapperOutput)
	if !ok {
		if chunk == interface{}(nil) {
			return nil, nil
		}
	}
	return chunk, nil
}

type Executor struct {
	stmt           *sql.SelectStatement
	mappers        []*StatefulMapper
	chunkSize      int
	limitedTagSets map[string]struct{} // Set tagsets for which data has reached the LIMIT.
}

// NewExecutor returns a new Executor.
func NewExecutor(stmt *sql.SelectStatement, mappers []Mapper, chunkSize int) *Executor {
	a := []*StatefulMapper{}
	for _, m := range mappers {
		a = append(a, &StatefulMapper{m, nil, false})
	}
	return &Executor{
		stmt:           stmt,
		mappers:        a,
		chunkSize:      chunkSize,
		limitedTagSets: make(map[string]struct{}),
	}
}

// Execute begins execution of the query and returns a channel to receive rows.
func (e *Executor) Execute() <-chan *sql.Row {
	// Create output channel and stream data in a separate goroutine.
	out := make(chan *sql.Row, 0)

	go e.execute(out)
	return out
}

// mappersDrained returns whether all the executors Mappers have been drained of data.
func (e *Executor) mappersDrained() bool {
	for _, m := range e.mappers {
		if !m.drained {
			return false
		}
	}
	return true
}

// nextMapperTagset returns the alphabetically lowest tagset across all Mappers.
func (e *Executor) nextMapperTagSet() string {
	tagset := ""
	for _, m := range e.mappers {
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
func (e *Executor) nextMapperLowestTime(tagset string) int64 {
	minTime := int64(math.MaxInt64)
	for _, m := range e.mappers {
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
func (e *Executor) tagSetIsLimited(tagset string) bool {
	_, ok := e.limitedTagSets[tagset]
	return ok
}

// limitTagSet marks the given taset as LIMITed.
func (e *Executor) limitTagSet(tagset string) {
	e.limitedTagSets[tagset] = struct{}{}
}

func (e *Executor) execute(out chan *sql.Row) {
	// It's important that all resources are released when execution completes.
	defer e.close()

	// Open the mappers.
	for _, m := range e.mappers {
		if err := m.Open(); err != nil {
			out <- &sql.Row{Err: err}
			return
		}
	}

	// Used to read ahead chunks from mappers.
	var rw *rowWriter
	var currTagset string

	// Keep looping until all mappers drained.
	var err error
	for {
		// Get the next chunk from each Mapper.
		for _, m := range e.mappers {
			if m.drained {
				continue
			}

			// Set the next buffered chunk on the mapper, or mark it drained.
			for {
				if m.bufferedChunk == nil {
					m.bufferedChunk, err = m.NextChunk()
					if err != nil {
						out <- &sql.Row{Err: err}
						return
					}
					if m.bufferedChunk == nil {
						// Mapper can do no more for us.
						m.drained = true
						break
					}
				}

				if e.tagSetIsLimited(m.bufferedChunk.Name) {
					// chunk's tagset is limited, so no good. Try again.
					m.bufferedChunk = nil
					continue
				}
				// This mapper has a chunk available, and it is not limited.
				break
			}
		}

		// All Mappers done?
		if e.mappersDrained() {
			rw.Flush()
			break
		}

		// Send out data for the next alphabetically-lowest tagset. All Mappers emit data in this order,
		// so by always continuing with the lowest tagset until it is finished, we process all data in
		// the required order, and don't "miss" any.
		tagset := e.nextMapperTagSet()
		if tagset != currTagset {
			currTagset = tagset
			// Tagset has changed, time for a new rowWriter. Be sure to kick out any residual values.
			rw.Flush()
			rw = nil
		}

		// Process the mapper outputs. We can send out everything up to the min of the last time
		// of the chunks for the next tagset.
		minTime := e.nextMapperLowestTime(tagset)

		// Now empty out all the chunks up to the min time. Create new output struct for this data.
		var chunkedOutput *MapperOutput
		for _, m := range e.mappers {
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
				chunkedOutput = &MapperOutput{
					Name: m.bufferedChunk.Name,
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
		sort.Sort(mapperValues(chunkedOutput.Values))

		// Now that we have full name and tag details, initialize the rowWriter.
		// The Name and Tags will be the same for all mappers.
		if rw == nil {
			rw = &rowWriter{
				limit:       e.stmt.Limit,
				offset:      e.stmt.Offset,
				chunkSize:   e.chunkSize,
				name:        chunkedOutput.Name,
				selectNames: e.stmt.NamesInSelect(),
				fields:      e.stmt.Fields,
				c:           out,
			}
		}

		// Emit the data via the limiter.
		if limited := rw.Add(chunkedOutput.Values); limited {
			// Limit for this tagset was reached, mark it and start draining a new tagset.
			e.limitTagSet(chunkedOutput.key())
			continue
		}
	}

	close(out)
}

// Close closes the executor such that all resources are released. Once closed,
// an executor may not be re-used.
func (e *Executor) close() {
	if e != nil {
		for _, m := range e.mappers {
			m.Close()
		}
	}
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
