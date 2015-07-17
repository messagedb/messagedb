package messageql

import (
	"fmt"
)

// Iterator represents a forward-only iterator over a set of points.
// These are used by the MapFunctions in this file
type Iterator interface {
	Next() (conversationsKey string, time int64, value interface{})
}

// MapFunc represents a function used for mapping over a sequential series of data.
// The iterator represents a single group by interval
type MapFunc func(Iterator) interface{}

// ReduceFunc represents a function used for reducing mapper output.
type ReduceFunc func([]interface{}) interface{}

// UnmarshalFunc represents a function that can take bytes from a mapper from remote
// server and marshal it into an interface the reducer can use
type UnmarshalFunc func([]byte) (interface{}, error)

// InitializeMapFunc takes an aggregate call from the query and returns the MapFunc
func InitializeMapFunc(c *Call) (MapFunc, error) {
	// see if it's a query for raw data
	return MapRawQuery, nil
}

// MapRawQuery is for queries without aggregates
func MapRawQuery(itr Iterator) interface{} {
	var values []*rawQueryMapOutput
	for _, k, v := itr.Next(); k != 0; _, k, v = itr.Next() {
		val := &rawQueryMapOutput{k, v}
		values = append(values, val)
	}
	return values
}

// MapCount computes the number of values in an iterator.
func MapCount(itr Iterator) interface{} {
	n := float64(0)
	for _, k, _ := itr.Next(); k != 0; _, k, _ = itr.Next() {
		n++
	}
	if n > 0 {
		return n
	}
	return nil
}

type rawQueryMapOutput struct {
	Time   int64
	Values interface{}
}

func (r *rawQueryMapOutput) String() string {
	return fmt.Sprintf("{%#v %#v}", r.Time, r.Values)
}

type rawOutputs []*rawQueryMapOutput

func (a rawOutputs) Len() int           { return len(a) }
func (a rawOutputs) Less(i, j int) bool { return a[i].Time < a[j].Time }
func (a rawOutputs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
