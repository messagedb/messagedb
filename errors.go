package messagedb

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
)

var (
	// ErrFieldsRequired is returned when a point does not any fields.
	ErrFieldsRequired = errors.New("fields required")

	// ErrFieldTypeConflict is returned when a new field already exists with a different type.
	ErrFieldTypeConflict = errors.New("field type conflict")
)

func ErrDatabaseNotFound(name string) error { return fmt.Errorf("database not found: %s", name) }

func Errorf(format string, a ...interface{}) (err error) {
	if _, file, line, ok := runtime.Caller(2); ok {
		a = append(a, file, line)
		err = fmt.Errorf(format+" (%s:%d)", a...)
	} else {
		err = fmt.Errorf(format, a...)
	}
	return
}

// mustMarshal encodes a value to JSON.
// This will panic if an error occurs. This should only be used internally when
// an invalid marshal will cause corruption and a panic is appropriate.
func mustMarshalJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("marshal: " + err.Error())
	}
	return b
}

// mustUnmarshalJSON decodes a value from JSON.
// This will panic if an error occurs. This should only be used internally when
// an invalid unmarshal will cause corruption and a panic is appropriate.
func mustUnmarshalJSON(b []byte, v interface{}) {
	if err := json.Unmarshal(b, v); err != nil {
		panic("unmarshal: " + err.Error())
	}
}

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}
