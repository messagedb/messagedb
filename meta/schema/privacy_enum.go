// generated by jsonenums -type=Privacy -suffix=_enum; DO NOT EDIT

package schema

import (
	"encoding/json"
	"fmt"
)

var (
	_PrivacyNameToValue = map[string]Privacy{
		"PrivacyPersonal":  PrivacyPersonal,
		"PrivacyPublic":    PrivacyPublic,
		"PrivacyPrivate":   PrivacyPrivate,
		"PrivacyProtected": PrivacyProtected,
		"PrivacySecret":    PrivacySecret,
	}

	_PrivacyValueToName = map[Privacy]string{
		PrivacyPersonal:  "PrivacyPersonal",
		PrivacyPublic:    "PrivacyPublic",
		PrivacyPrivate:   "PrivacyPrivate",
		PrivacyProtected: "PrivacyProtected",
		PrivacySecret:    "PrivacySecret",
	}
)

func init() {
	var v Privacy
	if _, ok := interface{}(v).(fmt.Stringer); ok {
		_PrivacyNameToValue = map[string]Privacy{
			interface{}(PrivacyPersonal).(fmt.Stringer).String():  PrivacyPersonal,
			interface{}(PrivacyPublic).(fmt.Stringer).String():    PrivacyPublic,
			interface{}(PrivacyPrivate).(fmt.Stringer).String():   PrivacyPrivate,
			interface{}(PrivacyProtected).(fmt.Stringer).String(): PrivacyProtected,
			interface{}(PrivacySecret).(fmt.Stringer).String():    PrivacySecret,
		}
	}
}

// MarshalJSON is generated so Privacy satisfies json.Marshaler.
func (r Privacy) MarshalJSON() ([]byte, error) {
	if s, ok := interface{}(r).(fmt.Stringer); ok {
		return json.Marshal(s.String())
	}
	s, ok := _PrivacyValueToName[r]
	if !ok {
		return nil, fmt.Errorf("invalid Privacy: %d", r)
	}
	return json.Marshal(s)
}

// UnmarshalJSON is generated so Privacy satisfies json.Unmarshaler.
func (r *Privacy) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("Privacy should be a string, got %s", data)
	}
	v, ok := _PrivacyNameToValue[s]
	if !ok {
		return fmt.Errorf("invalid Privacy %q", s)
	}
	*r = v
	return nil
}
