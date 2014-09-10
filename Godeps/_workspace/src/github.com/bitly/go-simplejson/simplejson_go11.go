// +build go1.1

package simplejson

import (
	"bytes"
	"encoding/json"
	"errors"
)

// Implements the json.Unmarshaler interface.
func (j *Json) UnmarshalJSON(p []byte) error {
	dec := json.NewDecoder(bytes.NewBuffer(p))
	dec.UseNumber()
	return dec.Decode(&j.data)
}

// Float64 type asserts to `json.Number` then converts to `float64`
func (j *Json) Float64() (float64, error) {
	if n, ok := (j.data).(json.Number); ok {
		return n.Float64()
	}
	if f, ok := (j.data).(float64); ok {
		return f, nil
	}
	return -1, errors.New("type assertion to json.Number failed")
}

// Int type asserts to `json.Number` then converts to `int`
func (j *Json) Int() (int, error) {
	if n, ok := (j.data).(json.Number); ok {
		i, ok := n.Int64()
		return int(i), ok
	}
	if f, ok := (j.data).(float64); ok {
		return int(f), nil
	}
	return -1, errors.New("type assertion to json.Number failed")
}

// Int type asserts to `json.Number` then converts to `int64`
func (j *Json) Int64() (int64, error) {
	if n, ok := (j.data).(json.Number); ok {
		return n.Int64()
	}
	if f, ok := (j.data).(float64); ok {
		return int64(f), nil
	}
	return -1, errors.New("type assertion to json.Number failed")
}
