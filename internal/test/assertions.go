package test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func Equal(t *testing.T, expected, actual interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\t   %#v (expected)\n\n\t!= %#v (actual)\033[39m\n\n",
			filepath.Base(file), line, expected, actual)
		t.FailNow()
	}
}

func NotEqual(t *testing.T, expected, actual interface{}) {
	if reflect.DeepEqual(expected, actual) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\tnexp: %#v\n\n\tgot:  %#v\033[39m\n\n",
			filepath.Base(file), line, expected, actual)
		t.FailNow()
	}
}

func Nil(t *testing.T, object interface{}) {
	if !isNil(object) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\t   <nil> (expected)\n\n\t!= %#v (actual)\033[39m\n\n",
			filepath.Base(file), line, object)
		t.FailNow()
	}
}

func NotNil(t *testing.T, object interface{}) {
	if isNil(object) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\tExpected value not to be <nil>\033[39m\n\n",
			filepath.Base(file), line)
		t.FailNow()
	}
}

func isNil(object interface{}) bool {
	if object == nil {
		return true
	}

	value := reflect.ValueOf(object)
	kind := value.Kind()
	if kind >= reflect.Chan && kind <= reflect.Slice && value.IsNil() {
		return true
	}

	return false
}

func HTTPError(t *testing.T, resp *http.Response, code int, message string) {
	type ErrMessage struct {
		Message string `json:"message"`
	}

	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	t.Log(string(body))
	Equal(t, code, resp.StatusCode)

	var em ErrMessage
	Nil(t, json.Unmarshal(body, &em))
	Equal(t, message, em.Message)
}
