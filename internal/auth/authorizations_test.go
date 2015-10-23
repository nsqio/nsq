package auth

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func equal(t *testing.T, act, exp interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

func nequal(t *testing.T, act, exp interface{}) {
	if reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\tnexp: %#v\n\n\tgot:  %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

func TestAuthCache(t *testing.T) {
	var reqCount int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json := `{
  "ttl": 1,
  "identity": "my_identity",
  "authorizations": [
    {
      "permissions": [
        "subscribe",
        "publish"
      ],
      "topic": ".*",
      "channels": [
        ".*"
      ]
    }
  ]
}`
		w.Write([]byte(json))
		atomic.AddInt32(&reqCount, 1)
	}))
	defer ts.Close()

	// test initial cache miss followed by cache hit
	state, err := QueryAnyAuthd([]string{ts.URL}, "127.0.0.1", "true", "secret")
	nequal(t, state, nil)
	equal(t, err, nil)
	equal(t, reqCount, int32(1))

	state, err = QueryAnyAuthd([]string{ts.URL}, "127.0.0.1", "true", "secret")
	nequal(t, state, nil)
	equal(t, err, nil)
	equal(t, reqCount, int32(1))

	// force expire cache item
	key := ts.URL + "/auth?remote_ip=127.0.0.1&secret=secret&tls=true"
	state.Expires = time.Now().Add(-1 * time.Second)
	authStateCache[key] = *state

	// test cache miss (expired) followed by cache hit
	state, err = QueryAnyAuthd([]string{ts.URL}, "127.0.0.1", "true", "secret")
	nequal(t, state, nil)
	equal(t, err, nil)
	equal(t, reqCount, int32(2))

	state, err = QueryAnyAuthd([]string{ts.URL}, "127.0.0.1", "true", "secret")
	nequal(t, state, nil)
	equal(t, err, nil)
	equal(t, reqCount, int32(2))

	// test variation on params increases request count
	state, err = QueryAnyAuthd([]string{ts.URL}, "127.0.0.2", "true", "secret")
	nequal(t, state, nil)
	equal(t, err, nil)
	equal(t, reqCount, int32(3))

	state, err = QueryAnyAuthd([]string{ts.URL}, "127.0.0.1", "false", "secret")
	nequal(t, state, nil)
	equal(t, err, nil)
	equal(t, reqCount, int32(4))

	state, err = QueryAnyAuthd([]string{ts.URL}, "127.0.0.1", "true", "secret2")
	nequal(t, state, nil)
	equal(t, err, nil)
	equal(t, reqCount, int32(5))

	state, err = QueryAnyAuthd([]string{ts.URL + "/"}, "127.0.0.1", "true", "secret")
	nequal(t, state, nil)
	equal(t, err, nil)
	equal(t, reqCount, int32(6))

	// test variations are now in cache
	state, err = QueryAnyAuthd([]string{ts.URL}, "127.0.0.2", "true", "secret")
	nequal(t, state, nil)
	equal(t, err, nil)
	equal(t, reqCount, int32(6))

	state, err = QueryAnyAuthd([]string{ts.URL}, "127.0.0.1", "false", "secret")
	nequal(t, state, nil)
	equal(t, err, nil)
	equal(t, reqCount, int32(6))

	state, err = QueryAnyAuthd([]string{ts.URL}, "127.0.0.1", "true", "secret2")
	nequal(t, state, nil)
	equal(t, err, nil)
	equal(t, reqCount, int32(6))

	state, err = QueryAnyAuthd([]string{ts.URL + "/"}, "127.0.0.1", "true", "secret")
	nequal(t, state, nil)
	equal(t, err, nil)
	equal(t, reqCount, int32(6))
}
