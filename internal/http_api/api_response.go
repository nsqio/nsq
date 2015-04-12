package http_api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/nsqio/nsq/internal/app"
)

type Decorator func(APIHandler) APIHandler

type APIHandler func(http.ResponseWriter, *http.Request, httprouter.Params) (interface{}, error)

type Err struct {
	Code int
	Text string
}

func (e Err) Error() string {
	return e.Text
}

func acceptVersion(req *http.Request) int {
	if req.Header.Get("accept") == "application/vnd.nsq; version=1.0" {
		return 1
	}

	return 0
}

func PlainText(f APIHandler) APIHandler {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
		code := 200
		data, err := f(w, req, ps)
		if err != nil {
			code = err.(Err).Code
			data = err.Error()
		}
		switch d := data.(type) {
		case string:
			w.WriteHeader(code)
			io.WriteString(w, d)
		case []byte:
			w.WriteHeader(code)
			w.Write(d)
		default:
			panic(fmt.Sprintf("unknown response type %T", data))
		}
		return nil, nil
	}
}

func NegotiateVersion(f APIHandler) APIHandler {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
		data, err := f(w, req, ps)
		if err != nil {
			if acceptVersion(req) == 1 {
				RespondV1(w, err.(Err).Code, err)
			} else {
				// this handler always returns 500 for backwards compatibility
				Respond(w, 500, err.Error(), nil)
			}
			return nil, nil
		}
		if acceptVersion(req) == 1 {
			RespondV1(w, 200, data)
		} else {
			Respond(w, 200, "OK", data)
		}
		return nil, nil
	}
}

func V1(f APIHandler) APIHandler {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
		data, err := f(w, req, ps)
		if err != nil {
			RespondV1(w, err.(Err).Code, err)
			return nil, nil
		}
		RespondV1(w, 200, data)
		return nil, nil
	}
}

func Respond(w http.ResponseWriter, statusCode int, statusTxt string, data interface{}) {
	var response []byte
	var err error

	switch data.(type) {
	case string:
		response = []byte(data.(string))
	case []byte:
		response = data.([]byte)
	default:
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		response, err = json.Marshal(struct {
			StatusCode int         `json:"status_code"`
			StatusTxt  string      `json:"status_txt"`
			Data       interface{} `json:"data"`
		}{
			statusCode,
			statusTxt,
			data,
		})
		if err != nil {
			response = []byte(fmt.Sprintf(`{"status_code":500, "status_txt":"%s", "data":null}`, err))
		}
	}

	w.WriteHeader(statusCode)
	w.Write(response)
}

func RespondV1(w http.ResponseWriter, code int, data interface{}) {
	var response []byte
	var err error
	var isJSON bool

	if code == 200 {
		switch data.(type) {
		case string:
			response = []byte(data.(string))
		case []byte:
			response = data.([]byte)
		case nil:
			response = []byte{}
		default:
			isJSON = true
			response, err = json.Marshal(data)
			if err != nil {
				code = 500
				data = err
			}
		}
	}

	if code != 200 {
		isJSON = true
		response = []byte(fmt.Sprintf(`{"message":"%s"}`, data))
	}

	if isJSON {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}
	w.Header().Set("X-NSQ-Content-Type", "nsq; version=1.0")
	w.WriteHeader(code)
	w.Write(response)
}

func Decorate(f APIHandler, ds ...Decorator) httprouter.Handle {
	decorated := f
	for _, decorate := range ds {
		decorated = decorate(decorated)
	}
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		decorated(w, req, ps)
	}
}

func Log(l app.Logger) Decorator {
	return func(f APIHandler) APIHandler {
		return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
			start := time.Now()
			response, err := f(w, req, ps)
			elapsed := time.Since(start)
			status := 200
			if e, ok := err.(Err); ok {
				status = e.Code
			}
			l.Output(2, fmt.Sprintf("%d %s %s (%s) %s",
				status, req.Method, req.URL.RequestURI(), req.RemoteAddr, elapsed))
			return response, err
		}
	}
}

func LogPanicHandler(l app.Logger) func(w http.ResponseWriter, req *http.Request, p interface{}) {
	return func(w http.ResponseWriter, req *http.Request, p interface{}) {
		l.Output(2, fmt.Sprintf("ERROR: panic in HTTP handler - %s", p))
		Decorate(func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
			return nil, Err{500, "INTERNAL_ERROR"}
		}, Log(l), V1)(w, req, nil)
	}
}

func LogNotFoundHandler(l app.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		Decorate(func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
			return nil, Err{404, "NOT_FOUND"}
		}, Log(l), V1)(w, req, nil)
	})
}

func LogMethodNotAllowedHandler(l app.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		Decorate(func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
			return nil, Err{405, "METHOD_NOT_ALLOWED"}
		}, Log(l), V1)(w, req, nil)
	})
}
