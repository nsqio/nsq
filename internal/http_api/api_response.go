package http_api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

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

func RequirePOST(req *http.Request, f func() (interface{}, error)) func() (interface{}, error) {
	if req.Method != "POST" {
		return func() (interface{}, error) {
			return nil, Err{405, "INVALID_REQUEST"}
		}
	}
	return f
}

func NegotiateVersionWrapper(w http.ResponseWriter, req *http.Request, f func() (interface{}, error)) {
	data, err := f()
	if err != nil {
		if acceptVersion(req) == 1 {
			RespondV1(w, err.(Err).Code, err)
		} else {
			// this handler always returns 500 for backwards compatibility
			Respond(w, 500, err.Error(), nil)
		}
		return
	}
	if acceptVersion(req) == 1 {
		RespondV1(w, 200, data)
	} else {
		Respond(w, 200, "OK", data)
	}
}

func V1Wrapper(w http.ResponseWriter, req *http.Request, f func() (interface{}, error)) {
	data, err := f()
	if err != nil {
		RespondV1(w, err.(Err).Code, err)
		return
	}
	RespondV1(w, 200, data)
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

	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
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
	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.WriteHeader(code)
	w.Write(response)
}
