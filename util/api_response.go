package util

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

func ApiResponse(w http.ResponseWriter, statusCode int, statusTxt string, data interface{}) {
	response, err := json.Marshal(struct {
		StatusCode int         `json:"status_code"`
		StatusTxt  string      `json:"status_txt"`
		Data       interface{} `json:"data"`
	}{
		statusCode,
		statusTxt,
		data,
	})
	if err != nil {
		response = []byte(fmt.Sprintf(`{"status_code":500, "status_txt":"%s", "data":null}`, err.Error()))
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.WriteHeader(statusCode)
	w.Write(response)
}
