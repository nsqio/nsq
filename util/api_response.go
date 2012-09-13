package util

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func ApiResponse(w http.ResponseWriter, statusCode int, statusTxt string, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
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
		errorTxt := fmt.Sprintf(`{"status_code":500, "status_txt":"%s", "data":null}`, err.Error())
		w.Write([]byte(errorTxt))
	} else {
		w.Write(response)
	}
}
