package util

import (
	"encoding/json"
	"fmt"
)

func ApiResponse(statusCode int, statusTxt string, data interface{}) []byte {
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
		return []byte(errorTxt)
	}
	return response
}
