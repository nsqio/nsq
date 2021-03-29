package main

import (
	"fmt"

	"github.com/nsqio/nsq/internal/lg"
)

type config map[string]interface{}

// Validate settings in the config file, and fatal on errors
func (cfg config) Validate() {
	if v, exists := cfg["log_level"]; exists {
		var t lg.LogLevel
		err := t.Set(fmt.Sprintf("%v", v))
		if err == nil {
			cfg["log_level"] = t
		} else {
			logFatal("failed parsing log_level %+v", v)
		}
	}
}
