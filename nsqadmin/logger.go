package nsqadmin

import (
	"github.com/absolute8511/nsq/internal/levellogger"
)

var adminLog = levellogger.NewLevelLogger(levellogger.LOG_INFO, &levellogger.GLogger{})
