package consistence

import (
	"github.com/absolute8511/nsq/internal/levellogger"
)

var coordLog = levellogger.NewLevelLogger(levellogger.LOG_INFO, &levellogger.GLogger{})

func SetCoordLogger(log levellogger.Logger, level int32) {
	coordLog.Logger = log
	coordLog.SetLevel(level)
}

func SetCoordLogLevel(level int32) {
	coordLog.SetLevel(level)
}
