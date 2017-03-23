package nsqd

import (
	"github.com/absolute8511/nsq/internal/levellogger"
)

var nsqLog = levellogger.NewLevelLogger(levellogger.LOG_INFO, &levellogger.SimpleLogger{})

func SetLogger(log levellogger.Logger) {
	nsqLog.Logger = log
}

func NsqLogger() *levellogger.LevelLogger {
	return nsqLog
}
