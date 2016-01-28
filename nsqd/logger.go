package nsqd

import (
	"github.com/absolute8511/nsq/internal/levellogger"
)

var nsqLog = levellogger.NewLevelLogger(1, &levellogger.GLogger{})

func NsqLogger() *levellogger.LevelLogger {
	return nsqLog
}
