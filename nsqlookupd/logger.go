package nsqlookupd

import (
	"github.com/absolute8511/nsq/consistence"
	"github.com/absolute8511/nsq/internal/levellogger"
)

var nsqlookupLog = levellogger.NewLevelLogger(1, &levellogger.SimpleLogger{})

func SetLogger(logger levellogger.Logger, level int32) {
	nsqlookupLog.Logger = logger
	nsqlookupLog.SetLevel(level)
	consistence.SetCoordLogger(logger, level)
}
