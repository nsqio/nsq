package nsqlookupd

import (
	"github.com/absolute8511/nsq/internal/levellogger"
)

var nsqlookupLog = levellogger.NewLevelLogger(1, &levellogger.GLogger{})
