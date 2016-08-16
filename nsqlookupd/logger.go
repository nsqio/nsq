package nsqlookupd

import (
	"github.com/absolute8511/nsq/consistence"
	"github.com/absolute8511/nsq/internal/levellogger"
)

var nsqlookupLog = levellogger.NewLevelLogger(1, &levellogger.GLogger{})

func SetLogger(opts *Options) {
	nsqlookupLog.Logger = opts.Logger
	nsqlookupLog.SetLevel(opts.LogLevel)
	consistence.SetCoordLogger(opts.Logger, opts.LogLevel)
}
