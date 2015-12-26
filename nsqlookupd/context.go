package nsqlookupd

import (
	"github.com/absolute8511/nsq/internal/levellogger"
)

type Context struct {
	nsqlookupd *NSQLookupd
	log        levellogger.Logger
}
