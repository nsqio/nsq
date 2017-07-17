package contrib

import (
	"testing"
	"github.com/nsqio/nsq/internal/test"
)



type TestAddon struct {
	numStartCalls int
}

func (ta *TestAddon) Start() {
	ta.numStartCalls += 1
}

func (ta *TestAddon) Enabled() bool {
	return true
}


func TestStartMultipleAddons(t *testing.T) {
	ta1 := &TestAddon{}
	ta2 := &TestAddon{}

	as := &NSQDAddons{
		addons: []INSQDAddon{ta1, ta2},
	}
	as.Start()

	test.Equal(t, ta1.numStartCalls, 1)
	test.Equal(t, ta2.numStartCalls, 1)
}