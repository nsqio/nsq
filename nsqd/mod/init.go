package mod
// optional modules for nsqd

import (
	"fmt"
	"strings"

	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/nsqd"
	"github.com/nsqio/nsq/nsqd/mod/iface"

	"github.com/nsqio/nsq/nsqd/mod/dogstatsd"
)

var builtinMods = map[string]iface.ModInitFunc{
	"dogstatsd": dogstatsd.Init,
}

var activeMods = map[string]iface.NSQDModule{}

func Init(opts []string, n *nsqd.NSQD) error {
	// group options by module name
	aggOpts := make(map[string][]string)
	for _, s := range opts {
		pair := strings.SplitN(s, "=", 2)
		if len(pair) != 2 {
			return fmt.Errorf("missing option name in mod-opt: '%s'", s)
		}
		cur, ok := aggOpts[pair[0]]
		if ok {
			aggOpts[pair[0]] = append(cur, pair[1])
		} else {
			aggOpts[pair[0]] = []string{pair[1]}
		}
	}

	// call module init functions to validate options
	for name, modOpts := range aggOpts {
		modInit, ok := builtinMods[name]
		if ok {
			m, err := modInit(modOpts)
			if err != nil {
				return err
			}
			activeMods[name] = m
		} else {
			return fmt.Errorf("module not found: '%s'", name)
		}
		n.Logf(lg.INFO, "Initialized module '%s'", name)
	}

	// start all activated modules
	for _, m := range activeMods {
		err := m.Start(n)
		if err != nil {
			return err
		}
	}
	return nil
}

// func Check()
// func Stop()
