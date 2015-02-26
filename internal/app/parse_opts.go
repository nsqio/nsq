package app

import (
	"errors"
	"strings"

	"github.com/bitly/go-nsq"
)

func ParseOpts(cfg *nsq.Config, opts StringArray) error {
	var err error
	for _, opt := range opts {
		parts := strings.Split(opt, ",")
		key := parts[0]
		switch len(parts) {
		case 1:
			// default options specified without a value to boolean true
			err = cfg.Set(key, true)
		case 2:
			err = cfg.Set(key, parts[1])
		default:
			err = errors.New("cannot have more than 2 parameters")
		}
		if err != nil {
			return err
		}
	}
	return nil
}
