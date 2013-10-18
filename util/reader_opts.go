package util

import (
	"errors"
	"github.com/bitly/go-nsq"
	"strings"
)

func ParseReaderOpts(r *nsq.Reader, opts StringArray) error {
	var err error
	for _, opt := range opts {
		parts := strings.Split(opt, ",")
		key := parts[0]
		switch len(parts) {
		case 1:
			// default options specified without a value to boolean true
			err = r.Configure(key, true)
		case 2:
			err = r.Configure(key, parts[1])
		default:
			err = errors.New("--reader-opt cannot have more than 2 parameters")
		}
		if err != nil {
			return err
		}
	}
	return nil
}
