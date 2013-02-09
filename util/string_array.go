package util

import (
	"strings"
	"fmt"
)

type StringArray []string

func (a *StringArray) Set(s string) error {
	for _, entry := range strings.Split(s, ",") {
		*a = append(*a, entry)
	}

	return nil
}

func (a *StringArray) String() string {
	return fmt.Sprint(*a)
}
