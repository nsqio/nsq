package util

import (
	"strings"
	"fmt"
)

type StringArray []string

func (a *StringArray) Set(s string) error {
	if strings.Contains(s, ",") {
		for _, entry := range strings.Split(s, ",") {
			*a = append(*a, entry)
		}
	} else {
		*a = append(*a, s)
	}

	return nil
}

func (a *StringArray) String() string {
	return fmt.Sprint(*a)
}
