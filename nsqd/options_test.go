package nsqd

import (
	"fmt"
	"testing"

	"github.com/nsqio/nsq/internal/test"
)

func TestOptionsValidate(t *testing.T) {
	type testCase struct {
		options  func() *Options
		expected []string
	}
	tests := []testCase{
		{
			options: func() *Options {
				o := NewOptions()
				o.SigtermMode = "a"
				return o
			},
			expected: []string{`invalid sigterm-mode="a" (valid: "shutdown", "drain")`},
		},
	}
	for i, tc := range tests {
		tc := tc
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			o := tc.options()
			err := o.Validate()
			if err == nil {
				test.Equal(t, len(tc.expected), 0)
			} else {
				v := err.(ValidationErrors)
				for n, m := range v {
					t.Logf("[%d] %s", n, m)
				}
				test.Equal(t, tc.expected, []string(v))
			}
		})
	}
}
