package protocol

import (
	"strings"
	"testing"

	"github.com/nsqio/nsq/internal/test"
)

func TestIsValidTopicName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		valid bool
	}{
		{"simple", "test", true},
		{"single char", "a", true},
		{"digits", "0123456789", true},
		{"dots", "test.topic", true},
		{"underscores", "test_topic", true},
		{"dashes", "test-topic", true},
		{"mixed case", "TestTopic", true},
		{"all valid chars", "aZ0._-", true},
		{"ephemeral", "test#ephemeral", true},
		{"ephemeral only suffix", "a#ephemeral", true},
		{"max length", strings.Repeat("a", 64), true},

		{"empty", "", false},
		{"too long", strings.Repeat("a", 65), false},
		{"ephemeral pushing over max length", strings.Repeat("a", 56) + "#ephemeral", false},
		{"space", "test topic", false},
		{"hash without ephemeral", "test#", false},
		{"bare ephemeral suffix", "#ephemeral", false},
		{"misspelled ephemeral", "test#ephemera", false},
		{"ephemeral not at end", "test#ephemeralx", false},
		{"double ephemeral", "test#ephemeral#ephemeral", false},
		{"slash", "test/topic", false},
		{"colon", "test:topic", false},
		{"newline", "test\ntopic", false},
		{"leading newline", "\ntest", false},
		{"trailing newline", "test\n", false},
		{"non-ascii", "tëst", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			test.Equal(t, tt.valid, IsValidTopicName(tt.input))
		})
	}
}

// IsValidChannelName currently shares an implementation with IsValidTopicName.
// These cases pin that equivalence so a future divergence is deliberate.
func TestIsValidChannelName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		valid bool
	}{
		{"simple", "test", true},
		{"ephemeral", "test#ephemeral", true},
		{"max length", strings.Repeat("a", 64), true},
		{"empty", "", false},
		{"too long", strings.Repeat("a", 65), false},
		{"space", "test channel", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			test.Equal(t, tt.valid, IsValidChannelName(tt.input))
		})
	}
}

func TestIsValidNameBoundaryLengths(t *testing.T) {
	// the length check is applied to the whole name, including any
	// "#ephemeral" suffix
	test.Equal(t, true, IsValidTopicName(strings.Repeat("a", 54)+"#ephemeral"))
	test.Equal(t, false, IsValidTopicName(strings.Repeat("a", 55)+"#ephemeral"))
}
