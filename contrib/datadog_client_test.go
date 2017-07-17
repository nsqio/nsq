package contrib

import (
	"testing"
	"github.com/nsqio/nsq/internal/test"
)

func TestDDTagsStringNoTags(t *testing.T) {
	test.Equal(
		t,
		(&DataDogTags{}).String(),
		"#",
	)
}

func TestDDTagsStringSingleString(t *testing.T) {
	test.Equal(
		t,
		(&DataDogTags{
			tags: []*DataDogTag{
				{k: "topic_name", v: "test_topic"},
			},
		}).String(),
		"#topic_name:test_topic",
	)
}

func TestDDTagsStringMultipleStrings(t *testing.T) {
	test.Equal(
		t,
		(&DataDogTags{
			tags: []*DataDogTag{
				{k: "topic_name", v: "test_topic"},
				{k: "channel_name", v: "test_channel"},
			},
		}).String(),
		"#topic_name:test_topic,channel_name:test_channel",
	)
}
