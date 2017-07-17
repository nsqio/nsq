package contrib

import (
	"testing"
	"github.com/nsqio/nsq/internal/test"
	"net"
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

func TestDDCSend(t *testing.T) {
	r, w := net.Pipe()
	b := make([]byte, len("nsq.topic.depth:100|t|#"))

	go func() {
		ddc := &DataDogClient{
			conn: w,
			addr: "test",
			prefix: "nsq.",
		}
		testValue := int64(100)
		ddc.send("topic.depth", "%d|t", testValue, &DataDogTags{})
	}()

	r.Read(b)
	test.Equal(t, string(b), "nsq.topic.depth:100|t|#")
}
