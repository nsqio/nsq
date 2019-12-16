package nsqd

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack"
)

type messageWithAbsTsDelete struct {
	ID        MessageID `msgpack:"message_id"`
	Body      []byte    `msgpack:"-"`
	Timestamp int64     `msgpack:"timestamp"`
	Attempts  uint16    `msgpack:"attempts"`

	// for in-flight handling
	deliveryTS time.Time
	clientID   int64
	pri        int64
	index      int
}

func TestMessageWithAbsTsDeleteAfterMarshaling(t *testing.T) {
	now := time.Now()

	cases := []struct {
		Desc string
		Msg  *Message
	}{
		{
			Desc: "nonzero AbsTs",
			Msg: &Message{
				ID:        MessageID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
				Body:      []byte("abc"),
				Timestamp: now.UnixNano(),
				Attempts:  1,
				AbsTs:     now.Add(time.Second).UnixNano(),
			},
		},
		{
			Desc: "zero AbsTs",
			Msg: &Message{
				ID:        MessageID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
				Body:      []byte("abc"),
				Timestamp: now.UnixNano(),
				Attempts:  1,
			},
		},
	}

	for _, ut := range cases {
		meta, err := msgpack.Marshal(ut.Msg)
		if err != nil {
			t.Fatalf("Desc: %q. Msgpack marshal error: %v", ut.Desc, err)
		}

		var unMarshaledMsgWithAbsTsDelete messageWithAbsTsDelete
		err = msgpack.Unmarshal(meta, &unMarshaledMsgWithAbsTsDelete)
		if err != nil {
			t.Fatalf("Desc: %q. Msgpack unmarshal error: %v", ut.Desc, err)
		}

		originalMsg := *ut.Msg
		originalMsg.Body = nil
		if !(reflect.DeepEqual(unMarshaledMsgWithAbsTsDelete.ID, originalMsg.ID) &&
			reflect.DeepEqual(unMarshaledMsgWithAbsTsDelete.Body, originalMsg.Body) &&
			reflect.DeepEqual(unMarshaledMsgWithAbsTsDelete.Timestamp, originalMsg.Timestamp) &&
			reflect.DeepEqual(unMarshaledMsgWithAbsTsDelete.Attempts, originalMsg.Attempts)) {
			t.Fatalf("Desc: %q. MarshaledMessage unmarshal does not equal to original msg. Unmarshaled msg: %#v, orignal msg: %#v",
				ut.Desc, unMarshaledMsgWithAbsTsDelete, originalMsg)
		}
	}
}

type messageWithAddedFieldAdd struct {
	ID         MessageID `msgpack:"message_id"`
	Body       []byte    `msgpack:"-"`
	Timestamp  int64     `msgpack:"timestamp"`
	Attempts   uint16    `msgpack:"attempts"`
	AbsTs      int64     `msgpack:"abs_ts"`
	AddedField []byte    `msgpack:"added_field"`

	// for in-flight handling
	deliveryTS time.Time
	clientID   int64
	pri        int64
	index      int
}

func TestMessageWithAddedFieldAddMarshaling(t *testing.T) {
	now := time.Now()

	cases := []struct {
		Desc string
		Msg  *Message
	}{
		{
			Desc: "no AddedField",
			Msg: &Message{
				ID:        MessageID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
				Body:      []byte("abc"),
				Timestamp: now.UnixNano(),
				Attempts:  1,
				AbsTs:     now.Add(time.Second).UnixNano(),
			},
		},
	}

	for _, ut := range cases {
		meta, err := msgpack.Marshal(ut.Msg)
		if err != nil {
			t.Fatalf("Desc: %q. Msgpack marshal error: %v", ut.Desc, err)
		}

		var unMarshaledMsgWithAddedFieldAdd messageWithAddedFieldAdd
		err = msgpack.Unmarshal(meta, &unMarshaledMsgWithAddedFieldAdd)
		if err != nil {
			t.Fatalf("Desc: %q. Msgpack unmarshal error: %v", ut.Desc, err)
		}

		originalMsg := *ut.Msg
		originalMsg.Body = nil
		if !(reflect.DeepEqual(unMarshaledMsgWithAddedFieldAdd.ID, originalMsg.ID) &&
			reflect.DeepEqual(unMarshaledMsgWithAddedFieldAdd.Body, originalMsg.Body) &&
			reflect.DeepEqual(unMarshaledMsgWithAddedFieldAdd.Timestamp, originalMsg.Timestamp) &&
			reflect.DeepEqual(unMarshaledMsgWithAddedFieldAdd.Attempts, originalMsg.Attempts) &&
			reflect.DeepEqual(unMarshaledMsgWithAddedFieldAdd.AbsTs, originalMsg.AbsTs) &&
			unMarshaledMsgWithAddedFieldAdd.AddedField == nil) {
			t.Fatalf("Desc: %q. MarshaledMessage unmarshal does not equal to original msg. Unmarshaled msg: %#v, orignal msg: %#v",
				ut.Desc, unMarshaledMsgWithAddedFieldAdd, originalMsg)
		}
	}
}

func TestMessageMarshalAndUnmarshal(t *testing.T) {
	now := time.Now()

	cases := []struct {
		Desc string
		Msg  *Message
	}{
		{
			Desc: "zero Attempts",
			Msg: &Message{
				ID:        MessageID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
				Body:      []byte("abc"),
				Timestamp: now.UnixNano(),
				AbsTs:     now.Add(time.Second).UnixNano(),
			},
		},
		{
			Desc: "zero AbsTs, normal msg",
			Msg: &Message{
				ID:        MessageID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
				Body:      []byte("abc"),
				Timestamp: now.UnixNano(),
				Attempts:  1,
			},
		},
		{
			Desc: "zero AbsTs and zero Attempts",
			Msg: &Message{
				ID:        MessageID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
				Body:      []byte("abc"),
				Timestamp: now.UnixNano(),
			},
		},
		{
			Desc: "defer msg",
			Msg: &Message{
				ID:        MessageID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
				Body:      []byte("abc"),
				Timestamp: now.UnixNano(),
				Attempts:  1,
				AbsTs:     now.Add(time.Second).UnixNano(),
			},
		},
	}

	for _, ut := range cases {
		meta, err := msgpack.Marshal(ut.Msg)
		if err != nil {
			t.Fatalf("Desc: %q. Msgpack marshal error: %v", ut.Desc, err)
		}

		var unMarshaledMsg Message
		err = msgpack.Unmarshal(meta, &unMarshaledMsg)
		if err != nil {
			t.Fatalf("Desc: %q. Msgpack unmarshal error: %v", ut.Desc, err)
		}

		originalMsg := *ut.Msg
		originalMsg.Body = nil
		if !reflect.DeepEqual(unMarshaledMsg, originalMsg) {
			t.Fatalf("Desc: %q. MarshaledMessage unmarshal does not equal to original msg. Unmarshaled msg: %#v, orignal msg: %#v",
				ut.Desc, unMarshaledMsg, originalMsg)
		}
	}
}

func BenchmarkMessageJsonMarshalAndUnmarshal(b *testing.B) {
	msg := Message{
		ID:        MessageID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		Timestamp: time.Now().UnixNano(),
		Attempts:  1,
		AbsTs:     time.Now().Add(time.Second).UnixNano(),
	}

	content, err := json.Marshal(msg)
	if err != nil {
		b.Fatalf("Json marshal error: %v", err)
	}

	b.SetBytes(int64(len(content)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		content, err := json.Marshal(msg)
		if err != nil {
			b.Fatalf("Json marshal error: %v", err)
		}

		var tmp Message
		err = json.Unmarshal(content, &tmp)
		if err != nil {
			b.Fatalf("Json unmarshal error: %v", err)
		}
	}
}

func BenchmarkMessageMsgPackMarshalAndUnmarshal(b *testing.B) {
	msg := Message{
		ID:        MessageID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		Timestamp: time.Now().UnixNano(),
		Attempts:  1,
		AbsTs:     time.Now().Add(time.Second).UnixNano(),
	}

	content, err := msgpack.Marshal(&msg)
	if err != nil {
		b.Fatalf("Msgpack marshal error: %v", err)
	}

	b.SetBytes(int64(len(content)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		content, err := msgpack.Marshal(&msg)
		if err != nil {
			b.Fatalf("Msgpack marshal error: %v", err)
		}

		var tmp Message
		err = msgpack.Unmarshal(content, &tmp)
		if err != nil {
			b.Fatalf("Msgpack unmarshal error: %v", err)
		}
	}
}

func MarshalMessageInNewFormat(t *testing.T, m *Message) []byte {
	var ret []byte
	ret = append(ret, msgMagic...)
	ret = append(ret, metaKey...)

	meta, err := msgpack.Marshal(&m)
	if err != nil {
		t.Fatalf("")
	}
	var metaLen [2]byte
	binary.BigEndian.PutUint16(metaLen[:], uint16(len(meta)))
	ret = append(ret, metaLen[:]...)
	ret = append(ret, meta...)

	ret = append(ret, bodyKey...)
	var bodyLen [8]byte
	binary.BigEndian.PutUint64(bodyLen[:], uint64(len(m.Body)))
	ret = append(ret, bodyLen[:]...)
	ret = append(ret, m.Body...)

	return ret
}

func MarshalMessageInOldFormat(t *testing.T, m *Message) []byte {
	bf := bytes.NewBuffer(nil)

	_, err := m.WriteTo(bf)
	if err != nil {
		t.Fatalf("Marshal message in old format error: %v", err)
	}

	return bf.Bytes()
}

func TestMessage_WriteToBackend(t *testing.T) {
	now := time.Now()

	cases := []struct {
		Desc       string
		MaxMetaLen uint16
		Msg        *Message
		WantedErr  bool
	}{
		{
			Desc:       "normal message",
			MaxMetaLen: math.MaxUint16,
			Msg: &Message{
				ID:        MessageID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
				Timestamp: time.Now().UnixNano(),
				Attempts:  1,
				AbsTs:     now.Add(time.Second).UnixNano(),
				Body:      []byte("abc"),
			},
			WantedErr: false,
		},
		{
			Desc:       "empty body message",
			MaxMetaLen: math.MaxUint16,
			Msg: &Message{
				ID:        MessageID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
				Timestamp: time.Now().UnixNano(),
				Attempts:  1,
				AbsTs:     now.Add(time.Second).UnixNano(),
				Body:      []byte(""),
			},
			WantedErr: false,
		},
		{
			Desc:       "Exceed max meta length",
			MaxMetaLen: 1,
			Msg: &Message{
				ID:        MessageID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
				Timestamp: time.Now().UnixNano(),
				Attempts:  1,
				AbsTs:     now.Add(time.Second).UnixNano(),
				Body:      []byte("abc"),
			},
			WantedErr: true,
		},
	}

	for _, ut := range cases {
		oldMaxMetaLen := maxMetaLen

		maxMetaLen = ut.MaxMetaLen

		bf := &bytes.Buffer{}
		writtenLen, err := ut.Msg.WriteToBackend(bf)
		if (err != nil && !ut.WantedErr) || (err == nil && ut.WantedErr) {
			t.Fatalf("Desc: %q. WriteToBackend for message got error: %v, wanted error: %v", ut.Desc, err, ut.WantedErr)
		}

		wanted := MarshalMessageInNewFormat(t, ut.Msg)
		if writtenLen != int64(len(wanted)) && bytes.EqualFold(bf.Bytes(), wanted) {
			t.Fatalf("Desc: %q. WriteToBackend output does not match wanted bytes. Got: %v, wanted: %v", ut.Desc, bf.Bytes(), wanted)
		}

		maxMetaLen = oldMaxMetaLen
	}
}

func TestDecodeMessage(t *testing.T) {
	msg := &Message{
		ID:        MessageID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		Timestamp: time.Now().UnixNano(),
		Attempts:  1,
		Body:      []byte("abc"),
	}

	cases := []struct {
		Desc             string
		MarshaledMessage []byte
	}{
		{
			Desc:             "old message format",
			MarshaledMessage: MarshalMessageInOldFormat(t, msg),
		},
		{
			Desc:             "new message format",
			MarshaledMessage: MarshalMessageInNewFormat(t, msg),
		},
	}

	for _, ut := range cases {
		gotMsg, err := decodeMessage(ut.MarshaledMessage)
		if err != nil {
			t.Fatalf("Desc: %q. Decode message error: %v", ut.Desc, err)
		}

		if !reflect.DeepEqual(gotMsg, msg) {
			t.Fatalf("Desc: %q. Decoded message does not equal to wanted message. Got: %#v, wanted: %#v", ut.Desc, *gotMsg, *msg)
		}
	}
}
