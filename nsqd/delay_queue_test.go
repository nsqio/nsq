package nsqd

import (
	//"github.com/absolute8511/nsq/internal/levellogger"
	"fmt"
	"github.com/absolute8511/nsq/internal/test"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"
)

func TestDelayQueuePutChannelDelayed(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-delay-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1

	dq, err := NewDelayQueue("test", 0, tmpDir, opts, nil)
	test.Nil(t, err)
	defer dq.Close()
	cnt := 10
	var end BackendOffset
	for i := 0; i < cnt; i++ {
		msg := NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, dend, err := dq.PutDelayMessage(msg)
		test.Nil(t, err)
		end = dend.Offset()
	}
	synced, err := dq.GetSyncedOffset()
	test.Nil(t, err)
	test.Equal(t, end, synced)
	test.Equal(t, cnt, int(dq.GetCurrentDelayedCnt(ChannelDelayed, "test")))
	_, err = os.Stat(dq.dataPath)
	test.Nil(t, err)
	dq.Delete()
	_, err = os.Stat(dq.dataPath)
	test.Nil(t, err)
	_, err = os.Stat(path.Join(dq.dataPath, getDelayQueueDBName(dq.tname, dq.partition)))
	test.NotNil(t, err)
}

func TestDelayQueueEmptyUntil(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-delay-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1
	SetLogger(opts.Logger)

	dq, err := NewDelayQueue("test", 0, tmpDir, opts, nil)
	test.Nil(t, err)
	defer dq.Close()
	cnt := 10
	var middleTs int64
	for i := 0; i < cnt; i++ {
		msg := NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err := dq.PutDelayMessage(msg)
		test.Nil(t, err)

		msg = NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test2"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err = dq.PutDelayMessage(msg)
		time.Sleep(time.Millisecond * 100)
		if i == cnt/2 {
			middleTs = msg.DelayedTs
		}
	}

	test.Equal(t, cnt, int(dq.GetCurrentDelayedCnt(ChannelDelayed, "test")))
	test.Equal(t, cnt, int(dq.GetCurrentDelayedCnt(ChannelDelayed, "test2")))
	dq.EmptyDelayedUntil(ChannelDelayed, middleTs, "test")

	test.Equal(t, cnt/2-1, int(dq.GetCurrentDelayedCnt(ChannelDelayed, "test")))
	test.Equal(t, cnt, int(dq.GetCurrentDelayedCnt(ChannelDelayed, "test2")))
	dq.EmptyDelayedChannel("test")
	test.Equal(t, 0, int(dq.GetCurrentDelayedCnt(ChannelDelayed, "test")))
	test.Equal(t, cnt, int(dq.GetCurrentDelayedCnt(ChannelDelayed, "test2")))
}

func TestDelayQueuePeekRecent(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-delay-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1

	dq, err := NewDelayQueue("test", 0, tmpDir, opts, nil)
	test.Nil(t, err)
	defer dq.Close()
	cnt := 10
	for i := 0; i < cnt; i++ {
		msg := NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err := dq.PutDelayMessage(msg)
		test.Nil(t, err)

		msg = NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test2"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err = dq.PutDelayMessage(msg)
		time.Sleep(time.Millisecond * 100)
	}

	ret := make([]Message, cnt)
	for {
		n, err := dq.PeekRecentChannelTimeout(ret, "test")
		test.Nil(t, err)
		for _, m := range ret[:n] {
			test.Equal(t, "test", m.DelayedChannel)
			test.Equal(t, true, m.DelayedTs <= time.Now().UnixNano())
		}

		n, err = dq.PeekRecentChannelTimeout(ret, "test2")
		test.Nil(t, err)
		for _, m := range ret[:n] {
			test.Equal(t, "test2", m.DelayedChannel)
			test.Equal(t, true, m.DelayedTs <= time.Now().UnixNano())
		}

		if n >= cnt {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func TestDelayQueueConfirmMsg(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-delay-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1
	SetLogger(opts.Logger)

	dq, err := NewDelayQueue("test", 0, tmpDir, opts, nil)
	test.Nil(t, err)
	defer dq.Close()
	cnt := 10
	for i := 0; i < cnt; i++ {
		msg := NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err := dq.PutDelayMessage(msg)
		test.Nil(t, err)

		msg = NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test2"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err = dq.PutDelayMessage(msg)
		time.Sleep(time.Millisecond * 100)
	}

	ret := make([]Message, cnt)
	for {
		n, err := dq.PeekRecentChannelTimeout(ret, "test")
		test.Nil(t, err)
		for _, m := range ret[:n] {
			test.Equal(t, "test", m.DelayedChannel)
			test.Equal(t, true, m.DelayedTs <= time.Now().UnixNano())
			oldCnt := int(dq.GetCurrentDelayedCnt(ChannelDelayed, "test"))
			m.DelayedOrigID = m.ID
			dq.ConfirmedMessage(&m)
			newCnt := int(dq.GetCurrentDelayedCnt(ChannelDelayed, "test"))
			test.Equal(t, oldCnt-1, newCnt)
			cursorList, cntList, channelCntList := dq.GetOldestConsumedState([]string{"test"})
			for _, v := range cntList {
				test.Equal(t, uint64(0), v)
			}
			test.Equal(t, 1, len(channelCntList))
			test.Equal(t, uint64(newCnt), channelCntList["test"])
			for _, c := range cursorList {
				dt, ts, id, ch, err := decodeDelayedMsgDBKey(c)
				test.Nil(t, err)
				if dt == ChannelDelayed {
					test.Equal(t, "test", ch)
					test.Equal(t, true, ts > m.DelayedTs)
					t.Logf("confirmed: %v, oldest ts: %v\n", m.DelayedTs, ts)
					test.Equal(t, true, ts < m.DelayedTs+int64(time.Millisecond*110))
					test.Equal(t, true, id > m.ID)
				}
			}
		}

		n, err = dq.PeekRecentChannelTimeout(ret, "test2")
		test.Nil(t, err)
		for _, m := range ret[:n] {
			test.Equal(t, "test2", m.DelayedChannel)
			test.Equal(t, true, m.DelayedTs <= time.Now().UnixNano())
			oldCnt := int(dq.GetCurrentDelayedCnt(ChannelDelayed, "test2"))
			m.DelayedOrigID = m.ID
			dq.ConfirmedMessage(&m)
			newCnt := int(dq.GetCurrentDelayedCnt(ChannelDelayed, "test2"))
			test.Equal(t, oldCnt-1, newCnt)

			cursorList, cntList, channelCntList := dq.GetOldestConsumedState([]string{"test2"})
			for _, v := range cntList {
				test.Equal(t, uint64(0), v)
			}
			test.Equal(t, 1, len(channelCntList))
			test.Equal(t, uint64(newCnt), channelCntList["test2"])
			for _, c := range cursorList {
				dt, ts, id, ch, err := decodeDelayedMsgDBKey(c)
				test.Nil(t, err)
				if dt == ChannelDelayed {
					test.Equal(t, "test2", ch)
					test.Equal(t, true, ts > m.DelayedTs)
					test.Equal(t, true, ts < m.DelayedTs+int64(time.Millisecond*110))
					test.Equal(t, true, id > m.ID)
				}
			}
		}

		if dq.GetCurrentDelayedCnt(ChannelDelayed, "test2") <= 0 {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}

}
