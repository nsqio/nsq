package nsqd

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/test"
)

// TestDurableTopicDeleteSubRace is a regression test for a race between a
// real SUB (the exact TCP wire sequence a client sends) and a concurrent
// "POST /topic/delete" admin request on a *durable* topic.
//
// Before the fix, protocol_v2.go's post-AddClient safety check
// ((channel.ephemeral && channel.Exiting()) || (topic.ephemeral &&
// topic.Exiting())) only applied to ephemeral topics/channels (added by
// #891 for #883, scoped to the ephemeral case only). For a durable topic
// deleted concurrently via the admin HTTP API, that check never fired, so
// SUB returned OK while the client was left attached to a channel whose
// messagePump had already exited -- a silent, permanent zombie
// subscription with no error ever surfaced to the client.
//
// This test races the two paths for real, over the real wire protocol, and
// asserts the invariant that must hold regardless of how the race resolves:
// every SUB that returns OK must correspond to a channel that actually
// receives subsequently published messages. A zombie subscription (SUB ok,
// message never delivered) is the bug.
func TestDurableTopicDeleteSubRace(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, httpAddr, nsqdInst := mustStartNSQD(opts)
	defer nsqdInst.Exit()

	const topicName = "race_durable_sub_delete_topic"
	const channelName = "ch"

	deleteURL := fmt.Sprintf("http://%s/topic/delete?topic=%s", httpAddr.String(), topicName)
	pubURLFmt := "http://%s/pub?topic=%s"

	const attempts = 50
	zombies := 0
	correctlyClosed := 0

	for i := 0; i < attempts; i++ {
		topic := nsqdInst.GetTopic(topicName)
		topic.GetChannel(channelName)
		time.Sleep(2 * time.Millisecond) // let messagePump start

		conn, err := mustConnectNSQD(tcpAddr)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		identify(t, conn, map[string]interface{}{}, frameTypeResponse)

		var wg sync.WaitGroup
		wg.Add(2)

		var subErr error
		var subFrame int32

		go func() {
			defer wg.Done()
			_, werr := nsq.Subscribe(topicName, channelName).WriteTo(conn)
			if werr != nil {
				subErr = werr
				return
			}
			resp, rerr := nsq.ReadResponse(conn)
			if rerr != nil {
				subErr = rerr
				return
			}
			ft, _, uerr := nsq.UnpackResponse(resp)
			if uerr != nil {
				subErr = uerr
				return
			}
			subFrame = ft
		}()

		go func() {
			defer wg.Done()
			req, _ := http.NewRequest("POST", deleteURL, nil)
			resp, derr := http.DefaultClient.Do(req)
			if derr == nil {
				resp.Body.Close()
			}
		}()

		wg.Wait()

		if subErr != nil || subFrame != frameTypeResponse {
			// SUB was correctly rejected (or the connection was torn down) --
			// not the bug. Recreate the topic for the next attempt.
			_ = conn.Close()
			_ = nsqdInst.DeleteExistingTopic(topicName)
			continue
		}

		// SUB reported OK. Publish a fresh, uniquely-tagged message to the
		// same topic name and check whether this subscriber ever gets it.
		_, _ = nsq.Ready(1).WriteTo(conn)

		payload := []byte(fmt.Sprintf("probe-%d", i))
		pubResp, perr := http.Post(fmt.Sprintf(pubURLFmt, httpAddr.String(), topicName),
			"application/octet-stream", bytes.NewReader(payload))
		if perr == nil {
			pubResp.Body.Close()
		}

		_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		resp, rerr := nsq.ReadResponse(conn)
		got := false
		if rerr == nil {
			ft, data, uerr := nsq.UnpackResponse(resp)
			if uerr == nil && ft == frameTypeMessage && bytes.Contains(data, payload) {
				got = true
			}
		}

		if !got {
			// A connection error (closed/reset/aborted) means the server
			// detected the dying topic/channel and tore the connection
			// down -- a legitimate, client-detectable failure, not the bug.
			// The bug is specifically a read that times out on a
			// connection the server never closed: SUB said OK, nothing was
			// ever delivered, and nothing ever told the client why.
			var netErr net.Error
			isTimeout := false
			if ne, ok := rerr.(net.Error); ok {
				netErr = ne
				isTimeout = netErr.Timeout()
			}
			if rerr != nil && !isTimeout {
				correctlyClosed++
			} else {
				zombies++
				t.Logf("attempt %d: SILENT ZOMBIE -- SUB returned OK, message never delivered, connection never closed (readErr=%v)", i, rerr)
			}
		}

		_ = conn.Close()
		_ = nsqdInst.DeleteExistingTopic(topicName)
	}

	t.Logf("attempts=%d silentZombies=%d correctlyClosedByServer=%d", attempts, zombies, correctlyClosed)
	if zombies > 0 {
		t.Fatalf("%d/%d attempts produced a silent zombie subscription (SUB ok, message never delivered, connection never closed) -- durable topic delete raced a concurrent SUB", zombies, attempts)
	}
}
