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

// TestDurableTopicDeleteSubRace races a real SUB (the exact TCP wire
// sequence a client sends) against a concurrent "POST /topic/delete" admin
// request on a *durable* topic, and classifies every possible outcome:
//
//   - cleanReject:    SUB got a protocol-level E_SUB_FAILED response. This is
//     the *good* outcome for a race loss -- a clear, documented rejection.
//   - healthy:        SUB succeeded and a subsequently published probe
//     message was delivered. The race was won cleanly, no bug.
//   - rawConnAbort:   SUB either failed with a raw network error (not a
//     clean protocol response), or SUB reported OK but the connection was
//     then force-closed before the probe message arrived. Not silent -- the
//     client gets a detectable failure -- but it's a confusing raw TCP
//     reset instead of a clean rejection.
//   - silentZombie:   SUB reported OK, the probe message never arrived, and
//     the read simply timed out with no indication the connection was ever
//     closed. This is the one truly severe outcome: permanent, undetectable
//     message loss. This must never happen, before or after any fix.
//
// Before the fix in this same commit, protocol_v2.go's post-AddClient
// safety check in SUB ((channel.ephemeral && channel.Exiting()) ||
// (topic.ephemeral && topic.Exiting())) only applied to ephemeral
// topics/channels (added by #891 for #883, scoped to the ephemeral case
// only), so a durable topic/channel being deleted could never produce a
// cleanReject -- every race loss surfaced as a rawConnAbort. The fix widens
// that check so durable topics get the same cleanReject path ephemeral ones
// already had; this test reports counts for all four categories rather than
// asserting an exact split, since the underlying race is inherently
// timing-sensitive (see PR description for measured counts on this
// environment).
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
	var healthy, cleanReject, rawConnAbort, silentZombie int

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
		var subFrame int32 = -1

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

		if subFrame == frameTypeError {
			// A clean, documented rejection (e.g. E_SUB_FAILED). Good outcome.
			cleanReject++
			_ = conn.Close()
			_ = nsqdInst.DeleteExistingTopic(topicName)
			continue
		}

		if subErr != nil || subFrame != frameTypeResponse {
			// SUB itself failed at the raw connection level (not via a
			// clean protocol response) -- the connection was torn down
			// mid-handshake.
			rawConnAbort++
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

		if got {
			healthy++
		} else {
			// Distinguish a detectable connection failure (not the bug)
			// from a true silent timeout on a connection the server never
			// closed (the bug).
			isTimeout := false
			if ne, ok := rerr.(net.Error); ok {
				isTimeout = ne.Timeout()
			}
			if rerr != nil && !isTimeout {
				rawConnAbort++
			} else {
				silentZombie++
				t.Logf("attempt %d: SILENT ZOMBIE -- SUB returned OK, message never delivered, connection never closed (readErr=%v)", i, rerr)
			}
		}

		_ = conn.Close()
		_ = nsqdInst.DeleteExistingTopic(topicName)
	}

	t.Logf("attempts=%d healthy=%d cleanReject=%d rawConnAbort=%d silentZombie=%d",
		attempts, healthy, cleanReject, rawConnAbort, silentZombie)

	if silentZombie > 0 {
		t.Fatalf("%d/%d attempts produced a silent zombie subscription (SUB ok, message never delivered, connection never closed) -- durable topic delete raced a concurrent SUB", silentZombie, attempts)
	}
}
