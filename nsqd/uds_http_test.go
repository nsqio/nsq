package nsqd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/test"
)

func skipIfNoUnixSockets(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix sockets not supported on windows")
	}
}

// startNSQDWithUDS configures an NSQD with both a TCP HTTP listener (on a random
// port) and an additional Unix-domain-socket HTTP listener. Returns the TCP HTTP
// address and the UDS path. The caller is responsible for nsqd.Exit() and
// removing opts.DataPath.
func startNSQDWithUDS(t *testing.T, opts *Options, perm string) (httpAddr net.Addr, udsPath string, nsqd *NSQD) {
	t.Helper()
	if opts.UnixSocketPath == "" {
		opts.UnixSocketPath = filepath.Join(t.TempDir(), "nsqd.sock")
	}
	udsPath = opts.UnixSocketPath
	opts.UnixSocketPermission = perm
	_, httpAddr, nsqd = mustStartNSQD(opts)
	// wait briefly for Main() to actually start serving on the UDS
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(udsPath); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return httpAddr, udsPath, nsqd
}

func udsHTTPClient(sockPath string) *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sockPath)
			},
		},
	}
}

// (1) UDS-HTTP und TCP-HTTP koexistieren und teilen sich denselben NSQD-State.
func TestUDSAndTCPHTTPCoexist(t *testing.T) {
	skipIfNoUnixSockets(t)

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	httpAddr, udsPath, nsqd := startNSQDWithUDS(t, opts, "")
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_uds_tcp_coexist" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	// publish via TCP HTTP
	tcpURL := fmt.Sprintf("http://%s/pub?topic=%s", httpAddr, topicName)
	resp, err := http.Post(tcpURL, "application/octet-stream", bytes.NewBufferString("from-tcp"))
	test.Nil(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, 200, resp.StatusCode)
	test.Equal(t, "OK", string(body))

	// publish via UDS HTTP
	udsClient := udsHTTPClient(udsPath)
	udsURL := fmt.Sprintf("http://unix/pub?topic=%s", topicName)
	resp, err = udsClient.Post(udsURL, "application/octet-stream", bytes.NewBufferString("from-uds"))
	test.Nil(t, err)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, 200, resp.StatusCode)
	test.Equal(t, "OK", string(body))

	time.Sleep(25 * time.Millisecond)
	test.Equal(t, int64(2), topic.Depth())
}

// (2) Standard-HTTP-Endpunkte (/ping, /info, /stats) sind über UDS erreichbar.
func TestUDSServesHTTPEndpoints(t *testing.T) {
	skipIfNoUnixSockets(t)

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, udsPath, nsqd := startNSQDWithUDS(t, opts, "")
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	client := udsHTTPClient(udsPath)

	for _, endpoint := range []string{"/ping", "/info", "/stats"} {
		resp, err := client.Get("http://unix" + endpoint)
		test.Nil(t, err)
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s returned %d", endpoint, resp.StatusCode)
		}
	}
}

// (3) Explizit gesetzte Permission wird angewandt.
func TestUDSPermissionApplied(t *testing.T) {
	skipIfNoUnixSockets(t)

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, udsPath, nsqd := startNSQDWithUDS(t, opts, "0600")
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	info, err := os.Stat(udsPath)
	test.Nil(t, err)
	test.Equal(t, os.FileMode(0600), info.Mode().Perm())
}

// (4) Default-Permission (leer): kein Chmod, Modus ergibt sich aus umask.
func TestUDSPermissionDefault(t *testing.T) {
	skipIfNoUnixSockets(t)

	oldMask := syscall.Umask(0022)
	defer syscall.Umask(oldMask)

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, udsPath, nsqd := startNSQDWithUDS(t, opts, "")
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	info, err := os.Stat(udsPath)
	test.Nil(t, err)
	// Mit umask 0022 darf das gid/other-write-Bit nicht gesetzt sein.
	if info.Mode().Perm()&0022 != 0 {
		t.Fatalf("expected umask 0022 to clear group/other write bits, got %o", info.Mode().Perm())
	}
}

// (5) Ungültiger Pfad → New(opts) liefert Fehler statt Panic.
func TestUDSListenFailureWhenPathInvalid(t *testing.T) {
	skipIfNoUnixSockets(t)

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	opts.HTTPSAddress = "127.0.0.1:0"
	tmp, _ := os.MkdirTemp("", "nsq-test-")
	defer os.RemoveAll(tmp)
	opts.DataPath = tmp
	opts.UnixSocketPath = "/this/dir/does/not/exist/nsqd.sock"

	n, err := New(opts)
	if err == nil {
		n.Exit()
		t.Fatal("expected error for unreachable UDS path")
	}
}

// (6) Ungültige Permission (kein Octal) → New(opts) liefert Fehler.
func TestUDSInvalidPermissionRejected(t *testing.T) {
	skipIfNoUnixSockets(t)

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	opts.HTTPSAddress = "127.0.0.1:0"
	tmp, _ := os.MkdirTemp("", "nsq-test-")
	defer os.RemoveAll(tmp)
	opts.DataPath = tmp
	opts.UnixSocketPath = filepath.Join(t.TempDir(), "nsqd.sock")
	opts.UnixSocketPermission = "garbage"

	n, err := New(opts)
	if err == nil {
		n.Exit()
		t.Fatal("expected error for invalid permission string")
	}
}

// (7) Socket-Datei wird beim sauberen Exit entfernt.
func TestUDSCleanupOnExit(t *testing.T) {
	skipIfNoUnixSockets(t)

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, udsPath, nsqd := startNSQDWithUDS(t, opts, "")
	defer os.RemoveAll(opts.DataPath)

	_, err := os.Stat(udsPath)
	test.Nil(t, err)

	nsqd.Exit()

	if _, err := os.Stat(udsPath); !os.IsNotExist(err) {
		t.Fatalf("expected socket file to be removed after Exit, stat err=%v", err)
	}
}

// (8a) Existierende Datei am Socket-Pfad, die KEIN Socket ist, wird abgelehnt
// (Schutz davor, versehentlich eine reguläre Datei zu löschen).
func TestUDSExistingRegularFileRejected(t *testing.T) {
	skipIfNoUnixSockets(t)

	regular := filepath.Join(t.TempDir(), "not-a-socket")
	f, err := os.Create(regular)
	test.Nil(t, err)
	f.Close()

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	opts.HTTPSAddress = "127.0.0.1:0"
	tmp, _ := os.MkdirTemp("", "nsq-test-")
	defer os.RemoveAll(tmp)
	opts.DataPath = tmp
	opts.UnixSocketPath = regular

	n, err := New(opts)
	if err == nil {
		n.Exit()
		t.Fatal("expected error when path exists and is not a socket")
	}
	// Die reguläre Datei darf nicht gelöscht worden sein.
	if _, statErr := os.Stat(regular); statErr != nil {
		t.Fatalf("regular file must not be removed, stat err=%v", statErr)
	}
}

// (8b) Verwaister Socket aus einem vorigen Crash wird automatisch entfernt
// und Listen gelingt.
func TestUDSStaleSocketAutoRemoved(t *testing.T) {
	skipIfNoUnixSockets(t)

	udsPath := filepath.Join(t.TempDir(), "stale.sock")

	// Simuliere verwaisten Socket: kurz binden, Listener leaken (kein Close),
	// dann den File-Descriptor manuell schließen, damit nur das Inode bleibt.
	stale, err := net.Listen("unix", udsPath)
	test.Nil(t, err)
	if ul, ok := stale.(*net.UnixListener); ok {
		ul.SetUnlinkOnClose(false)
	}
	stale.Close()
	info, err := os.Stat(udsPath)
	test.Nil(t, err)
	if info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("test setup: expected socket inode at %s", udsPath)
	}

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.UnixSocketPath = udsPath
	_, gotPath, nsqd := startNSQDWithUDS(t, opts, "")
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	test.Equal(t, udsPath, gotPath)
	if _, err := os.Stat(udsPath); err != nil {
		t.Fatalf("expected fresh socket at %s after auto-remove, err=%v", udsPath, err)
	}
}

// (9) TLSRequired beeinflusst den UDS-HTTP-Server nicht (lokaler Trust).
func TestUDSIgnoresTLSRequired(t *testing.T) {
	skipIfNoUnixSockets(t)

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRequired = TLSRequired
	// vermeidet, dass der HTTPS-Listener auf dem TLS-Default-Port versucht zu binden
	opts.HTTPSAddress = "127.0.0.1:0"

	_, udsPath, nsqd := startNSQDWithUDS(t, opts, "")
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_uds_tls_required" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopic(topicName)

	client := udsHTTPClient(udsPath)
	resp, err := client.Post("http://unix/pub?topic="+topicName, "application/octet-stream", bytes.NewBufferString("payload"))
	test.Nil(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, 200, resp.StatusCode)
	test.Equal(t, "OK", string(body))
}

// (10) Defaults: Feature ist opt-in.
func TestUDSOptionDefaults(t *testing.T) {
	opts := NewOptions()
	test.Equal(t, "", opts.UnixSocketPath)
	test.Equal(t, "", opts.UnixSocketPermission)
}

// (11) RealUDSAddr liefert die tatsächlich gebundene Adresse bzw. einen Zero-Value.
func TestRealUDSAddr(t *testing.T) {
	skipIfNoUnixSockets(t)

	// ohne UDS: Zero-Value, kein Panic
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	addr := nsqd.RealUDSAddr()
	test.Equal(t, "", addr.String())
	nsqd.Exit()

	// mit UDS: Adresse == konfigurierter Pfad
	opts2 := NewOptions()
	opts2.Logger = test.NewTestLogger(t)
	_, udsPath, nsqd2 := startNSQDWithUDS(t, opts2, "")
	defer os.RemoveAll(opts2.DataPath)
	defer nsqd2.Exit()
	test.Equal(t, udsPath, nsqd2.RealUDSAddr().String())
}
