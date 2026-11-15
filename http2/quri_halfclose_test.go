package http2

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/metacubex/http"
)

func testConnection(t *testing.T, server *Server, handler http.Handler) *ClientConn {
	t.Helper()
	local, remote := net.Pipe()
	done := make(chan struct{})
	go func() { server.ServeConn(remote, &ServeConnOpts{Handler: handler}); close(done) }()
	client, err := new(Transport).NewClientConn(local)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
		if err := remote.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("HTTP/2 server did not stop")
		}
	})
	return client
}

func TestResponseCloseWritePreservesUpload(t *testing.T) {
	upload := make(chan string, 1)
	handlerErr := make(chan error, 1)
	client := testConnection(t, &Server{}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "CONNECT" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		closer, ok := w.(interface{ CloseWrite() error })
		if !ok {
			w.WriteHeader(http.StatusNotImplemented)
			return
		}
		if _, err := io.WriteString(w, "response-before-upload"); err != nil {
			handlerErr <- err
			return
		}
		if err := closer.CloseWrite(); err != nil {
			handlerErr <- err
			return
		}
		if err := closer.CloseWrite(); err != nil {
			handlerErr <- err
			return
		}
		if _, err := w.Write([]byte("too late")); !errors.Is(err, io.ErrClosedPipe) {
			handlerErr <- errors.New("write after CloseWrite did not fail")
			return
		}
		b, err := io.ReadAll(r.Body)
		if err != nil {
			handlerErr <- err
			return
		}
		upload <- string(b)
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer func() {
		if err := writer.Close(); err != nil {
			t.Error(err)
		}
	}()
	req, err := http.NewRequestWithContext(ctx, "CONNECT", "https://target.example:443", reader)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status=%d, want 200 with half-close support", response.StatusCode)
	}
	b, err := io.ReadAll(response.Body)
	if err != nil || string(b) != "response-before-upload" {
		t.Fatalf("response=%q, %v", b, err)
	}
	neighbor, err := http.NewRequestWithContext(ctx, "GET", "https://relay.example/", nil)
	if err != nil {
		t.Fatal(err)
	}
	other, err := client.RoundTrip(neighbor)
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if other.StatusCode != http.StatusNoContent {
		t.Fatalf("neighbor status=%d", other.StatusCode)
	}
	if _, err := io.Copy(writer, strings.NewReader("upload-after-fin")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-upload:
		if got != "upload-after-fin" {
			t.Errorf("upload=%q", got)
		}
	case err := <-handlerErr:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("response FIN stopped request upload")
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExtendedConnectIsPerServer(t *testing.T) {
	for _, enabled := range []bool{true, false, true, false} {
		t.Run(map[bool]string{true: "enabled", false: "disabled"}[enabled], func(t *testing.T) {
			client := testConnection(t, &Server{EnableExtendedConnectProtocol: enabled}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(":protocol") != "connect-udp" {
					t.Error("missing Extended CONNECT protocol")
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, "CONNECT", "https://relay.example/udp", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set(":protocol", "connect-udp")
			resp, err := client.RoundTrip(req)
			if !enabled {
				if err == nil {
					if closeErr := resp.Body.Close(); closeErr != nil {
						t.Error(closeErr)
					}
					t.Fatal("disabled server accepted Extended CONNECT")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := resp.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusNoContent {
				t.Fatalf("status=%d", resp.StatusCode)
			}
		})
	}
}

func TestCloseWriteThenHandlerReturnStopsUpload(t *testing.T) {
	client := testConnection(t, &Server{}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := w.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
			t.Error(err)
		}
	}))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer writer.Close() // Releases the producer after the peer resets the stream.
	req, err := http.NewRequestWithContext(ctx, "CONNECT", "https://target.example:443", reader)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("after handler returned")); err == nil {
		t.Fatal("returned handler left request upload open")
	}
}
