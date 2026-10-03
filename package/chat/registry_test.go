package chat

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeSocket struct {
	mu       sync.Mutex
	written  []interface{}
	writeErr error
	closed   bool
	order    *[]string
}

func (s *fakeSocket) WriteJSON(v interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writeErr != nil {
		return s.writeErr
	}
	if s.order != nil {
		*s.order = append(*s.order, "deliver")
	}
	s.written = append(s.written, v)
	return nil
}

func (s *fakeSocket) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func TestRegistryDeliversToCurrentConnection(t *testing.T) {
	registry := NewRegistry()
	oldSocket, newSocket := &fakeSocket{}, &fakeSocket{}
	oldClient, newClient := NewClient(oldSocket), NewClient(newSocket)
	registry.Register(17, oldClient)
	registry.Register(17, newClient)
	registry.Remove(17, oldClient)

	want := Message{EventID: "event-1", FromUserID: 4, ToUserID: 17, Content: "你好", CreateTime: 42}
	if err := registry.Deliver(17, want); err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	if len(oldSocket.written) != 0 {
		t.Fatalf("old connection received a message after replacement")
	}
	if len(newSocket.written) != 1 {
		t.Fatalf("current connection received %d messages, want 1", len(newSocket.written))
	}
	delivered, ok := newSocket.written[0].(struct {
		Type string `json:"type"`
		Message
	})
	if !ok || delivered.Type != "message" || delivered.Message != want {
		t.Fatalf("current connection message = %#v, want message envelope for %#v", newSocket.written[0], want)
	}
}

func TestClientSerializesConcurrentWrites(t *testing.T) {
	socket := &overlapSocket{}
	client := NewClient(socket)
	const writes = 32
	var wg sync.WaitGroup
	for i := 0; i < writes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := client.WriteJSON(i); err != nil {
				t.Errorf("WriteJSON() error = %v", err)
			}
		}(i)
	}
	wg.Wait()
	if socket.overlap.Load() != 0 {
		t.Fatal("concurrent writes reached the underlying WebSocket")
	}
}

type overlapSocket struct{ active, writes, overlap atomic.Int32 }

func (s *overlapSocket) WriteJSON(interface{}) error {
	if s.active.Add(1) > 1 {
		s.overlap.Store(1)
	}
	time.Sleep(time.Millisecond)
	s.writes.Add(1)
	s.active.Add(-1)
	return nil
}

func (s *overlapSocket) Close() error { return nil }

func TestRegistryReturnsDeliveryError(t *testing.T) {
	registry := NewRegistry()
	socket := &fakeSocket{writeErr: errors.New("socket closed")}
	registry.Register(9, NewClient(socket))
	if err := registry.Deliver(9, Message{EventID: "event-2"}); err == nil {
		t.Fatal("Deliver() error = nil, want socket error")
	}
}
