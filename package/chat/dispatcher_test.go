package chat

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type fakeQueue struct {
	messages []Message
	err      error
	order    *[]string
}

func (q *fakeQueue) Enqueue(_ context.Context, msg Message) error {
	if q.order != nil {
		*q.order = append(*q.order, "enqueue")
	}
	if q.err != nil {
		return q.err
	}
	q.messages = append(q.messages, msg)
	return nil
}

type fakeRoutes struct {
	address string
	err     error
}

func (r fakeRoutes) Lookup(context.Context, uint64) (string, error) { return r.address, r.err }

type fakeRemote struct {
	address string
	message Message
	called  bool
	order   *[]string
}

func (p *fakeRemote) Push(_ context.Context, address string, msg Message) error {
	if p.order != nil {
		*p.order = append(*p.order, "deliver")
	}
	p.called, p.address, p.message = true, address, msg
	return nil
}

func TestDispatcherQueuesBeforeLocalDelivery(t *testing.T) {
	order := make([]string, 0, 2)
	registry := NewRegistry()
	socket := &fakeSocket{order: &order}
	registry.Register(21, NewClient(socket))
	queue := &fakeQueue{order: &order}
	dispatcher := NewDispatcher("node-a:8014", registry, queue, fakeRoutes{address: "node-a:8014"}, &fakeRemote{})
	msg := Message{EventID: "local-1", FromUserID: 3, ToUserID: 21, Content: "本机", CreateTime: 1}

	if err := dispatcher.Send(context.Background(), msg); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if !reflect.DeepEqual(order, []string{"enqueue", "deliver"}) {
		t.Fatalf("operation order = %#v", order)
	}
	if len(queue.messages) != 1 || len(socket.written) != 1 {
		t.Fatalf("queued=%d local deliveries=%d, want one each", len(queue.messages), len(socket.written))
	}
}

func TestDispatcherRoutesRemoteAndKeepsOfflineMessagesQueued(t *testing.T) {
	msg := Message{EventID: "remote-1", FromUserID: 3, ToUserID: 21, Content: "跨节点"}
	queue := &fakeQueue{}
	remote := &fakeRemote{}
	dispatcher := NewDispatcher("node-a:8014", NewRegistry(), queue, fakeRoutes{address: "node-b:8014"}, remote)
	if err := dispatcher.Send(context.Background(), msg); err != nil {
		t.Fatalf("remote Send() error = %v", err)
	}
	if !remote.called || remote.address != "node-b:8014" || remote.message != msg {
		t.Fatalf("remote delivery = %#v", remote)
	}

	queue = &fakeQueue{}
	remote = &fakeRemote{}
	dispatcher = NewDispatcher("node-a:8014", NewRegistry(), queue, fakeRoutes{}, remote)
	if err := dispatcher.Send(context.Background(), msg); err != nil {
		t.Fatalf("offline Send() error = %v", err)
	}
	if len(queue.messages) != 1 || remote.called {
		t.Fatalf("offline send queued=%d remote=%v, want queued and no RPC", len(queue.messages), remote.called)
	}
}

func TestDispatcherDoesNotRouteWhenEnqueueFails(t *testing.T) {
	remote := &fakeRemote{}
	dispatcher := NewDispatcher("node-a:8014", NewRegistry(), &fakeQueue{err: errors.New("redis unavailable")}, fakeRoutes{address: "node-b:8014"}, remote)
	if err := dispatcher.Send(context.Background(), Message{EventID: "lost-1", ToUserID: 5}); err == nil {
		t.Fatal("Send() error = nil, want queue error")
	}
	if remote.called {
		t.Fatal("remote delivery occurred before durable enqueue")
	}
}
