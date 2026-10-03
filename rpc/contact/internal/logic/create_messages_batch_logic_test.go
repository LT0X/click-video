package logic

import (
	"context"
	"regexp"
	"testing"

	"douyin/rpc/contact/contact"
	"douyin/rpc/contact/internal/svc"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func TestCreateMessagesBatchUsesEventIDForIdempotentInsert(t *testing.T) {
	db, mock, cleanup := newChatLogicDB(t)
	defer cleanup()
	sqlPattern := regexp.QuoteMeta("INSERT INTO `message` (`event_id`") + ".*ON DUPLICATE KEY UPDATE"
	mock.ExpectExec(sqlPattern).WillReturnResult(sqlmock.NewResult(0, 2))

	logic := NewCreateMessagesBatchLogic(context.Background(), &svc.ServiceContext{DBList: &svc.DBList{Mysql: db}})
	result, err := logic.CreateMessagesBatch(&contact.CreateMessagesBatchRequest{Messages: []*contact.ChatMessageInput{
		{EventID: "event-a", UserID: 1, ToUserID: 2, Content: "one", CreateTime: 1700000000000},
		{EventID: "event-b", UserID: 2, ToUserID: 1, Content: "two", CreateTime: 1700000000001},
	}})
	if err != nil {
		t.Fatalf("CreateMessagesBatch() error = %v", err)
	}
	if result.GetReply() != 2 {
		t.Fatalf("Reply = %d, want 2", result.GetReply())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("SQL expectations: %v", err)
	}
}

func TestCreateMessagesBatchRejectsInvalidOrOversizedBatch(t *testing.T) {
	db, mock, cleanup := newChatLogicDB(t)
	defer cleanup()
	logic := NewCreateMessagesBatchLogic(context.Background(), &svc.ServiceContext{DBList: &svc.DBList{Mysql: db}})
	for _, req := range []*contact.CreateMessagesBatchRequest{
		{},
		{Messages: []*contact.ChatMessageInput{{EventID: "", UserID: 1, ToUserID: 2, Content: "x", CreateTime: 1}}},
		{Messages: make([]*contact.ChatMessageInput, 51)},
	} {
		if _, err := logic.CreateMessagesBatch(req); err == nil {
			t.Fatalf("CreateMessagesBatch(%#v) error = nil", req)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected SQL calls: %v", err)
	}
}

func newChatLogicDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	conn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	db, err := gorm.Open(mysql.New(mysql.Config{Conn: conn, SkipInitializeWithVersion: true}), &gorm.Config{
		SkipDefaultTransaction: true,
		NamingStrategy:         schema.NamingStrategy{SingularTable: true},
	})
	if err != nil {
		t.Fatalf("open gorm db: %v", err)
	}
	return db, mock, func() { _ = conn.Close() }
}
