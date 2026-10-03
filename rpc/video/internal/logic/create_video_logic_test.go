package logic

import (
	"context"
	"database/sql"
	"regexp"
	"testing"
	"time"

	"douyin/rpc/video/internal/svc"
	"douyin/rpc/video/video"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func TestCreateVideoIsIdempotentByUploadID(t *testing.T) {
	db, mock, closeDB := newVideoLogicTestDB(t)
	defer closeDB()
	logic := NewCreateVideoLogic(context.Background(), &svc.ServiceContext{DBList: &svc.DBList{Mysql: db}})
	request := &video.CreateVideoRequest{
		UploadID: "9b2e6a61-1ab9-4d22-918d-8c2a2f7bd601",
		VideoID: &video.VideoInfo{
			AuthorID: 7, PlayURL: "http://media/video.mp4", CoverURL: "http://media/cover.jpg",
			Title: "示例标题", Topic: "默认", PublishTime: 1700000000000,
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `video`")).WillReturnResult(sqlmock.NewResult(81, 1))
	mock.ExpectCommit()
	first, err := logic.CreateVideo(request)
	if err != nil || first.VideoID != 81 {
		t.Fatalf("first CreateVideo() = (%+v, %v), want ID 81", first, err)
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `video`")).WillReturnResult(sqlmock.NewResult(0, 0))
	existing := mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `video` WHERE upload_id = ? ORDER BY `video`.`id` LIMIT 1"))
	existing.WithArgs(request.UploadID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "upload_id", "author_id", "play_url", "cover_url", "title", "publish_time", "favorite_count", "comment_count", "topic"}).
			AddRow(81, request.UploadID, 7, "http://media/video.mp4", "http://media/cover.jpg", "示例标题", time.UnixMilli(1700000000000), 0, 0, "默认"))
	mock.ExpectCommit()
	retry, err := logic.CreateVideo(request)
	if err != nil || retry.VideoID != first.VideoID {
		t.Fatalf("retry CreateVideo() = (%+v, %v), want same ID %d", retry, err, first.VideoID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestCreateVideoRejectsMissingRequiredFields(t *testing.T) {
	db, _, closeDB := newVideoLogicTestDB(t)
	defer closeDB()
	logic := NewCreateVideoLogic(context.Background(), &svc.ServiceContext{DBList: &svc.DBList{Mysql: db}})
	valid := &video.CreateVideoRequest{
		UploadID: "9b2e6a61-1ab9-4d22-918d-8c2a2f7bd601",
		VideoID:  &video.VideoInfo{AuthorID: 7, PlayURL: "http://media/video.mp4", Title: "标题"},
	}
	for _, test := range []struct {
		name string
		edit func(*video.CreateVideoRequest)
	}{
		{name: "empty title", edit: func(in *video.CreateVideoRequest) { in.VideoID.Title = " " }},
		{name: "empty author", edit: func(in *video.CreateVideoRequest) { in.VideoID.AuthorID = 0 }},
		{name: "empty required URL", edit: func(in *video.CreateVideoRequest) { in.VideoID.PlayURL = "" }},
		{name: "invalid upload ID", edit: func(in *video.CreateVideoRequest) { in.UploadID = "../video" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := protoCloneCreateVideo(valid)
			test.edit(request)
			if _, err := logic.CreateVideo(request); err == nil {
				t.Fatal("CreateVideo() error = nil, want validation error")
			}
		})
	}
}

func newVideoLogicTestDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	var conn *sql.DB
	conn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	db, err := gorm.Open(mysql.New(mysql.Config{Conn: conn, SkipInitializeWithVersion: true}), &gorm.Config{
		DisableAutomaticPing: true,
		NamingStrategy:       schema.NamingStrategy{SingularTable: true},
	})
	if err != nil {
		_ = conn.Close()
		t.Fatalf("open GORM test DB: %v", err)
	}
	return db, mock, func() { _ = conn.Close() }
}

func protoCloneCreateVideo(in *video.CreateVideoRequest) *video.CreateVideoRequest {
	return proto.Clone(in).(*video.CreateVideoRequest)
}
