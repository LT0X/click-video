package logic

import (
	"context"
	"regexp"
	"testing"

	"douyin/rpc/user/internal/svc"
	"douyin/rpc/user/user"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func TestIncrementWorkCountAppliesOncePerVideo(t *testing.T) {
	db, mock, closeDB := newWorkCountLogicTestDB(t)
	defer closeDB()
	logic := NewIncrementWorkCountLogic(context.Background(), &svc.ServiceContext{DBList: &svc.DBList{Mysql: db}})
	request := &user.IncrementWorkCountRequest{UserID: 7, VideoID: 81}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `work_count_video`")).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE `user` SET `work_count`=work_count + ? WHERE id = ?")).
		WithArgs(1, uint64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	first, err := logic.IncrementWorkCount(request)
	if err != nil || !first.Applied {
		t.Fatalf("first IncrementWorkCount() = (%+v, %v), want applied", first, err)
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `work_count_video`")).WillReturnResult(sqlmock.NewResult(0, 0))
	existing := mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `work_count_video` WHERE video_id = ? ORDER BY `work_count_video`.`video_id` LIMIT 1"))
	existing.WithArgs(uint64(81)).WillReturnRows(sqlmock.NewRows([]string{"video_id", "user_id"}).AddRow(81, 7))
	mock.ExpectCommit()
	retry, err := logic.IncrementWorkCount(request)
	if err != nil || retry.Applied {
		t.Fatalf("retry IncrementWorkCount() = (%+v, %v), want not applied", retry, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestIncrementWorkCountRejectsUnknownUser(t *testing.T) {
	db, mock, closeDB := newWorkCountLogicTestDB(t)
	defer closeDB()
	logic := NewIncrementWorkCountLogic(context.Background(), &svc.ServiceContext{DBList: &svc.DBList{Mysql: db}})
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `work_count_video`")).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE `user` SET `work_count`=work_count + ? WHERE id = ?")).
		WithArgs(1, uint64(404)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	if _, err := logic.IncrementWorkCount(&user.IncrementWorkCountRequest{UserID: 404, VideoID: 81}); err == nil {
		t.Fatal("IncrementWorkCount() error = nil, want unknown user error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func newWorkCountLogicTestDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
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
