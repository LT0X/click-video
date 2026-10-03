package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"douyin/config"
	"douyin/package/util"
	"douyin/service"

	"github.com/gofiber/fiber/v2"
)

func TestUploadInitReturnsResumeAndFastPassResults(t *testing.T) {
	token := uploadTestToken(t)
	api := &uploadAPIMock{initResult: service.UploadInitResult{
		UploadID: "9b2e6a61-1ab9-4d22-918d-8c2a2f7bd601", UploadedParts: []int{1, 3},
	}}
	app := newUploadTestApp(api)
	body, _ := json.Marshal(service.UploadInitRequest{
		FileName: "clip.mp4", FileSize: 10, FileMD5: "5eb63bbbe01eeed093cb22bb8f5acdc3",
		TotalParts: 2, Title: "标题", Topic: "默认",
	})
	request := httptest.NewRequest(http.MethodPost, "/init", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("token", token)
	response, err := app.Test(request)
	if err != nil {
		t.Fatalf("POST /init: %v", err)
	}
	defer response.Body.Close()
	var got struct {
		StatusCode    int    `json:"status_code"`
		UploadID      string `json:"upload_id"`
		UploadedParts []int  `json:"uploaded_parts"`
	}
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatalf("decode init response: %v", err)
	}
	if got.StatusCode != 0 || got.UploadID != api.initResult.UploadID || len(got.UploadedParts) != 2 || got.UploadedParts[1] != 3 {
		t.Fatalf("init response = %+v", got)
	}

	api.initResult = service.UploadInitResult{AlreadyUploaded: true, VideoID: 91}
	request = httptest.NewRequest(http.MethodPost, "/init", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("token", token)
	response, err = app.Test(request)
	if err != nil {
		t.Fatalf("POST /init fast pass: %v", err)
	}
	defer response.Body.Close()
	var fastPass struct {
		AlreadyUploaded bool   `json:"already_uploaded"`
		VideoID         uint64 `json:"video_id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&fastPass); err != nil {
		t.Fatalf("decode fast pass response: %v", err)
	}
	if !fastPass.AlreadyUploaded || fastPass.VideoID != 91 {
		t.Fatalf("fast pass response = %+v, want existing video 91", fastPass)
	}
}

func TestUploadChunkStreamsRawBodyAndRejectsInvalidToken(t *testing.T) {
	token := uploadTestToken(t)
	api := &uploadAPIMock{}
	app := newUploadTestApp(api)
	request := httptest.NewRequest(http.MethodPost, "/chunk?upload_id=9b2e6a61-1ab9-4d22-918d-8c2a2f7bd601&part_number=1&size=5", bytes.NewBufferString("hello"))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("token", token)
	response, err := app.Test(request)
	if err != nil {
		t.Fatalf("POST /chunk: %v", err)
	}
	response.Body.Close()
	if string(api.chunkBody) != "hello" || api.chunkCalls != 1 {
		t.Fatalf("chunk body/calls = %q/%d, want hello/1", api.chunkBody, api.chunkCalls)
	}

	request = httptest.NewRequest(http.MethodPost, "/chunk?upload_id=9b2e6a61-1ab9-4d22-918d-8c2a2f7bd601&part_number=1&size=5", bytes.NewBufferString("hello"))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("token", "invalid-token")
	response, err = app.Test(request)
	if err != nil {
		t.Fatalf("POST /chunk invalid token: %v", err)
	}
	defer response.Body.Close()
	var failed struct {
		StatusCode int `json:"status_code"`
	}
	if err := json.NewDecoder(response.Body).Decode(&failed); err != nil {
		t.Fatalf("decode invalid-token response: %v", err)
	}
	if failed.StatusCode != -1 || api.chunkCalls != 1 {
		t.Fatalf("invalid-token response/calls = %+v/%d, want failed and no extra call", failed, api.chunkCalls)
	}
}

func TestUploadMergeReturnsVideoAndPropagatesFailures(t *testing.T) {
	token := uploadTestToken(t)
	api := &uploadAPIMock{mergeResult: service.UploadMergeResult{VideoID: 91, PlayURL: "/video/videos/clip.mp4"}}
	app := newUploadTestApp(api)
	requestBody := []byte(`{"upload_id":"9b2e6a61-1ab9-4d22-918d-8c2a2f7bd601","file_md5":"5eb63bbbe01eeed093cb22bb8f5acdc3"}`)
	request := httptest.NewRequest(http.MethodPost, "/merge", bytes.NewReader(requestBody))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("token", token)
	response, err := app.Test(request)
	if err != nil {
		t.Fatalf("POST /merge: %v", err)
	}
	var got struct {
		StatusCode int    `json:"status_code"`
		VideoID    uint64 `json:"video_id"`
		PlayURL    string `json:"play_url"`
	}
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatalf("decode merge response: %v", err)
	}
	response.Body.Close()
	if got.StatusCode != 0 || got.VideoID != 91 || got.PlayURL != api.mergeResult.PlayURL {
		t.Fatalf("merge response = %+v", got)
	}

	api.mergeErr = errors.New("上传分片不完整")
	request = httptest.NewRequest(http.MethodPost, "/merge", bytes.NewReader(requestBody))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("token", token)
	response, err = app.Test(request)
	if err != nil {
		t.Fatalf("POST /merge failure: %v", err)
	}
	defer response.Body.Close()
	var failed struct {
		StatusCode int `json:"status_code"`
	}
	if err := json.NewDecoder(response.Body).Decode(&failed); err != nil {
		t.Fatalf("decode merge error: %v", err)
	}
	if failed.StatusCode != -1 {
		t.Fatalf("merge error response = %+v, want failed", failed)
	}
}

func newUploadTestApp(api uploadAPI) *fiber.App {
	handler := NewUploadHandler(api)
	app := fiber.New(fiber.Config{StreamRequestBody: true, BodyLimit: 30 * 1024 * 1024})
	app.Post("/init", handler.Init)
	app.Post("/chunk", handler.Chunk)
	app.Post("/merge", handler.Merge)
	return app
}

type uploadAPIMock struct {
	initResult  service.UploadInitResult
	chunkBody   []byte
	chunkCalls  int
	mergeResult service.UploadMergeResult
	mergeErr    error
}

func (m *uploadAPIMock) Init(context.Context, uint64, service.UploadInitRequest) (service.UploadInitResult, error) {
	return m.initResult, nil
}

func (m *uploadAPIMock) UploadChunk(_ context.Context, _ uint64, _ string, _ int, _ int64, body io.Reader) (service.UploadChunkResult, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return service.UploadChunkResult{}, err
	}
	m.chunkBody = data
	m.chunkCalls++
	return service.UploadChunkResult{PartNumber: 1}, nil
}

func (m *uploadAPIMock) Merge(context.Context, uint64, string, string) (service.UploadMergeResult, error) {
	return m.mergeResult, m.mergeErr
}

func uploadTestToken(t *testing.T) string {
	t.Helper()
	previous := config.System.JwtSecret
	config.System.JwtSecret = "upload-handler-test-secret"
	t.Cleanup(func() { config.System.JwtSecret = previous })
	token, err := util.SignToken(23)
	if err != nil {
		t.Fatalf("sign test token: %v", err)
	}
	return token
}
