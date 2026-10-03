package handler

import (
	"context"
	"errors"
	"io"
	"mime"
	"strconv"
	"strings"

	"douyin/package/util"
	"douyin/response"
	"douyin/service"

	"github.com/gofiber/fiber/v2"
)

type uploadAPI interface {
	Init(ctx context.Context, userID uint64, req service.UploadInitRequest) (service.UploadInitResult, error)
	UploadChunk(ctx context.Context, userID uint64, uploadID string, partNumber int, declaredSize int64, src io.Reader) (service.UploadChunkResult, error)
	Merge(ctx context.Context, userID uint64, uploadID, expectedMD5 string) (service.UploadMergeResult, error)
}

type UploadHandler struct {
	api uploadAPI
}

func NewUploadHandler(api uploadAPI) *UploadHandler { return &UploadHandler{api: api} }

func (h *UploadHandler) Init(c *fiber.Ctx) error {
	claims, err := uploadClaims(c)
	if err != nil {
		return uploadFailure(c, response.WrongToken)
	}
	var req service.UploadInitRequest
	if err := c.BodyParser(&req); err != nil {
		return uploadFailure(c, response.BadParaRequest)
	}
	if h == nil || h.api == nil {
		return uploadFailure(c, "上传服务未初始化")
	}
	result, err := h.api.Init(c.UserContext(), claims.UserID, req)
	if err != nil {
		return uploadFailure(c, err.Error())
	}
	return c.JSON(struct {
		StatusCode      int    `json:"status_code"`
		StatusMsg       string `json:"status_msg"`
		UploadID        string `json:"upload_id,omitempty"`
		UploadedParts   []int  `json:"uploaded_parts,omitempty"`
		AlreadyUploaded bool   `json:"already_uploaded"`
		VideoID         uint64 `json:"video_id,omitempty"`
	}{
		StatusCode: response.Success, StatusMsg: response.UploadVideoSuccess,
		UploadID: result.UploadID, UploadedParts: result.UploadedParts,
		AlreadyUploaded: result.AlreadyUploaded, VideoID: result.VideoID,
	})
}

func (h *UploadHandler) Chunk(c *fiber.Ctx) error {
	claims, err := uploadClaims(c)
	if err != nil {
		return uploadFailure(c, response.WrongToken)
	}
	mediaType, _, err := mime.ParseMediaType(c.Get("Content-Type"))
	if err != nil || mediaType != "application/octet-stream" {
		return uploadFailure(c, "分片请求必须使用 application/octet-stream")
	}
	uploadID := c.Query("upload_id")
	partNumber, err := strconv.Atoi(c.Query("part_number"))
	if err != nil || partNumber < 1 {
		return uploadFailure(c, "分片序号不合法")
	}
	declaredSize, err := strconv.ParseInt(c.Query("size"), 10, 64)
	if err != nil || declaredSize <= 0 {
		return uploadFailure(c, "分片大小不合法")
	}
	contentLength := c.Request().Header.ContentLength()
	if contentLength >= 0 && int64(contentLength) != declaredSize {
		return uploadFailure(c, "Content-Length 与分片大小不一致")
	}
	stream := c.Request().BodyStream()
	if stream == nil || !c.Request().IsBodyStream() {
		return uploadFailure(c, "分片请求未启用流式读取")
	}
	defer c.Request().CloseBodyStream()
	if h == nil || h.api == nil {
		return uploadFailure(c, "上传服务未初始化")
	}
	result, err := h.api.UploadChunk(c.UserContext(), claims.UserID, uploadID, partNumber, declaredSize, stream)
	if err != nil {
		return uploadFailure(c, err.Error())
	}
	return c.JSON(struct {
		StatusCode int    `json:"status_code"`
		StatusMsg  string `json:"status_msg"`
		PartNumber int    `json:"part_number"`
		MD5        string `json:"md5"`
	}{StatusCode: response.Success, StatusMsg: "分片上传成功", PartNumber: result.PartNumber, MD5: result.MD5})
}

func (h *UploadHandler) Merge(c *fiber.Ctx) error {
	claims, err := uploadClaims(c)
	if err != nil {
		return uploadFailure(c, response.WrongToken)
	}
	var req struct {
		UploadID string `json:"upload_id"`
		FileMD5  string `json:"file_md5"`
	}
	if err := c.BodyParser(&req); err != nil {
		return uploadFailure(c, response.BadParaRequest)
	}
	if h == nil || h.api == nil {
		return uploadFailure(c, "上传服务未初始化")
	}
	result, err := h.api.Merge(c.UserContext(), claims.UserID, req.UploadID, req.FileMD5)
	if err != nil {
		return uploadFailure(c, err.Error())
	}
	return c.JSON(struct {
		StatusCode int    `json:"status_code"`
		StatusMsg  string `json:"status_msg"`
		VideoID    uint64 `json:"video_id"`
		PlayURL    string `json:"play_url"`
	}{StatusCode: response.Success, StatusMsg: response.UploadVideoSuccess, VideoID: result.VideoID, PlayURL: result.PlayURL})
}

func uploadClaims(c *fiber.Ctx) (*util.UserClaims, error) {
	token := strings.TrimSpace(c.Get("token"))
	if token == "" {
		authorization := strings.TrimSpace(c.Get("Authorization"))
		if strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
			token = strings.TrimSpace(authorization[len("Bearer "):])
		}
	}
	if token == "" {
		return nil, errors.New("token 未提供")
	}
	return util.ParseToken(token)
}

func uploadFailure(c *fiber.Ctx, message string) error {
	if strings.TrimSpace(message) == "" {
		message = response.BadParaRequest
	}
	return c.JSON(response.CommonResponse{StatusCode: response.Failed, StatusMsg: message})
}
