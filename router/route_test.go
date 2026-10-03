package router

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestProtectUploadInternalFilesHidesChunksAndMergeScratch(t *testing.T) {
	root := t.TempDir()
	tempDir := filepath.Join(root, "private", "sessions")
	uploadID := "9b2e6a61-1ab9-4d22-918d-8c2a2f7bd601"
	files := map[string]string{
		filepath.Join("private", "sessions", uploadID, "meta.json"):             "metadata",
		filepath.Join("private", "sessions", uploadID, "part_00001"):            "chunk",
		filepath.Join("videos", ".upload-merge", "."+uploadID+"-crashed.merge"): "scratch",
	}
	for relativePath, content := range files {
		path := filepath.Join(root, relativePath)
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	app := fiber.New()
	ProtectUploadInternalFiles(app, "/static", root, tempDir)
	app.Static("/static", root)
	ProtectUploadInternalFiles(app, "/video", root, tempDir)
	app.Static("/video", root)

	blockedPaths := []string{
		"/static/private/sessions/" + uploadID + "/meta.json",
		"/video/private/sessions/" + uploadID + "/part_00001",
		"/static/videos/.upload-merge/." + uploadID + "-crashed.merge",
		"/video/videos/.upload-merge/." + uploadID + "-crashed.merge",
	}
	for _, path := range blockedPaths {
		response, err := app.Test(httptest.NewRequest(http.MethodGet, path, nil))
		if err != nil {
			t.Fatalf("GET %s error = %v", path, err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s status = %d, want %d", path, response.StatusCode, http.StatusNotFound)
		}
	}
}
