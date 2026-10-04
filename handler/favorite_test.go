package handler

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"douyin/response"

	"github.com/gofiber/fiber/v2"
)

func TestFavoriteCountResponseHeaderKeepsJSONBody(t *testing.T) {
	app := fiber.New()
	app.Get("/", func(c *fiber.Ctx) error {
		count := int64(8)
		setFavoriteCountResponseHeaders(c, &count)
		return c.JSON(response.CommonResponse{StatusCode: response.Success, StatusMsg: "点赞成功"})
	})

	got, err := app.Test(httptest.NewRequest("GET", "/", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	if value := got.Header.Get("X-Favorite-Count"); value != "8" {
		t.Fatalf("X-Favorite-Count = %q, want 8", value)
	}
	if value := got.Header.Get("Access-Control-Expose-Headers"); value != "X-Favorite-Count" {
		t.Fatalf("Access-Control-Expose-Headers = %q", value)
	}
	var body response.CommonResponse
	if err := json.NewDecoder(got.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.StatusCode != response.Success || body.StatusMsg != "点赞成功" {
		t.Fatalf("JSON response changed: %#v", body)
	}
}

func TestFavoriteCountResponseHeaderCanBeOmitted(t *testing.T) {
	app := fiber.New()
	app.Get("/", func(c *fiber.Ctx) error {
		setFavoriteCountResponseHeaders(c, nil)
		return c.JSON(response.CommonResponse{StatusCode: response.Success, StatusMsg: "点赞成功"})
	})

	got, err := app.Test(httptest.NewRequest("GET", "/", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	if value := got.Header.Get("X-Favorite-Count"); value != "" {
		t.Fatalf("X-Favorite-Count = %q, want omitted", value)
	}
}
