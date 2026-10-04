package handler

import (
	"douyin/package/constant"
	"douyin/package/util"
	"douyin/response"
	"douyin/service"
	"fmt"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

func FavoriteVideoAction(c *fiber.Ctx) error {
	var service service.FavoriteService
	err := c.QueryParser(&service)
	if err != nil {
		zap.L().Error(err.Error())
		res := response.CommonResponse{
			StatusCode: response.Failed,
			StatusMsg:  response.BadParaRequest,
		}
		c.Status(fiber.StatusOK)
		return c.JSON(res)
	}
	userID := c.Locals(constant.UserID).(uint64)
	var resp *response.CommonResponse
	var favoriteCount *int64
	if service.ActionType == constant.DoAction {
		resp, favoriteCount, err = service.Favorite(userID)
	} else if service.ActionType == constant.UndoAction {
		resp, favoriteCount, err = service.UnFavorite(userID)
	} else {
		err = fmt.Errorf(constant.BadParaRequest)
	}
	if err != nil {
		res := response.CommonResponse{
			StatusCode: response.Failed,
			StatusMsg:  err.Error(),
		}
		c.Status(fiber.StatusOK)
		return c.JSON(res)
	}
	setFavoriteCountResponseHeaders(c, favoriteCount)
	c.Status(fiber.StatusOK)
	return c.JSON(resp)
}

func setFavoriteCountResponseHeaders(c *fiber.Ctx, favoriteCount *int64) {
	c.Set("Access-Control-Expose-Headers", "X-Favorite-Count")
	if favoriteCount != nil && *favoriteCount >= 0 {
		c.Set("X-Favorite-Count", strconv.FormatInt(*favoriteCount, 10))
	}
}

func FavoriteList(c *fiber.Ctx) error {
	var service service.FavoriteService
	err := c.QueryParser(&service)
	token := c.Get("token")
	if service.Token == "" {
		service.Token = token
	}
	if err != nil {
		zap.L().Error(err.Error())
		res := response.VideoListResponse{
			StatusCode: response.Failed,
			StatusMsg:  response.BadParaRequest,
			VideoList:  nil,
		}
		c.Status(fiber.StatusOK)
		return c.JSON(res)
	}
	var userID uint64
	if service.Token == "" {
		userID = 0
	} else {
		claims, err := util.ParseToken(service.Token)
		if err != nil {
			res := response.UserRegisterOrLogin{
				StatusCode: response.Failed,
				StatusMsg:  response.WrongToken,
			}
			c.Status(fiber.StatusOK)
			return c.JSON(res)
		}
		userID = claims.UserID
	}
	resp, err := service.FavoriteList(userID)
	if err != nil {
		res := response.VideoListResponse{
			StatusCode: response.Failed,
			StatusMsg:  response.WrongToken,
			VideoList:  nil,
		}
		c.Status(fiber.StatusOK)
		return c.JSON(res)
	}
	res := response.VideoListResponse{
		StatusCode: response.Success,
		StatusMsg:  response.FavoriteListSuccess,
		VideoList:  resp,
	}
	c.Status(fiber.StatusOK)
	return c.JSON(res)
}
