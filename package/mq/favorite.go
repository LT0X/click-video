package mq

import "fmt"

// SendFavoriteMessage 将点赞动作持久化到 RabbitMQ，由 user.rpc 消费。
func SendFavoriteMessage(event FavoriteActionEvent) error {
	if favoriteBroker == nil {
		return fmt.Errorf("点赞事件 broker 尚未初始化")
	}
	return favoriteBroker.PublishFavoriteAction(event)
}
