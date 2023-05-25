package config

import (
	"encoding/json"

	"github.com/redis/go-redis/v9"
)

type Message struct {
	ID       string
	Body     string
	Metadata map[string]string
}

func NewMessageFromRedis(redisMsg redis.XMessage) Message {
	var metadata map[string]string
	if v, ok := redisMsg.Values[MetadataKey].(string); ok {
		json.Unmarshal([]byte(v), &metadata)
	}
	return Message{
		ID:       redisMsg.ID,
		Body:     redisMsg.Values[BodyKey].(string),
		Metadata: metadata,
	}
}

func (m Message) MetadataString() string {
	b, _ := json.Marshal(m.Metadata)
	return string(b)
}
