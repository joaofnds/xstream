package config

import (
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const BodyKey = "payload"
const MetadataKey = "metadata"

type Handler func(Message) error
type RedisOptions = redis.Options

type Config struct {
	Group    string
	Consumer string

	Streams  []string
	Handlers map[string]Handler

	ReadTimeout          time.Duration
	ReclaimEnabled       bool
	ReclaimCount         int
	ReclaimInterval      time.Duration
	ReclaimMinIdleTime   time.Duration
	ReclaimMaxDeliveries int

	Logger *log.Logger

	Redis *RedisOptions
}

func (config *Config) AddListener(stream string, h Handler) {
	config.Handlers[stream] = h
}

func (config *Config) CallHandler(stream string, m redis.XMessage) error {
	if handler, ok := config.Handlers[stream]; ok {
		return handler(NewMessageFromRedis(m))
	}
	return nil
}

func (config *Config) StreamsReadFormat(streams []string) []string {
	result := make([]string, 0, len(streams)*2)
	for _, s := range streams {
		result = append(result, s, ">")
	}
	return result
}

func (config *Config) DLQFormat(stream string) string {
	return "dead:" + stream
}
