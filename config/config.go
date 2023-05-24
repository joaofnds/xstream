package config

import (
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const PayloadKey = "payload"

type Handler func(id string, payload string) error

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

	Redis *redis.Options
}

func (config *Config) AddListener(stream string, h Handler) {
	config.Handlers[stream] = h
}

func (config *Config) CallHandler(stream string, m redis.XMessage) error {
	if handler, ok := config.Handlers[stream]; ok {
		return handler(m.ID, m.Values[PayloadKey].(string))
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
