package main

import (
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const PayloadKey = "payload"

type Handler func(id string, payload string) error

type Config struct {
	group    string
	consumer string

	streams  []string
	handlers map[string]Handler

	readTimeout          time.Duration
	reclaimEnabled       bool
	reclaimCount         int
	reclaimInterval      time.Duration
	reclaimMinIdleTime   time.Duration
	reclaimMaxDeliveries int

	logger *log.Logger

	redis *redis.Options
}

func (config *Config) addListener(stream string, h Handler) {
	config.handlers[stream] = h
}

func (config *Config) callHandler(stream string, m redis.XMessage) error {
	if handler, ok := config.handlers[stream]; ok {
		return handler(m.ID, m.Values[PayloadKey].(string))
	}

	return nil
}
