package main

import (
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

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
