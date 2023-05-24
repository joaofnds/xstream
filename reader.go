package main

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Reader struct {
	config *Config
	client *redis.Client
}

func NewReader(config *Config) *Reader {
	return &Reader{config: config, client: redis.NewClient(config.redis)}
}

func (r *Reader) Start(ctx context.Context) error {
	for {
		streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    r.config.group,
			Consumer: r.config.consumer,
			Streams:  streamsReadFormat(r.config.streams),
			Block:    r.config.readTimeout,
			Count:    1,
		}).Result()

		if err != nil {
			return err
		}

		for _, s := range streams {
			for _, m := range s.Messages {
				r.process(ctx, s.Stream, m)
			}
		}
	}
}

func (r *Reader) Stop(context.Context) error {
	return r.client.Close()
}

func (r *Reader) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r *Reader) process(ctx context.Context, stream string, msg redis.XMessage) error {
	if err := r.config.callHandler(stream, msg); err != nil {
		return err
	}

	return r.client.XAck(ctx, stream, r.config.group, msg.ID).Err()
}
