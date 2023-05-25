package xstream

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type reader struct {
	config *Config
	client *redis.Client
}

func newReader(cfg *Config) *reader {
	return &reader{config: cfg, client: redis.NewClient(cfg.Redis)}
}

func (r *reader) start(ctx context.Context) error {
	for {
		streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    r.config.Group,
			Consumer: r.config.Consumer,
			Streams:  streamsReadFormat(r.config.Streams),
			Block:    r.config.ReadTimeout,
			Count:    1,
		}).Result()

		if err != nil {
			return err
		}

		for _, s := range streams {
			for _, m := range s.Messages {
				go r.process(ctx, s.Stream, m)
			}
		}
	}
}

func (r *reader) stop(_ context.Context) error {
	return r.client.Close()
}

func (r *reader) ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r *reader) process(ctx context.Context, stream string, msg redis.XMessage) error {
	if err := r.config.callHandler(stream, msg); err != nil {
		return err
	}

	return r.client.XAck(ctx, stream, r.config.Group, msg.ID).Err()
}
