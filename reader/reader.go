package reader

import (
	"context"

	"github.com/joaofnds/xstream/config"

	"github.com/redis/go-redis/v9"
)

type Reader struct {
	config *config.Config
	client *redis.Client
}

func NewReader(cfg *config.Config) *Reader {
	return &Reader{config: cfg, client: redis.NewClient(cfg.Redis)}
}

func (r *Reader) Start(ctx context.Context) error {
	for {
		streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    r.config.Group,
			Consumer: r.config.Consumer,
			Streams:  r.config.StreamsReadFormat(r.config.Streams),
			Block:    r.config.ReadTimeout,
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

func (r *Reader) Stop(_ context.Context) error {
	return r.client.Close()
}

func (r *Reader) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r *Reader) process(ctx context.Context, stream string, msg redis.XMessage) error {
	if err := r.config.CallHandler(stream, msg); err != nil {
		return err
	}

	return r.client.XAck(ctx, stream, r.config.Group, msg.ID).Err()
}
