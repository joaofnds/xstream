package xstream

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
)

type writer struct {
	config *Config
	client *redis.Client
}

func newWriter(cfg *Config) *writer {
	return &writer{config: cfg, client: redis.NewClient(cfg.Redis)}
}

func (w *writer) start(ctx context.Context) error {
	return w.ensureGroupsExist(ctx)
}

func (w *writer) stop(_ context.Context) error {
	return w.client.Close()
}

func (w *writer) ping(ctx context.Context) error {
	return w.client.Ping(ctx).Err()
}

func (w *writer) emit(ctx context.Context, stream string, msg Message) error {
	if msg.ID == "" {
		msg.ID = "*"
	}

	values := map[string]any{BodyKey: msg.Body}
	if msg.Metadata != nil {
		values[MetadataKey] = msg.metadataString()
	}

	return w.client.XAdd(ctx, &redis.XAddArgs{
		ID:     msg.ID,
		Stream: stream,
		Values: values,
	}).Err()
}

func (w *writer) ensureGroupsExist(ctx context.Context) error {
	for _, stream := range w.config.Streams {
		err := w.client.XGroupCreateMkStream(ctx, stream, w.config.Group, "$").Err()
		if err != nil {
			if !strings.Contains(err.Error(), "BUSYGROUP") {
				return err
			}
		} else {
			w.config.Logger.Println("Group created for stream " + stream)
		}
	}

	return nil
}
