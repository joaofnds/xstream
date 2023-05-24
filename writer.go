package main

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
)

type Writer struct {
	config *Config
	client *redis.Client
}

func NewWriter(config *Config) *Writer {
	return &Writer{config: config, client: redis.NewClient(config.redis)}
}

func (w *Writer) Start(ctx context.Context) error {
	return w.ensureGroupsExist(ctx)
}

func (w *Writer) Stop(context.Context) error {
	return nil
}

func (w *Writer) Ping(ctx context.Context) error {
	return w.client.Ping(ctx).Err()
}

func (w *Writer) Emit(ctx context.Context, stream string, payload string) error {
	return w.client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		ID:     "*",
		Values: map[string]any{PayloadKey: payload},
	}).Err()
}

func (w *Writer) ensureGroupsExist(ctx context.Context) error {
	for _, stream := range w.config.streams {
		err := w.client.XGroupCreateMkStream(ctx, stream, w.config.group, "$").Err()
		if err != nil {
			if !strings.Contains(err.Error(), "BUSYGROUP") {
				return err
			}
		} else {
			w.config.logger.Println("group created for stream " + stream)
		}
	}

	return nil
}
