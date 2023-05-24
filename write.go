package main

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
)

type Writer struct {
	config *Config
	conn   *Connection
}

func NewWriter(config *Config, conn *Connection) *Writer {
	return &Writer{config: config, conn: conn}
}

func (w *Writer) Start(ctx context.Context) error {
	return w.ensureGroupsExists(ctx)
}

func (w *Writer) Stop(context.Context) error {
	return nil
}

func (w *Writer) Emit(ctx context.Context, stream string, payload string) error {
	return w.conn.write.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		ID:     "*",
		Values: map[string]any{PayloadKey: payload},
	}).Err()
}

func (w *Writer) ensureGroupsExists(ctx context.Context) error {
	for _, stream := range w.config.streams {
		err := w.conn.write.XGroupCreateMkStream(ctx, stream, w.config.group, "$").Err()
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
