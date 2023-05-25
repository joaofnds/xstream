package writer

import (
	"context"
	"strings"

	"github.com/joaofnds/xstream/config"

	"github.com/redis/go-redis/v9"
)

type Writer struct {
	config *config.Config
	client *redis.Client
}

func NewWriter(cfg *config.Config) *Writer {
	return &Writer{config: cfg, client: redis.NewClient(cfg.Redis)}
}

func (w *Writer) Start(ctx context.Context) error {
	return w.ensureGroupsExist(ctx)
}

func (w *Writer) Stop(_ context.Context) error {
	return w.client.Close()
}

func (w *Writer) Ping(ctx context.Context) error {
	return w.client.Ping(ctx).Err()
}

func (w *Writer) Emit(ctx context.Context, stream string, msg config.Message) error {
	if msg.ID == "" {
		msg.ID = "*"
	}

	values := map[string]any{config.BodyKey: msg.Body}
	if msg.Metadata != nil {
		values[config.MetadataKey] = msg.MetadataString()
	}

	return w.client.XAdd(ctx, &redis.XAddArgs{
		ID:     msg.ID,
		Stream: stream,
		Values: values,
	}).Err()
}

func (w *Writer) ensureGroupsExist(ctx context.Context) error {
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
