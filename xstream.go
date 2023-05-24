package xstream

import (
	"context"
	"log"

	"github.com/joaofnds/xstream/config"
	"github.com/joaofnds/xstream/reader"
	"github.com/joaofnds/xstream/reclaimer"
	"github.com/joaofnds/xstream/writer"
)

type XStream struct {
	config    *config.Config
	reader    *reader.Reader
	writer    *writer.Writer
	reclaimer *reclaimer.Reclaimer
}

func NewXStream(cfg *config.Config) *XStream {
	if cfg.Handlers == nil {
		cfg.Handlers = map[string]config.Handler{}
	}

	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}

	return &XStream{
		config:    cfg,
		reader:    reader.NewReader(cfg),
		writer:    writer.NewWriter(cfg),
		reclaimer: reclaimer.NewReclaimer(cfg),
	}
}

func (x *XStream) Start(ctx context.Context) error {
	if err := x.Ping(ctx); err != nil {
		return err
	}

	if err := x.writer.Start(ctx); err != nil {
		return err
	}

	go x.reader.Start(ctx)
	if x.config.ReclaimEnabled {
		go x.reclaimer.Start(ctx)
	}

	return nil
}

func (x *XStream) Stop(ctx context.Context) error {
	if err := x.reader.Stop(ctx); err != nil {
		return err
	}

	if err := x.writer.Stop(ctx); err != nil {
		return err
	}

	return x.reclaimer.Stop(ctx)
}

func (x *XStream) Ping(ctx context.Context) error {
	if err := x.reader.Ping(ctx); err != nil {
		return err
	}

	if err := x.writer.Ping(ctx); err != nil {
		return err
	}

	return x.reclaimer.Ping(ctx)
}

func (x *XStream) On(stream string, h config.Handler) {
	x.config.AddListener(stream, h)
}

func (x *XStream) OnDLQ(stream string, h config.Handler) {
	x.config.AddListener(x.config.DLQFormat(stream), h)
}

func (x *XStream) Emit(ctx context.Context, stream string, payload string) error {
	return x.writer.Emit(ctx, stream, payload)
}
