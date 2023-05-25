package xstream

import (
	"context"
	"log"
)

type XStream struct {
	config    *Config
	reader    *reader
	writer    *writer
	reclaimer *reclaimer
}

func NewXStream(cfg *Config) *XStream {
	if cfg.Handlers == nil {
		cfg.Handlers = map[string]Handler{}
	}

	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}

	return &XStream{
		config:    cfg,
		reader:    newReader(cfg),
		writer:    newWriter(cfg),
		reclaimer: newReclaimer(cfg),
	}
}

func (x *XStream) Start(ctx context.Context) error {
	if err := x.Ping(ctx); err != nil {
		return err
	}

	if err := x.writer.start(ctx); err != nil {
		return err
	}

	go x.reader.start(ctx)
	if x.config.ReclaimEnabled {
		go x.reclaimer.start(ctx)
	}

	return nil
}

func (x *XStream) Stop(ctx context.Context) error {
	if err := x.reader.stop(ctx); err != nil {
		return err
	}

	if err := x.writer.stop(ctx); err != nil {
		return err
	}

	return x.reclaimer.stop(ctx)
}

func (x *XStream) Ping(ctx context.Context) error {
	if err := x.reader.ping(ctx); err != nil {
		return err
	}

	if err := x.writer.ping(ctx); err != nil {
		return err
	}

	return x.reclaimer.ping(ctx)
}

func (x *XStream) On(stream string, h Handler) {
	x.config.addListener(stream, h)
}

func (x *XStream) OnDead(stream string, h Handler) {
	x.config.addListener(dlqFormat(stream), h)
}

func (x *XStream) Emit(ctx context.Context, stream string, msg Message) error {
	return x.writer.emit(ctx, stream, msg)
}
