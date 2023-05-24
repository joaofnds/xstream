package main

import (
	"context"
	"log"
)

type XStream struct {
	config    *Config
	reader    *Reader
	writer    *Writer
	reclaimer *Reclaimer
}

func NewXStream(config *Config) *XStream {
	if config.handlers == nil {
		config.handlers = map[string]Handler{}
	}

	if config.logger == nil {
		config.logger = log.Default()
	}

	return &XStream{
		config:    config,
		reader:    NewReader(config),
		writer:    NewWriter(config),
		reclaimer: NewReclaimer(config),
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
	if x.config.reclaimEnabled {
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

func (x *XStream) On(stream string, h Handler) {
	x.config.addListener(stream, h)
}

func (x *XStream) OnDLQ(stream string, h Handler) {
	x.config.addListener(dlqFormat(stream), h)
}

func streamsReadFormat(streams []string) []string {
	result := make([]string, 0, len(streams)*2)
	for _, s := range streams {
		result = append(result, s, ">")
	}
	return result
}

func dlqFormat(stream string) string {
	return "dead:" + stream
}
