package main

import (
	"context"
	"log"
)

type XStream struct {
	config    *Config
	conn      *Connection
	listener  *Reader
	reclaimer *Reclaimer
	writer    *Writer
}

func NewXStream(config *Config) *XStream {
	if config.handlers == nil {
		config.handlers = map[string]Handler{}
	}

	if config.logger == nil {
		config.logger = log.Default()
	}

	conn := NewConnection(config.redis)

	return &XStream{
		config:    config,
		conn:      conn,
		listener:  NewReader(config, conn),
		writer:    NewWriter(config, conn),
		reclaimer: NewReclaimer(config, conn),
	}
}

func (x *XStream) Ping(ctx context.Context) error {
	return x.conn.Ping(ctx)
}

func (x *XStream) Start(ctx context.Context) error {
	if err := x.Ping(ctx); err != nil {
		return err
	}

	if err := x.writer.Start(ctx); err != nil {
		return err
	}

	go x.listener.Start(ctx)
	if x.config.reclaimEnabled {
		go x.reclaimer.Start(ctx)
	}

	return nil
}

func (x *XStream) Stop(ctx context.Context) error {
	if err := x.listener.Stop(ctx); err != nil {
		return err
	}

	if err := x.writer.Stop(ctx); err != nil {
		return err
	}

	if err := x.reclaimer.Stop(ctx); err != nil {
		return err
	}

	return x.conn.Close()
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
