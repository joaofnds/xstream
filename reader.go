package main

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Reader struct {
	config *Config
	conn   *Connection
}

func NewReader(config *Config, conn *Connection) *Reader {
	return &Reader{config: config, conn: conn}
}

func (l *Reader) Start(ctx context.Context) error {
	for {
		streams, err := l.conn.read.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    l.config.group,
			Consumer: l.config.consumer,
			Streams:  streamsReadFormat(l.config.streams),
			Block:    l.config.readTimeout,
			Count:    1,
		}).Result()

		if err != nil {
			return err
		}

		for _, s := range streams {
			for _, m := range s.Messages {
				l.process(ctx, s.Stream, m)
			}
		}
	}
}

func (l *Reader) Stop(context.Context) error {
	return nil
}

func (l *Reader) process(ctx context.Context, stream string, msg redis.XMessage) error {
	if err := l.config.callHandler(stream, msg); err != nil {
		return err
	}

	return l.conn.write.XAck(ctx, stream, l.config.group, msg.ID).Err()
}
