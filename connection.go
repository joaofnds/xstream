package main

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Connection struct {
	read    *redis.Client
	write   *redis.Client
	reclaim *redis.Client
}

func NewConnection(config *redis.Options) *Connection {
	return &Connection{
		read:    redis.NewClient(config),
		write:   redis.NewClient(config),
		reclaim: redis.NewClient(config),
	}
}

func (conn *Connection) Ping(ctx context.Context) error {
	if err := conn.read.Ping(ctx).Err(); err != nil {
		return err
	}

	if err := conn.write.Ping(ctx).Err(); err != nil {
		return err
	}

	return conn.reclaim.Ping(ctx).Err()
}

func (conn *Connection) Close() error {
	if err := conn.read.Close(); err != nil {
		return err
	}

	if err := conn.write.Close(); err != nil {
		return err
	}

	return conn.reclaim.Close()
}
