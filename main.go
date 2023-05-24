package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

const PayloadKey = "payload"

type Handler func(payload string) error

type Config struct {
	group       string
	consumer    string
	readTimeout time.Duration
	streams     []string
	redis       *redis.Options
	handlers    map[string]Handler
}

type Connection struct {
	config *Config

	read    *redis.Client
	write   *redis.Client
	reclaim *redis.Client
}

func NewConnection(config *Config) *Connection {
	if config.handlers == nil {
		config.handlers = map[string]Handler{}
	}

	return &Connection{
		config:  config,
		read:    redis.NewClient(config.redis),
		write:   redis.NewClient(config.redis),
		reclaim: redis.NewClient(config.redis),
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

func (conn *Connection) Start(ctx context.Context) error {
	if err := conn.Ping(ctx); err != nil {
		return err
	}

	if err := conn.ensureGroupsExists(ctx); err != nil {
		return err
	}

	conn.Listen(ctx)

	return nil
}

func (conn *Connection) Stop() error {
	if err := conn.read.Close(); err != nil {
		return err
	}

	if err := conn.write.Close(); err != nil {
		return err
	}

	return conn.reclaim.Close()
}

func (conn *Connection) Emit(ctx context.Context, event string, payload string) error {
	_, err := conn.write.XAdd(ctx, &redis.XAddArgs{
		Stream: event,
		ID:     "*",
		Values: map[string]any{PayloadKey: payload},
	}).Result()
	return err
}

func streamsReadFormat(streams []string) []string {
	result := make([]string, 0, len(streams)*2)
	for _, s := range streams {
		result = append(result, s, ">")
	}
	return result
}

func (conn *Connection) ensureGroupsExists(ctx context.Context) error {
	for _, stream := range conn.config.streams {
		err := conn.write.XGroupCreateMkStream(ctx, stream, conn.config.group, "$").Err()
		if err != nil {
			if !strings.Contains(err.Error(), "BUSYGROUP") {
				return err
			}
		} else {
			fmt.Println("group created")
		}
	}

	return nil
}

func (conn *Connection) Listen(ctx context.Context) {
	go func() {
		for {
			result := conn.read.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    conn.config.group,
				Consumer: conn.config.consumer,
				Streams:  streamsReadFormat(conn.config.streams),
				Block:    conn.config.readTimeout,
				Count:    1,
			})

			if result.Err() != nil {
				fmt.Printf("%s\n", result.String())
				panic(result.Err())
			}

			for _, v := range result.Val() {
				for _, m := range v.Messages {
					h, ok := conn.config.handlers[v.Stream]
					if !ok {
						fmt.Printf("handler for stream %s not found\n", v.Stream)
						continue
					}
					if err := h(m.Values[PayloadKey].(string)); err != nil {
						fmt.Printf("failed to process message %s", m.ID)
					} else {
						conn.write.XAck(ctx, v.Stream, conn.config.group, m.ID)
					}
				}
				println()
			}
		}
	}()
}

func (conn *Connection) On(stream string, f Handler) {
	conn.config.handlers[stream] = f
}

func main() {
	conn := NewConnection(&Config{
		group:    "test-group",
		consumer: "test-consumer",
		streams:  []string{"user.created"},
		redis: &redis.Options{
			Addr:     "localhost:6379",
			Password: "",
			DB:       0,
		},
	})

	ctx := context.Background()

	conn.On("user.created", func(payload string) error {
		if rand.Intn(2) >= 1 {
			return errors.New("oops")
		}

		println(payload)
		return nil
	})

	if err := conn.Start(ctx); err != nil {
		panic(err)
	}

	go func() {
		for {
			conn.Emit(ctx, "user.created", time.Now().String())
			<-time.After(1000 * time.Millisecond)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGABRT)
	<-sigChan

	if err := conn.Stop(); err != nil {
		panic(err)
	}
}
