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
	group                string
	consumer             string
	readTimeout          time.Duration
	reclaimEnabled       bool
	reclaimCount         int
	reclaimInterval      time.Duration
	reclaimMinIdleTime   time.Duration
	reclaimMaxDeliveries int
	streams              []string
	redis                *redis.Options
	handlers             map[string]Handler
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
	conn.ReclaimLoop(ctx)

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

func dlqFormat(stream string) string {
	return "dead:" + stream
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

func (conn *Connection) Process(ctx context.Context, stream string, m redis.XMessage) error {
	h, ok := conn.config.handlers[stream]
	if !ok {
		fmt.Printf("handler for stream %s not found\n", stream)
		return nil
	}

	if err := h(m.Values[PayloadKey].(string)); err != nil {
		fmt.Printf("failed to process message %s\n", m.ID)
		return err
	}

	return conn.write.XAck(ctx, stream, conn.config.group, m.ID).Err()
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
				panic(result.Err())
			}

			for _, v := range result.Val() {
				for _, m := range v.Messages {
					conn.Process(ctx, v.Stream, m)
				}
			}
		}
	}()
}

func (conn *Connection) On(stream string, f Handler) {
	conn.config.handlers[stream] = f
}

func (conn *Connection) OnDlq(stream string, f Handler) {
	conn.config.handlers[dlqFormat(stream)] = f
}

func (conn *Connection) ReclaimLoop(ctx context.Context) {
	if !conn.config.reclaimEnabled {
		return
	}

	go func() {
		for range time.Tick(conn.config.reclaimInterval) {
			for _, stream := range conn.config.streams {
				result := conn.read.XAutoClaim(ctx, &redis.XAutoClaimArgs{
					Stream:   stream,
					Group:    conn.config.group,
					Consumer: conn.config.consumer,
					MinIdle:  conn.config.reclaimMinIdleTime,
					Start:    "0",
					Count:    int64(conn.config.reclaimCount),
				})

				if result.Err() != nil {
					panic(result.Err().Error())
				}

				msgs, _ := result.Val()
				for _, m := range msgs {
					isDead := conn.handleDead(ctx, stream, m)
					if !isDead {
						conn.Process(ctx, stream, m)
					}
				}
			}
		}
	}()
}

func (conn *Connection) handleDead(ctx context.Context, stream string, m redis.XMessage) bool {
	result, err := conn.reclaim.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  conn.config.group,
		Start:  m.ID,
		End:    m.ID,
		Count:  1,
	}).Result()
	if err != nil {
		panic(err.Error())
	}

	if result[0].RetryCount < int64(conn.config.reclaimMaxDeliveries) {
		return false
	}

	if err := conn.reclaim.XAck(ctx, stream, conn.config.group, m.ID).Err(); err != nil {
		panic(err)
	}

	if err := conn.write.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqFormat(stream),
		ID:     "*",
		Values: map[string]any{PayloadKey: m.Values[PayloadKey]},
	}).Err(); err != nil {
		panic(err)
	}

	if h, ok := conn.config.handlers[dlqFormat(stream)]; ok {
		if err := h(m.Values[PayloadKey].(string)); err != nil {
			fmt.Printf("failed to process dead message %s\n", m.ID)
		}
	}

	return true
}

func main() {
	conn := NewConnection(&Config{
		group:                "test-group",
		consumer:             "test-consumer",
		streams:              []string{"user.created"},
		reclaimEnabled:       true,
		reclaimInterval:      1 * time.Second,
		reclaimCount:         5,
		reclaimMinIdleTime:   500 * time.Millisecond,
		reclaimMaxDeliveries: 2,
		redis: &redis.Options{
			Addr:     "localhost:6379",
			Password: "",
			DB:       0,
		},
	})

	ctx := context.Background()

	conn.On("user.created", func(payload string) error {
		if rand.Intn(10) == 0 {
			return errors.New("oops")
		}

		println(payload)
		return nil
	})

	conn.OnDlq("user.created", func(payload string) error {
		println("dead message: " + payload)
		return nil
	})

	if err := conn.Start(ctx); err != nil {
		panic(err)
	}

	go func() {
		for {
			conn.Emit(ctx, "user.created", time.Now().String())
			<-time.After(100 * time.Millisecond)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGABRT)
	<-sigChan

	if err := conn.Stop(); err != nil {
		panic(err)
	}
}
