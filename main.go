package main

import (
	"context"
	"errors"
	"log"
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
	group    string
	consumer string

	readTimeout          time.Duration
	reclaimEnabled       bool
	reclaimCount         int
	reclaimInterval      time.Duration
	reclaimMinIdleTime   time.Duration
	reclaimMaxDeliveries int

	streams  []string
	handlers map[string]Handler

	logger *log.Logger

	redis *redis.Options

	read    *redis.Client
	write   *redis.Client
	reclaim *redis.Client
}

type XStream struct {
	config *Config
}

func NewXStream(config *Config) *XStream {
	if config.handlers == nil {
		config.handlers = map[string]Handler{}
	}

	if config.logger == nil {
		config.logger = log.Default()
	}

	if config.read == nil {
		config.read = redis.NewClient(config.redis)
	}

	if config.write == nil {
		config.write = redis.NewClient(config.redis)
	}

	if config.reclaim == nil {
		config.reclaim = redis.NewClient(config.redis)
	}

	return &XStream{
		config: config,
	}
}

func (x *XStream) Ping(ctx context.Context) error {
	if err := x.config.read.Ping(ctx).Err(); err != nil {
		return err
	}

	if err := x.config.write.Ping(ctx).Err(); err != nil {
		return err
	}

	return x.config.reclaim.Ping(ctx).Err()
}

func (x *XStream) Start(ctx context.Context) error {
	if err := x.Ping(ctx); err != nil {
		return err
	}

	if err := x.ensureGroupsExists(ctx); err != nil {
		return err
	}

	go x.listenLoop(ctx)
	if x.config.reclaimEnabled {
		go x.reclaimLoop(ctx)
	}

	return nil
}

func (x *XStream) Stop() error {
	if err := x.config.read.Close(); err != nil {
		return err
	}

	if err := x.config.write.Close(); err != nil {
		return err
	}

	return x.config.reclaim.Close()
}

func (x *XStream) Emit(ctx context.Context, event string, payload string) error {
	return x.config.write.XAdd(ctx, &redis.XAddArgs{
		Stream: event,
		ID:     "*",
		Values: map[string]any{PayloadKey: payload},
	}).Err()
}

func (x *XStream) On(stream string, f Handler) {
	x.config.handlers[stream] = f
}

func (x *XStream) OnDLQ(stream string, f Handler) {
	x.config.handlers[dlqFormat(stream)] = f
}

func (x *XStream) listenLoop(ctx context.Context) error {
	for {
		streams, err := x.config.read.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    x.config.group,
			Consumer: x.config.consumer,
			Streams:  streamsReadFormat(x.config.streams),
			Block:    x.config.readTimeout,
			Count:    1,
		}).Result()

		if err != nil {
			return err
		}

		for _, v := range streams {
			for _, m := range v.Messages {
				x.process(ctx, v.Stream, m)
			}
		}
	}
}

func (x *XStream) ensureGroupsExists(ctx context.Context) error {
	for _, stream := range x.config.streams {
		err := x.config.write.XGroupCreateMkStream(ctx, stream, x.config.group, "$").Err()
		if err != nil {
			if !strings.Contains(err.Error(), "BUSYGROUP") {
				return err
			}
		} else {
			x.config.logger.Println("group created for stream " + stream)
		}
	}

	return nil
}

func (x *XStream) process(ctx context.Context, stream string, m redis.XMessage) error {
	h, ok := x.config.handlers[stream]
	if !ok {
		x.config.logger.Println("no handlers for %s" + stream)
		return nil
	}

	if err := h(m.Values[PayloadKey].(string)); err != nil {
		x.config.logger.Println("failed to process message " + m.ID)
		return err
	}

	return x.config.write.XAck(ctx, stream, x.config.group, m.ID).Err()
}

func (x *XStream) reclaimLoop(ctx context.Context) error {
	for range time.Tick(x.config.reclaimInterval) {
		for _, stream := range x.config.streams {
			messages, _, err := x.config.read.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   stream,
				Group:    x.config.group,
				Consumer: x.config.consumer,
				MinIdle:  x.config.reclaimMinIdleTime,
				Start:    "0",
				Count:    int64(x.config.reclaimCount),
			}).Result()

			if err != nil {
				return err
			}

			for _, m := range messages {
				isDead, err := x.handleDead(ctx, stream, m)
				if err != nil {
					return err
				}
				if !isDead {
					x.process(ctx, stream, m)
				}
			}
		}
	}
	return nil
}

func (x *XStream) handleDead(ctx context.Context, stream string, m redis.XMessage) (bool, error) {
	result, err := x.config.reclaim.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  x.config.group,
		Start:  m.ID,
		End:    m.ID,
		Count:  1,
	}).Result()

	if err != nil {
		return false, err
	}

	if result[0].RetryCount < int64(x.config.reclaimMaxDeliveries) {
		return false, nil
	}

	if err := x.config.reclaim.XAck(ctx, stream, x.config.group, m.ID).Err(); err != nil {
		return false, err
	}

	if err := x.config.write.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqFormat(stream),
		ID:     "*",
		Values: map[string]any{PayloadKey: m.Values[PayloadKey]},
	}).Err(); err != nil {
		return false, err
	}

	if h, ok := x.config.handlers[dlqFormat(stream)]; ok {
		if err := h(m.Values[PayloadKey].(string)); err != nil {
			x.config.logger.Println("failed to process dead message " + m.ID)
		}
	}

	return true, nil
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

func main() {
	x := NewXStream(&Config{
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

	x.On("user.created", func(payload string) error {
		if rand.Intn(10) == 0 {
			return errors.New("oops")
		}

		println(payload)
		return nil
	})

	x.OnDLQ("user.created", func(payload string) error {
		println("dead message: " + payload)
		return nil
	})

	if err := x.Start(ctx); err != nil {
		panic(err)
	}

	go func() {
		for {
			x.Emit(ctx, "user.created", time.Now().String())
			<-time.After(100 * time.Millisecond)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGABRT)
	<-sigChan

	if err := x.Stop(); err != nil {
		panic(err)
	}
}
