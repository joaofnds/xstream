package main

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joaofnds/xstream/config"

	"github.com/redis/go-redis/v9"
)

func main() {
	x := NewXStream(&config.Config{
		Group:                "test-Group",
		Consumer:             "test-consumer",
		Streams:              []string{"user.created"},
		ReclaimEnabled:       true,
		ReclaimInterval:      1000 * time.Millisecond,
		ReclaimCount:         100,
		ReclaimMinIdleTime:   5000 * time.Millisecond,
		ReclaimMaxDeliveries: 3,
		Redis: &redis.Options{
			Addr:     "localhost:6379",
			Password: "",
			DB:       0,
		},
	})

	ctx := context.Background()

	x.On("user.created", func(id, payload string) error {
		// <-time.After(2000 * time.Millisecond)
		if rand.Intn(10) == 0 {
			return errors.New("oops")
		}

		println("> " + time.Now().String())
		return nil
	})

	x.OnDLQ("user.created", func(id, payload string) error {
		println("- " + payload)
		return nil
	})

	if err := x.Start(ctx); err != nil {
		panic(err)
	}

	go func() {
		for t := range time.Tick(1000 * time.Millisecond) {
			x.writer.Emit(ctx, "user.created", t.String())
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGABRT)
	<-sigChan

	if err := x.Stop(ctx); err != nil {
		panic(err)
	}
}
