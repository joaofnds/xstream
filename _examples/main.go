package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joaofnds/xstream"
	"github.com/joaofnds/xstream/config"
)

func main() {
	x := xstream.NewXStream(&config.Config{
		Group:                "test-Group",
		Consumer:             "test-consumer",
		Streams:              []string{"user.created"},
		ReclaimEnabled:       true,
		ReclaimInterval:      1000 * time.Millisecond,
		ReclaimCount:         100,
		ReclaimMinIdleTime:   5000 * time.Millisecond,
		ReclaimMaxDeliveries: 3,
		Redis: &config.RedisOptions{
			Addr:     "localhost:6379",
			Password: "",
			DB:       0,
		},
	})

	x.On("user.created", func(id, payload string) error {
		println("> " + payload)
		return nil
	})

	x.OnDLQ("user.created", func(id, payload string) error {
		println("- " + payload)
		return nil
	})

	ctx := context.Background()
	if err := x.Start(ctx); err != nil {
		panic(err)
	}

	go func() {
		for t := range time.Tick(1000 * time.Millisecond) {
			x.Emit(ctx, "user.created", t.String())
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGABRT)
	<-sigChan

	if err := x.Stop(ctx); err != nil {
		panic(err)
	}
}
