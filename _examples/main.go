package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joaofnds/xstream"
)

func main() {
	x := xstream.NewXStream(&xstream.Config{
		Group:                "test-Group",
		Consumer:             "test-consumer",
		Streams:              []string{"user.created"},
		ReclaimEnabled:       true,
		ReclaimInterval:      1000 * time.Millisecond,
		ReclaimCount:         100,
		ReclaimMinIdleTime:   5000 * time.Millisecond,
		ReclaimMaxDeliveries: 3,
		Redis: &xstream.RedisOptions{
			Addr:     "localhost:6379",
			Password: "",
			DB:       0,
		},
	})

	x.On("user.created", func(msg xstream.Message) error {
		fmt.Printf("< %#v\n", msg.Body)
		return nil
	})

	x.OnDead("user.created", func(msg xstream.Message) error {
		fmt.Printf("- %#v\n", msg)
		return nil
	})

	ctx := context.Background()
	x.Start(ctx)
	defer x.Stop(ctx)

	go func() {
		for t := range time.Tick(1000 * time.Millisecond) {
			x.Emit(ctx, "user.created", xstream.Message{Body: t.String()})
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGABRT)
	<-sigChan
}
