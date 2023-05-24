package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Reclaimer struct {
	config *Config
	client *redis.Client
}

func NewReclaimer(config *Config) *Reclaimer {
	return &Reclaimer{config: config, client: redis.NewClient(config.redis)}
}

func (r *Reclaimer) Start(ctx context.Context) error {
	for range time.Tick(r.config.reclaimInterval) {
		for _, stream := range r.config.streams {
			messages, _, err := r.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   stream,
				Group:    r.config.group,
				Consumer: r.config.consumer,
				MinIdle:  r.config.reclaimMinIdleTime,
				Start:    "0",
				Count:    int64(r.config.reclaimCount),
			}).Result()

			if err != nil {
				return err
			}

			for _, m := range messages {
				isDead, err := r.handleDead(ctx, stream, m)
				if err != nil {
					return err
				}

				if !isDead {
					r.process(ctx, stream, m)
				}
			}
		}
	}
	return nil
}

func (r *Reclaimer) Stop(context.Context) error {
	return nil
}

func (r *Reclaimer) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r *Reclaimer) handleDead(ctx context.Context, stream string, m redis.XMessage) (bool, error) {
	result, err := r.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  r.config.group,
		Start:  m.ID,
		End:    m.ID,
		Count:  1,
	}).Result()

	if err != nil {
		return false, err
	}

	if len(result) == 0 || result[0].RetryCount < int64(r.config.reclaimMaxDeliveries) {
		return false, nil
	}

	if err := r.client.XAck(ctx, stream, r.config.group, m.ID).Err(); err != nil {
		return false, err
	}

	if err := r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqFormat(stream),
		ID:     "*",
		Values: map[string]any{PayloadKey: m.Values[PayloadKey]},
	}).Err(); err != nil {
		return false, err
	}

	if err := r.config.callHandler(dlqFormat(stream), m); err != nil {
		r.config.logger.Println("failed to process dead message " + m.ID)
	}

	return true, nil
}

func (r *Reclaimer) process(ctx context.Context, stream string, msg redis.XMessage) error {
	if err := r.config.callHandler(stream, msg); err != nil {
		return err
	}

	return r.client.XAck(ctx, stream, r.config.group, msg.ID).Err()
}
