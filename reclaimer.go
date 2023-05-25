package xstream

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type reclaimer struct {
	config *Config
	client *redis.Client
}

func newReclaimer(cfg *Config) *reclaimer {
	return &reclaimer{config: cfg, client: redis.NewClient(cfg.Redis)}
}

func (r *reclaimer) start(ctx context.Context) error {
	for range time.Tick(r.config.ReclaimInterval) {
		for _, stream := range r.config.Streams {
			messages, _, err := r.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   stream,
				Group:    r.config.Group,
				Consumer: r.config.Consumer,
				MinIdle:  r.config.ReclaimMinIdleTime,
				Start:    "0",
				Count:    int64(r.config.ReclaimCount),
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
					go r.process(ctx, stream, m)
				}
			}
		}
	}
	return nil
}

func (r *reclaimer) stop(_ context.Context) error {
	return r.client.Close()
}

func (r *reclaimer) ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r *reclaimer) handleDead(ctx context.Context, stream string, m redis.XMessage) (bool, error) {
	result, err := r.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  r.config.Group,
		Start:  m.ID,
		End:    m.ID,
		Count:  1,
	}).Result()

	if err != nil {
		return false, err
	}

	if len(result) == 0 || result[0].RetryCount < int64(r.config.ReclaimMaxDeliveries) {
		return false, nil
	}

	if err := r.client.XAck(ctx, stream, r.config.Group, m.ID).Err(); err != nil {
		return false, err
	}

	if err := r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqFormat(stream),
		ID:     "*",
		Values: map[string]any{BodyKey: m.Values[BodyKey]},
	}).Err(); err != nil {
		return false, err
	}

	if err := r.config.callHandler(dlqFormat(stream), m); err != nil {
		r.config.Logger.Println("failed to process dead message " + m.ID)
	}

	return true, nil
}

func (r *reclaimer) process(ctx context.Context, stream string, msg redis.XMessage) error {
	if err := r.config.callHandler(stream, msg); err != nil {
		return err
	}

	return r.client.XAck(ctx, stream, r.config.Group, msg.ID).Err()
}
