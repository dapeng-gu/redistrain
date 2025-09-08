package redisengine

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type RedisEngine struct {
	client      redis.UniversalClient
	engine_name string
}

func NewRedisEngine(client redis.UniversalClient, engine_name string) *RedisEngine {
	return &RedisEngine{client: client, engine_name: engine_name}
}

func (engine *RedisEngine) GetName() string {
	return engine.engine_name
}

func (engine *RedisEngine) RPop(ctx context.Context, queueKey string) (string, error) {
	return engine.client.RPop(ctx, queueKey).Result()
}

func (engine *RedisEngine) RunScript(ctx context.Context, script *redis.Script, keys []string, args ...interface{}) (interface{}, error) {
	return script.Run(ctx, engine.client, keys, args).Result()
}

// ZSet操作方法
func (engine *RedisEngine) ZAdd(ctx context.Context, key string, score float64, member string) error {
	return engine.client.ZAdd(ctx, key, redis.Z{
		Score:  score,
		Member: member,
	}).Err()
}

func (engine *RedisEngine) ZIncrBy(ctx context.Context, key string, increment float64, member string) (float64, error) {
	return engine.client.ZIncrBy(ctx, key, increment, member).Result()
}

// 升序
func (engine *RedisEngine) ZRangeByScore(ctx context.Context, key string, min, max string, offset, count int64) ([]string, error) {
	return engine.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: offset,
		Count:  count,
	}).Result()
}

// 降序
func (engine *RedisEngine) ZRevRangeByScore(ctx context.Context, key string, max, min string, offset, count int64) ([]string, error) {
	return engine.client.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: offset,
		Count:  count,
	}).Result()
}

// 弹出单个元素
func (engine *RedisEngine) ZPopMinOne(ctx context.Context, key string) (score float64, member interface{}, err error) {
	result, err := engine.client.ZPopMin(ctx, key, 1).Result()
	if err != nil {
		return 0, nil, err
	}
	if len(result) == 0 {
		return 0, nil, redis.Nil
	}
	return result[0].Score, result[0].Member, nil
}

func (engine *RedisEngine) ZPopMaxOne(ctx context.Context, key string) (score float64, member interface{}, err error) {
	result, err := engine.client.ZPopMax(ctx, key, 1).Result()
	if err != nil {
		return 0, nil, err
	}
	if len(result) == 0 {
		return 0, nil, redis.Nil
	}
	return result[0].Score, result[0].Member, nil
}

// 弹出多个元素
func (engine *RedisEngine) ZPopMin(ctx context.Context, key string, count int64) ([]redis.Z, error) {
	return engine.client.ZPopMin(ctx, key, count).Result()
}

func (engine *RedisEngine) ZPopMax(ctx context.Context, key string, count int64) ([]redis.Z, error) {
	return engine.client.ZPopMax(ctx, key, count).Result()
}

// Hash操作方法
func (engine *RedisEngine) HSet(ctx context.Context, key string, values ...interface{}) error {
	return engine.client.HSet(ctx, key, values...).Err()
}

func (engine *RedisEngine) HGet(ctx context.Context, key, field string) (string, error) {
	return engine.client.HGet(ctx, key, field).Result()
}

func (engine *RedisEngine) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return engine.client.HGetAll(ctx, key).Result()
}

func (engine *RedisEngine) HIncrBy(ctx context.Context, key, field string, incr int64) (int64, error) {
	return engine.client.HIncrBy(ctx, key, field, incr).Result()
}

func (engine *RedisEngine) HIncrByBatch(ctx context.Context, key string, fieldIncrements map[string]int64) (map[string]int64, error) {
	pipe := engine.client.Pipeline()
	cmds := make(map[string]*redis.IntCmd)

	for field, incr := range fieldIncrements {
		cmds[field] = pipe.HIncrBy(ctx, key, field, incr)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}

	results := make(map[string]int64)
	for field, cmd := range cmds {
		val, err := cmd.Result()
		if err != nil {
			return nil, err
		}
		results[field] = val
	}

	return results, nil
}
