package redis_kits

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"strconv"
	"time"
)

type redisStandard struct {
	client *redis.Client
}

var c RedisClient

func GetClient() (RedisClient, error) {
	if c == nil {
		cfg, err := GetConfig()
		if err != nil {
			return nil, err
		}

		if len(cfg.clusterAddress) != 0 {
			c = &redisCluster{
				client: redis.NewClusterClient(&redis.ClusterOptions{
					Addrs:        cfg.clusterAddress,
					MaxRetries:   3,
					PoolSize:     cfg.poolSize,
					MinIdleConns: cfg.minIdle,
				}),
			}
		} else {
			c = &redisStandard{client: redis.NewClient(&redis.Options{
				Addr:         fmt.Sprintf("%s:%d", cfg.address, cfg.port),
				Password:     cfg.password,
				DB:           cfg.database,
				MaxRetries:   3,
				PoolSize:     cfg.poolSize,
				MinIdleConns: cfg.minIdle,
			})}
		}
	}
	return c, nil
}

var clients = make(map[int]RedisClient)

func GetClientByIndex(index int) (RedisClient, error) {
	client, ok := clients[index]
	if ok {
		return client, nil
	}

	cfg, err := GetConfig()
	if err != nil {
		return nil, err
	}

	client = &redisStandard{client: redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", cfg.address, cfg.port),
		Password:     cfg.password,
		DB:           index,
		MaxRetries:   3,
		PoolSize:     cfg.poolSize,
		MinIdleConns: cfg.minIdle,
	})}

	clients[index] = client
	return client, nil
}

func (c *redisStandard) GetKeys(keyLike string) ([]string, error) {
	return c.client.Keys(keyLike).Result()
}

func (c *redisStandard) Set(key string, value interface{}, timeout time.Duration) error {
	if _, err := c.client.Set(key, value, timeout).Result(); err != nil {
		return err
	}
	return nil
}

func (c *redisStandard) SetNX(key string, value interface{}, timeout time.Duration) error {
	if _, err := c.client.SetNX(key, value, timeout).Result(); err != nil {
		return err
	}
	return nil
}

func (c *redisStandard) FlushAll() error {

	_, err := c.client.FlushAll().Result()
	return err
}

func (c *redisStandard) FlushDB() error {
	_, err := c.client.FlushDB().Result()
	return err
}

func (c *redisStandard) Delete(key ...string) error {
	if _, err := c.client.Del(key...).Result(); err != nil {
		return err
	}
	return nil
}

func (c *redisStandard) Incr(key string) (int64, error) {
	return c.client.Incr(key).Result()
}

func (c *redisStandard) IncrAtExpire(key string, dur time.Duration) (int64, error) {
	i, err := c.Incr(key)
	if err != nil {
		return -1, err
	}

	if _, err := c.Expire(key, dur); err != nil {
		return -1, err
	}
	return i, nil
}

func (c *redisStandard) RPush(key string, value interface{}) error {
	_, err := c.client.RPush(key, value).Result()
	return err
}
func (c *redisStandard) LPush(key string, value interface{}) error {
	_, err := c.client.LPush(key, value).Result()
	return err
}

func (c *redisStandard) LTrim(key string, start int64, end int64) error {
	_, err := c.client.LTrim(key, start, end).Result()
	return err
}

func (c *redisStandard) Subscribe(channel string) *redis.PubSub {
	return c.client.Subscribe(channel)
}

func (c *redisStandard) Publish(channel string, value interface{}) error {
	return c.client.Publish(channel, value).Err()
}

func (c *redisStandard) Get(key string) string {
	value, err := c.client.Get(key).Result()
	if nil != err {
		return ""
	}
	return value
}

func (c *redisStandard) Ping() error {
	_, err := c.client.Ping().Result()
	return err
}

func (c *redisStandard) Exists(key string) (bool, error) {
	i, err := c.client.Exists(key).Result()
	if err == redis.Nil {
		return false, nil
	}
	return i == 1, nil
}

func (c *redisStandard) Expire(key string, duration time.Duration) (bool, error) {
	return c.client.Expire(key, duration).Result()
}

func (c *redisStandard) Pull(key string) ([]string, error) {
	len, err := c.client.LLen(key).Result()
	if err != nil {
		return nil, err
	}
	if len > 0 {
		values, err := c.client.LRange(key, 0, len).Result()
		if err != nil {
			return nil, err
		}
		return values, nil
	}
	return nil, nil
}
func (c *redisStandard) Pop(key string) ([]string, error) {
	len, err := c.client.LLen(key).Result()
	if err != nil {
		return nil, err
	}
	if len > 0 {
		values, err := c.client.LRange(key, 0, len).Result()
		if err != nil {
			return nil, err
		}
		result, err := c.client.Del(key).Result()
		if err != nil {
			return nil, err
		}
		if result != 1 {
			return nil, errors.New("result invalid")
		}
		return values, nil
	}
	return nil, nil
}

func (c *redisStandard) SetHash(key string, field string, value interface{}) error {
	_, err := c.client.HSet(key, field, value).Result()
	if err != nil {
		return err
	}
	return nil
}
func (c *redisStandard) GetHash(key string, field string) (string, error) {
	return c.client.HGet(key, field).Result()
}

func (c *redisStandard) GetHashAll(key string) (map[string]string, error) {
	return c.client.HGetAll(key).Result()
}

func (c *redisStandard) GetHashAllMapKey(key string) ([]string, error) {
	return c.client.HKeys(key).Result()
}

func (c *redisStandard) HashDelete(key string, field string) (int64, error) {
	return c.client.HDel(key, field).Result()
}

func (c *redisStandard) BatchSet(keys []string, value []interface{}, expire int) error {
	p := c.client.Pipeline()
	expired := time.Duration(expire) * time.Second
	for i, k := range keys {
		p.Set(k, value[i], expired)
	}

	_, err := p.Exec()
	if err != nil {
		return err
	}
	return nil
}

func (c *redisStandard) GetRaw() redis.Cmdable {
	return c.client
}

func (c *redisStandard) ZAdd(key string, uuid string, score float64) error {
	_, err := c.client.Do("ZADD", key, score, uuid).Result()
	return err
}

func (c *redisStandard) ZRevRank(key string, uuid string) (int64, error) {
	return c.client.ZRevRank(key, uuid).Result()
}

func (c *redisStandard) ZRank(key string, uuid string) (int64, error) {
	return c.client.ZRank(key, uuid).Result()
}

func (c *redisStandard) ZScore(key string, uuid string) (float64, error) {
	return c.client.ZScore(key, uuid).Result()
}

func (c *redisStandard) ZIncrBy(key string, scoreInc float64, uuid string) (float64, error) {
	return c.client.ZIncrBy(key, scoreInc, uuid).Result()
}

func (c *redisStandard) ZRangeByScoreWithScores(key string, minScore float64, maxScore float64) ([]redis.Z, error) {
	op := redis.ZRangeBy{
		Min: strconv.FormatFloat(minScore, 'E', -1, 64),
		Max: strconv.FormatFloat(maxScore, 'E', -1, 64),
	}
	return c.client.ZRangeByScoreWithScores(key, op).Result()
}

func (c *redisStandard) ZRevRangeByScoreWithScores(key string, minScore float64, maxScore float64) ([]redis.Z, error) {
	op := redis.ZRangeBy{
		Min: strconv.FormatFloat(minScore, 'E', -1, 64),
		Max: strconv.FormatFloat(maxScore, 'E', -1, 64),
	}
	return c.client.ZRevRangeByScoreWithScores(key, op).Result()
}

func (c *redisStandard) ZRangeWithScores(key string, minRank int64, maxRank int64) ([]redis.Z, error) {
	return c.client.ZRangeWithScores(key, minRank, maxRank).Result()
}

func (c *redisStandard) ZRevRangeWithScores(key string, minRank int64, maxRank int64) ([]redis.Z, error) {
	return c.client.ZRevRangeWithScores(key, minRank, maxRank).Result()
}

func (c *redisStandard) ZRemRangeByRank(key string, minRank int64, maxRank int64) (int64, error) {
	return c.client.ZRemRangeByRank(key, minRank, maxRank).Result()
}

func (c *redisStandard) ZRem(key string, members ...interface{}) (int64, error) {
	return c.client.ZRem(key, members).Result()
}

func (c *redisStandard) ZRemRangeByScore(key string, min string, max string) (int64, error) {
	return c.client.ZRemRangeByScore(key, min, max).Result()
}

func (c *redisStandard) SetsAdd(key string, value interface{}) error {
	_, err := c.client.SAdd(key, value).Result()
	return err
}

func (c *redisStandard) SetsDel(key string, value interface{}) error {
	_, err := c.client.SRem(key, value).Result()
	return err
}

func (c *redisStandard) SetsCard(key string) (int64, error) {
	return c.client.SCard(key).Result()
}

func (c *redisStandard) SetsMembers(key string) ([]string, error) {
	return c.client.SMembers(key).Result()
}

func (c *redisStandard) SetsExistMember(key string, member string) (bool, error) {
	return c.client.SIsMember(key, member).Result()
}

func (c *redisStandard) Scan(cursor uint64, key string, count int64) ([]string, uint64, error) {
	return c.client.Scan(cursor, key, count).Result()
}
