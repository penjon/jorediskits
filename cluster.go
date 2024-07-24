package redis_kits

import (
	"errors"
	"github.com/go-redis/redis"
	"strconv"
	"time"
)

type redisCluster struct {
	client *redis.ClusterClient
}

func (c *redisCluster) GetKeys(keyLike string) ([]string, error) {
	return c.client.Keys(keyLike).Result()
}

func (c *redisCluster) Set(key string, value interface{}, timeout time.Duration) error {
	if _, err := c.client.Set(key, value, timeout).Result(); err != nil {
		return err
	}
	return nil
}

func (c *redisCluster) SetNX(key string, value interface{}, timeout time.Duration) error {
	if _, err := c.client.SetNX(key, value, timeout).Result(); err != nil {
		return err
	}
	return nil
}

func (c *redisCluster) Delete(key ...string) error {
	if _, err := c.client.Del(key...).Result(); err != nil {
		return err
	}
	return nil
}

func (c *redisCluster) Incr(key string) (int64, error) {
	return c.client.Incr(key).Result()
}

func (c *redisCluster) RPush(key string, value interface{}) error {
	_, err := c.client.RPush(key, value).Result()
	return err
}
func (c *redisCluster) LPush(key string, value interface{}) error {
	_, err := c.client.LPush(key, value).Result()
	return err
}

func (c *redisCluster) LTrim(key string, start int64, end int64) error {
	_, err := c.client.LTrim(key, start, end).Result()
	return err
}

func (c *redisCluster) Subscribe(channel string) *redis.PubSub {
	return c.client.Subscribe(channel)
}

func (c *redisCluster) Publish(channel string, value interface{}) error {
	return c.client.Publish(channel, value).Err()
}

func (c *redisCluster) Get(key string) string {
	value, err := c.client.Get(key).Result()
	if nil != err {
		return ""
	}
	return value
}

func (c *redisCluster) Ping() error {
	_, err := c.client.Ping().Result()
	return err
}

func (c *redisCluster) Exists(key string) (bool, error) {
	i, err := c.client.Exists(key).Result()
	if err != nil {
		return false, err
	}
	return i == 1, nil
}

func (c *redisCluster) Expire(key string, duration time.Duration) (bool, error) {
	return c.client.Expire(key, duration).Result()
}

func (c *redisCluster) IncrAtExpire(key string, dur time.Duration) (int64, error) {
	i, err := c.Incr(key)
	if err != nil {
		return -1, err
	}

	if _, err := c.Expire(key, dur); err != nil {
		return -1, err
	}
	return i, nil
}

func (c *redisCluster) Pull(key string) ([]string, error) {
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
func (c *redisCluster) Pop(key string) ([]string, error) {
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

func (c *redisCluster) SetHash(key string, field string, value interface{}) error {
	flag, err := c.client.HSet(key, field, value).Result()
	if err != nil {
		return err
	}

	if !flag {
		return errors.New("HSet command result is false")
	}
	return nil
}
func (c *redisCluster) GetHash(key string, field string) (string, error) {
	return c.client.HGet(key, field).Result()
}

func (c *redisCluster) FlushAll() error {
	_, err := c.client.FlushAll().Result()
	return err
}

func (c *redisCluster) FlushDB() error {
	_, err := c.client.FlushDB().Result()
	return err
}

func (c *redisCluster) GetHashAll(key string) (map[string]string, error) {
	return c.client.HGetAll(key).Result()
}

func (c *redisCluster) GetHashAllMapKey(key string) ([]string, error) {
	return c.client.HKeys(key).Result()
}

func (c *redisCluster) HashDelete(key string, field string) (int64, error) {
	return c.client.HDel(key, field).Result()
}

func (c *redisCluster) BatchSet(keys []string, value []interface{}, expire int) error {
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

func (c *redisCluster) GetRaw() redis.Cmdable {
	return c.client
}

func (c *redisCluster) ZAdd(key string, uuid string, score float64) error {
	_, err := c.client.Do("ZADD", key, score, uuid).Result()
	return err
}

func (c *redisCluster) ZRevRank(key string, uuid string) (int64, error) {
	return c.client.ZRevRank(key, uuid).Result()
}

func (c *redisCluster) ZRank(key string, uuid string) (int64, error) {
	return c.client.ZRank(key, uuid).Result()
}

func (c *redisCluster) ZScore(key string, uuid string) (float64, error) {
	return c.client.ZScore(key, uuid).Result()
}

func (c *redisCluster) ZIncrBy(key string, scoreInc float64, uuid string) (float64, error) {
	return c.client.ZIncrBy(key, scoreInc, uuid).Result()
}

func (c *redisCluster) ZRangeByScoreWithScores(key string, minScore float64, maxScore float64) ([]redis.Z, error) {
	op := redis.ZRangeBy{
		Min: strconv.FormatFloat(minScore, 'E', -1, 64),
		Max: strconv.FormatFloat(maxScore, 'E', -1, 64),
	}
	return c.client.ZRangeByScoreWithScores(key, op).Result()
}

func (c *redisCluster) ZRevRangeByScoreWithScores(key string, minScore float64, maxScore float64) ([]redis.Z, error) {
	op := redis.ZRangeBy{
		Min: strconv.FormatFloat(minScore, 'E', -1, 64),
		Max: strconv.FormatFloat(maxScore, 'E', -1, 64),
	}
	return c.client.ZRevRangeByScoreWithScores(key, op).Result()
}

func (c *redisCluster) ZRangeWithScores(key string, minRank int64, maxRank int64) ([]redis.Z, error) {
	return c.client.ZRangeWithScores(key, minRank, maxRank).Result()
}

func (c *redisCluster) ZRevRangeWithScores(key string, minRank int64, maxRank int64) ([]redis.Z, error) {
	return c.client.ZRevRangeWithScores(key, minRank, maxRank).Result()
}

func (c *redisCluster) ZRemRangeByRank(key string, minRank int64, maxRank int64) (int64, error) {
	return c.client.ZRemRangeByRank(key, minRank, maxRank).Result()
}

func (c *redisCluster) ZRem(key string, members ...interface{}) (int64, error) {
	return c.client.ZRem(key, members).Result()
}

func (c *redisCluster) ZRemRangeByScore(key string, min string, max string) (int64, error) {
	return c.client.ZRemRangeByScore(key, min, max).Result()
}

func (c *redisCluster) SetsAdd(key string, value interface{}) error {
	_, err := c.client.SAdd(key, value).Result()
	return err
}

func (c *redisCluster) SetsDel(key string, value interface{}) error {
	_, err := c.client.SRem(key, value).Result()
	return err
}

func (c *redisCluster) SetsCard(key string) (int64, error) {
	return c.client.SCard(key).Result()
}

func (c *redisCluster) SetsMembers(key string) ([]string, error) {
	return c.client.SMembers(key).Result()
}

func (c *redisCluster) SetsExistMember(key string, member string) (bool, error) {
	return c.client.SIsMember(key, member).Result()
}

func (c *redisCluster) Scan(cursor uint64, key string, count int64) ([]string, uint64, error) {
	return c.client.Scan(cursor, key, count).Result()
}
