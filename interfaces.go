package redis_kits

import (
	"github.com/go-redis/redis"
	"time"
)

type RedisClient interface {
	GetKeys(keyLike string) ([]string, error)
	Set(string, interface{}, time.Duration) error
	SetNX(string, interface{}, time.Duration) error
	Delete(...string) error
	Incr(string) (int64, error)
	RPush(string, interface{}) error
	LPush(string, interface{}) error
	LTrim(string, int64, int64) error
	Pull(string) ([]string, error)
	Pop(string) ([]string, error)
	Subscribe(string) *redis.PubSub
	Publish(string, interface{}) error
	Get(string) string
	Ping() error
	Exists(string) (bool, error)
	Expire(key string, duration time.Duration) (bool, error)
	IncrAtExpire(key string, dur time.Duration) (int64, error)
	SetHash(key string, field string, value interface{}) error
	GetHash(key string, field string) (string, error)
	GetHashAll(string) (map[string]string, error)
	GetHashAllMapKey(key string) ([]string, error)
	HashDelete(key string, field string) (int64, error)
	BatchSet(keys []string, value []interface{}, expire int) error
	FlushAll() error
	FlushDB() error
	GetRaw() redis.Cmdable
	ZAdd(string, string, float64) error
	ZRevRank(string, string) (int64, error)
	ZRank(string, string) (int64, error)
	ZScore(string, string) (float64, error)
	ZIncrBy(string, float64, string) (float64, error)
	ZRangeByScoreWithScores(key string, minScore float64, maxScore float64) ([]redis.Z, error)
	ZRevRangeByScoreWithScores(key string, minScore float64, maxScore float64) ([]redis.Z, error)
	ZRangeWithScores(key string, minRank int64, maxRank int64) ([]redis.Z, error)
	ZRevRangeWithScores(key string, minRank int64, maxRank int64) ([]redis.Z, error)
	ZRemRangeByRank(key string, minRank int64, maxRank int64) (int64, error)
	ZRem(key string, members ...interface{}) (int64, error)
	ZRemRangeByScore(key string, min string, max string) (int64, error)
	SetsAdd(key string, value interface{}) error
	SetsDel(key string, value interface{}) error
	SetsCard(key string) (int64, error)
	SetsMembers(key string) ([]string, error)
	SetsExistMember(key string, member string) (bool, error)
	Scan(cursor uint64, key string, count int64) ([]string, uint64, error)
}
