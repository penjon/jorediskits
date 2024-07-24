package redis_kits

import (
	"fmt"
)

func getCacheName(name string) string {
	return fmt.Sprintf("REDIS:CACHE:%s", name)
}
func getCacheSwapName(name string) string {
	return fmt.Sprintf("REDIS:CACHE-SWAP:%s", name)
}
func getCacheLockName(name string) string {
	return fmt.Sprintf("REDIS:CACHE-LOCK:%s", name)
}

type cache struct {
	cacheName string
}

type cacheManager struct {
	caches map[string]*cache
}

//缓存数据
func (i *cache) Push(client RedisClient, key string, value interface{}) error {
	lockName := getCacheLockName(i.cacheName)
	listName := getCacheName(i.cacheName)
	exists, err := client.Exists(lockName)
	if nil != err {
		return err
	}
	if exists {
		//缓存同步锁存在
		listName = getCacheSwapName(i.cacheName)
	}

	return client.SetHash(listName, key, value)
}

//缓存上锁
func (i *cache) Lock(client RedisClient) error {
	lockName := getCacheLockName(i.cacheName)
	exists, err := client.Exists(lockName)
	if nil != err {
		return err
	}
	if exists {
		//缓存同步锁存在
		return nil
	}
	return client.SetNX(lockName, "", 0)
}

func (i *cache) Unlock(client RedisClient) error {
	lockName := getCacheLockName(i.cacheName)
	return client.Delete(lockName)
}

func (i *cache) PopAll(client RedisClient, data map[string]string) error {
	swapListName := getCacheSwapName(i.cacheName)
	listName := getCacheName(i.cacheName)

	//获取交换队列数据
	result, err := client.GetHashAll(swapListName)
	if err != nil {
		return err
	}
	if nil != result {
		for k, v := range result {
			data[k] = v
		}
	}

	if err = client.Delete(swapListName); err != nil {
		return err
	}

	if err = i.Lock(client); err != nil {
		return nil
	}

	defer func() {
		i.Unlock(client)
	}()

	result, err = client.GetHashAll(listName)
	if err != nil {
		return err
	}
	if nil != result {
		for k, v := range result {
			data[k] = v
		}
	}

	return client.Delete(listName)
}

var cm *cacheManager

func GetCacheMgr() *cacheManager {
	if nil == cm {
		cm = &cacheManager{caches: make(map[string]*cache)}
	}
	return cm
}

func (i *cacheManager) GetCache(name string) *cache {
	c, exists := i.caches[name]
	if !exists {
		c = &cache{
			name,
		}
		i.caches[name] = c
	}
	return c
}
