package redis_kits

import (
	"errors"
	"fmt"
	"time"
)

//检查锁是否存在
func ExistLock(lockName string) (bool,error) {
	client,err := GetClient()
	if err != nil {
		return false,err
	}
	key := fmt.Sprintf("LOCK:ID:%s",lockName)
	return client.Exists(key)
}

//获取Redis锁
func GetLock(lockName string,timeout time.Duration) error {
	client,err := GetClient()
	if err != nil {
		return err
	}

	key := fmt.Sprintf("LOCK:ID:%s",lockName)
	exists,err := client.Exists(key)
	if err != nil {
		return err
	}
	if exists {
		return errors.New("Lock exists")
	}

	if err := client.SetNX(key,"",timeout); err != nil {
		return err
	}
	return nil
}

//释放Redis锁
func ReleaseLock(lockName string) error {
	client,err := GetClient()
	if err != nil {
		return err
	}
	key := fmt.Sprintf("LOCK:ID:%s",lockName)
	if err != nil {
		return err
	}
	return client.Delete(key)
}