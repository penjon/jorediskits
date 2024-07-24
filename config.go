package redis_kits

import (
	"os"
	"strconv"
	"strings"
)

type config struct {
	address string
	port int
	database int
	poolSize int
	minIdle int
	password string
	clusterAddress []string
}

var cfg *config
func GetConfig() (*config,error) {
	if nil == cfg {
		cfg = &config{}
		if err := cfg.Parse(); err != nil {
			return nil,err
		}
	}

	return cfg,nil
}

func (c *config) Parse() error {
	var err error = nil
	c.address = os.Getenv("REDIS_ADDRESS")
	c.password = os.Getenv("REDIS_PASSWORD")
	clusterAddr := os.Getenv("REDIS_CLUSTER_ADDRESS")
	if len(clusterAddr) != 0 {
		c.clusterAddress = strings.Split(clusterAddr,",")
	}

	if c.database,err = strconv.Atoi(os.Getenv("REDIS_DATABASE")); err != nil {
		return err
	}
	if c.port,err = strconv.Atoi(os.Getenv("REDIS_PORT")); err != nil {
		return err
	}

	if c.minIdle,err = strconv.Atoi(os.Getenv("REDIS_MIN_IDLE")); err != nil {
		c.minIdle = 5
	}

	if c.poolSize,err = strconv.Atoi(os.Getenv("REDIS_POOL_SIZE")); err != nil {
		c.poolSize = 20
	}

	if c.port == 0 {
		c.port = 6379
	}
	return nil
}