package config

import (
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"fmt"
	"sync"
)

type Job struct {
	Name  string
	Table string
}

type Config struct {
	Jobs []Job
}

type ConfigManager struct {
	jobmap map[string]string
	mu     sync.Mutex
	config Config
}

func NewConfigManager() *ConfigManager {

	t := &ConfigManager{
		jobmap: make(map[string]string, 0),
		config: Config{},
	}
	return t
}

func (c *ConfigManager) GetJobMap() map[string]string {
	return c.jobmap
}

func (c *ConfigManager) Load() error {

	if c == nil {
		return fmt.Errorf("null ConfigManager")
	}

	viper.SetConfigName("config")
	viper.AddConfigPath("/etc")
	if err := viper.ReadInConfig(); err == nil {
		fmt.Printf("successful parse the config file : %s\n", viper.ConfigFileUsed())
	}
	var config Config
	err := viper.Unmarshal(&config)
	if err != nil {
		return fmt.Errorf("unable to decode into struct,%v\n", err)
	}
	c.mu.Lock()
	for k, v := range config.Jobs {
		fmt.Printf("%v,%v,%v\n", k, v.Name, v.Table)
		c.jobmap[v.Name] = v.Table
	}
	c.mu.Unlock()

	//config xml 有改动，需要重做结构体
	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {

		var config Config
		err := viper.Unmarshal(&config)
		if err != nil {
			fmt.Printf("unable to decode into struct,%v\n", err)
		}
		c.mu.Lock()
		for k, v := range config.Jobs {
			fmt.Printf("%v,%v,%v", k, v.Name, v.Table)
			c.jobmap[v.Name] = v.Table
		}
		c.mu.Unlock()
	})

	return nil
}
