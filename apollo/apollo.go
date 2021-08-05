package apollo

import (
	"strings"
	"sync"

	"github.com/zouyx/agollo/v4/constant"
	"github.com/zouyx/agollo/v4/extension"

	"github.com/gw123/glog"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/zouyx/agollo/v4"
	"github.com/zouyx/agollo/v4/env/config"
	"github.com/zouyx/agollo/v4/storage"
)

func NewGApollo(c *config.AppConfig) error {
	agollo.SetLogger(glog.DefaultLogger())
	client, err := agollo.StartWithConfig(func() (*config.AppConfig, error) {
		return c, nil
	})
	extension.AddFormatParser(constant.YAML, &YamlParser{})
	if err != nil {
		return errors.Wrap(err, "NewGApollo")
	}
	listener := &CustomChangeListener{}
	client.AddChangeListener(listener)
	listener.Wait()
	return nil
}

type CustomChangeListener struct {
	wg   sync.WaitGroup
	once sync.Once
}

func (c *CustomChangeListener) Wait() {
	c.wg.Add(1)
	c.wg.Wait()
}

func (c *CustomChangeListener) OnChange(changeEvent *storage.ChangeEvent) {
	for key, value := range changeEvent.Changes {
		viper.Set(key, value)
	}

	c.once.Do(func() {
		c.wg.Done()
	})
}

func (c *CustomChangeListener) OnNewestChange(event *storage.FullChangeEvent) {
	for key, value := range event.Changes {
		viper.Set(key, value)
	}
	c.once.Do(func() {
		c.wg.Done()
	})
}

type YamlParser struct{}

func (p *YamlParser) Parse(configContent interface{}) (map[string]interface{}, error) {
	tmpViper := viper.New()
	tmpViper.SetConfigType("yaml")
	content, ok := configContent.(string)
	if !ok {
		return nil, errors.New("parse configContent not string")
	}

	// 从buf中读取配置信息
	err := tmpViper.ReadConfig(strings.NewReader(content))
	if err != nil {
		return nil, errors.Wrap(err, "viper.ReadConfig")
	}
	result := make(map[string]interface{})
	keys := tmpViper.AllKeys()
	for _, key := range keys {
		result[key] = tmpViper.Get(key)
	}
	return result, nil
}

const ENV_DEFAULT_APOLLO_PREFIX = "APOLLO_"

func InitApolloFromDefaultEnv(appName string) {
	appName = strings.ToUpper(appName)
	prefix := ENV_DEFAULT_APOLLO_PREFIX + appName + "_"
	viper.AutomaticEnv()
	namespace := viper.GetString(prefix + "NAMESPACE")
	appId := viper.GetString(prefix + "APPID")
	secret := viper.GetString(prefix + "SECRET")
	ip := viper.GetString(prefix + "IP")
	isBackup := viper.GetBool(prefix + "IS_BACKUP_CONFIG`")
	backupPath := viper.GetString(prefix + "BACKUP_CONFIG_PATH")

	c := &config.AppConfig{
		AppID:            appId,
		Cluster:          "default",
		IP:               ip,
		NamespaceName:    namespace,
		Secret:           secret,
		IsBackupConfig:   isBackup,
		BackupConfigPath: backupPath,
	}

	glog.DefaultLogger().Infof("InitApolloFromDefaultEnv default config %+v", c)

	NewGApollo(c)
}
