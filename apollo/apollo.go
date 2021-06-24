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
	client, err := agollo.StartWithConfig(func() (*config.AppConfig, error) {
		return c, nil
	})
	extension.AddFormatParser(constant.YAML, &YamlParser{})
	if err != nil {
		return errors.Wrap(err, "NewGApollo")
	}

	agollo.SetLogger(glog.DefaultLogger())
	listener := &CustomChangeListener{}
	client.AddChangeListener(listener)
	return nil
}

type CustomChangeListener struct {
	wg sync.WaitGroup
}

func (c *CustomChangeListener) OnChange(changeEvent *storage.ChangeEvent) {
	for key, value := range changeEvent.Changes {
		viper.Set(key, value)
		glog.Infof("change key : %s ,old value %v, new value %v\n ", key, value.OldValue, value.NewValue)
	}
}

func (c *CustomChangeListener) OnNewestChange(event *storage.FullChangeEvent) {
	for key, value := range event.Changes {
		viper.Set(key, value)
		glog.Infof("apollo change key : %s ,value %v\n ", key, value)
	}
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

const ENV_DEFAULT_APOLLO_PREFIX = "APOLLO_DEFAULT_"

func InitApolloFromDefaultEnv() {
	viper.AutomaticEnv()
	namespace := viper.GetString(ENV_DEFAULT_APOLLO_PREFIX + "NAMESPACE")
	appId := viper.GetString(ENV_DEFAULT_APOLLO_PREFIX + "APPID")
	secret := viper.GetString(ENV_DEFAULT_APOLLO_PREFIX + "SECRET")
	ip := viper.GetString(ENV_DEFAULT_APOLLO_PREFIX + "IP")
	isBackup := viper.GetBool(ENV_DEFAULT_APOLLO_PREFIX + "IS_BACKUP_CONFIG`")
	backupPath := viper.GetString(ENV_DEFAULT_APOLLO_PREFIX + "BACKUP_CONFIG_PATH")
	//DOCKER_VOLUME_ROOT
	c := &config.AppConfig{
		AppID:            appId,
		Cluster:          "default",
		IP:               ip,
		NamespaceName:    namespace,
		Secret:           secret,
		IsBackupConfig:   isBackup,
		BackupConfigPath: backupPath,
	}

	glog.Infof("InitApolloFromDefaultEnv default config %+v", c)

	NewGApollo(c)
}
