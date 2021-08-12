package apollo

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/zouyx/agollo/v4/constant"
	"github.com/zouyx/agollo/v4/extension"

	"github.com/gw123/glog"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/zouyx/agollo/v4"
	"github.com/zouyx/agollo/v4/env/config"
	"github.com/zouyx/agollo/v4/storage"
)

type UnmarshalFunc func(viper *viper.Viper)
type ConfigChange func(key string, newVal, oldVal interface{})

//Config 配置文件
type Config struct {
	AppID             string `json:"appId"`
	Cluster           string `json:"cluster"`
	NamespaceName     string `json:"namespaceName"`
	IP                string `json:"ip"`
	IsBackupConfig    bool   `default:"true" json:"isBackupConfig"`
	BackupConfigPath  string `json:"backupConfigPath"`
	Secret            string `json:"secret"`
	SyncServerTimeout int    `json:"syncServerTimeout"`
	Timeout           time.Duration
	UnmarshalFunc     UnmarshalFunc
	ConfigChange      ConfigChange
}

func NewGApollo(c *Config) (*agollo.Client, error) {
	if c.Timeout == 0 {
		c.Timeout = time.Second * 12
	}

	client, err := agollo.StartWithConfig(func() (*config.AppConfig, error) {
		cfg := config.AppConfig{
			AppID:             c.AppID,
			Cluster:           c.Cluster,
			NamespaceName:     c.NamespaceName,
			IP:                c.IP,
			IsBackupConfig:    c.IsBackupConfig,
			BackupConfigPath:  c.BackupConfigPath,
			Secret:            c.Secret,
			SyncServerTimeout: c.SyncServerTimeout,
		}
		return &cfg, nil
	})

	if err != nil {
		return nil, errors.Wrap(err, "NewGApollo")
	}

	extension.AddFormatParser(constant.YAML, &YamlParser{
		UnmarshalFunc: c.UnmarshalFunc,
	})

	listener := NewCustomChangeListener(c.ConfigChange)
	client.AddChangeListener(listener)

	agollo.SetLogger(glog.DefaultLogger())
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, c.Timeout)
	if err := listener.Wait(ctx); err != nil {
		return nil, err
	}
	glog.DefaultLogger().Info("init apollo config success")
	return client, nil
}

type CustomChangeListener struct {
	wg           chan struct{}
	once         sync.Once
	ConfigChange ConfigChange
}

func NewCustomChangeListener(change ConfigChange) *CustomChangeListener {
	return &CustomChangeListener{
		wg:           make(chan struct{}, 1),
		once:         sync.Once{},
		ConfigChange: change,
	}
}

func (c *CustomChangeListener) Wait(ctx context.Context) error {
	select {
	case <-c.wg:
		return nil
	case <-ctx.Done():
		return errors.New("wait config init timeout")
	}
}

func (c *CustomChangeListener) OnChange(changeEvent *storage.ChangeEvent) {
	for key, value := range changeEvent.Changes {
		//glog.DefaultLogger().Infof("onchange key %s val %+v", key, value)
		if c.ConfigChange != nil {
			c.ConfigChange(key, value.NewValue, value.OldValue)
		}
	}
}

func (c *CustomChangeListener) OnNewestChange(event *storage.FullChangeEvent) {
	//for key, value := range event.Changes {
	//	glog.DefaultLogger().Infof("OnNewestChange key:%s \t,val:%+v", key, value)
	//}
	c.once.Do(func() {
		c.wg <- struct{}{}
	})
}

type YamlParser struct {
	UnmarshalFunc UnmarshalFunc
}

func (p *YamlParser) Parse(configContent interface{}) (map[string]interface{}, error) {
	tmpViper := viper.GetViper()
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

	if p.UnmarshalFunc != nil {
		p.UnmarshalFunc(tmpViper)
	}

	result := make(map[string]interface{})
	keys := tmpViper.AllKeys()
	for _, key := range keys {
		result[key] = tmpViper.Get(key)
	}
	return result, nil
}

const ENV_DEFAULT_APOLLO_PREFIX = "APOLLO_"

func InitApolloFromDefaultEnv(appName string, unmarshalFunc UnmarshalFunc, configChange ConfigChange) (*agollo.Client, error) {
	appName = strings.ToUpper(appName)
	prefix := ENV_DEFAULT_APOLLO_PREFIX + appName + "_"
	viper.AutomaticEnv()
	namespace := viper.GetString(prefix + "NAMESPACE")
	appId := viper.GetString(prefix + "APPID")
	secret := viper.GetString(prefix + "SECRET")
	ip := viper.GetString(prefix + "IP")
	isBackup := viper.GetBool(prefix + "IS_BACKUP_CONFIG`")
	backupPath := viper.GetString(prefix + "BACKUP_CONFIG_PATH")

	c := &Config{
		AppID:            appId,
		Cluster:          "default",
		IP:               ip,
		NamespaceName:    namespace,
		Secret:           secret,
		IsBackupConfig:   isBackup,
		BackupConfigPath: backupPath,
		UnmarshalFunc:    unmarshalFunc,
		ConfigChange:     configChange,
	}

	glog.DefaultLogger().Infof("InitApolloFromDefaultEnv default config %+v", c)

	return NewGApollo(c)
}
