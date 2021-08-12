package apollo

import (
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestNewGApollo(t *testing.T) {
	viper.AutomaticEnv()

	const APP_APOLLO_PREFIX = "APOLLO_EMAIL_"
	namespace := viper.GetString(APP_APOLLO_PREFIX + "NAMESPACE")
	appId := viper.GetString(APP_APOLLO_PREFIX + "APPID")
	secret := viper.GetString(APP_APOLLO_PREFIX + "SECRET")
	ip := viper.GetString(APP_APOLLO_PREFIX + "IP")

	//DOCKER_VOLUME_ROOT
	c := &Config{
		AppID:            appId,
		Cluster:          "default",
		IP:               ip,
		NamespaceName:    namespace,
		Secret:           secret,
		IsBackupConfig:   true,
		BackupConfigPath: "/tmp",
	}
	t.Logf("config %+v", c)

	NewGApollo(c)

	for {
		time.Sleep(time.Second * 5)
		t.Log("point", viper.Get("point"))
		t.Log("abc", viper.Get("abc"))
	}
}

type TestCfg struct {
	ServerIp string `json:"server_ip" yaml:"server_ip" mapstructure:"server_ip"`
	Username string `json:"username"  yaml:"username" mapstructure:"username"`
	Password string `json:"password"  yaml:"password" mapstructure:"password"`
}

func TestInitApolloFromDefaultEnv(t *testing.T) {
	cfg := &TestCfg{}
	_, err := InitApolloFromDefaultEnv("default", func(viper *viper.Viper) {
		err := viper.Unmarshal(cfg)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("cfg:  %+v", cfg)
	}, func(key string, newVal, oldVal interface{}) {
		t.Logf("key %s , newVal %+v , oldVal %+v", key, newVal, oldVal)
	})

	if err != nil {
		t.Error(err)
	}

	time.Sleep(time.Second * 200)
}

func TestInitApolloFromDefaultEnv2(t *testing.T) {
	cfg := &TestCfg{}
	_, err := InitApolloFromDefaultEnv("jobsvr", func(viper *viper.Viper) {
		err := viper.Unmarshal(cfg)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("cfg:  %+v", cfg)
	}, func(key string, newVal, oldVal interface{}) {
		t.Logf("key %s , newVal %+v , oldVal %+v", key, newVal, oldVal)
	})

	if err != nil {
		t.Error(err)
	}

	time.Sleep(time.Second * 200)
}
