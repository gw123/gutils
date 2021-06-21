package m

import (
	"testing"
	"time"

	"github.com/spf13/viper"

	"github.com/zouyx/agollo/v4/env/config"
)

const APP_APOLLO_PREFIX = "APOLLO_EMAIL_"

func TestNewGApollo(t *testing.T) {
	viper.AutomaticEnv()

	namespace := viper.GetString(APP_APOLLO_PREFIX + "NAMESPACE")
	appId := viper.GetString(APP_APOLLO_PREFIX + "APPID")
	secret := viper.GetString(APP_APOLLO_PREFIX + "SECRET")
	ip := viper.GetString(APP_APOLLO_PREFIX + "IP")

	//DOCKER_VOLUME_ROOT
	c := &config.AppConfig{
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
		t.Log("point.addr", viper.Get("point.addr"))
		t.Log("point", viper.Get("point"))
		t.Log("abc", viper.Get("abc"))
	}
}
