package global

import (
	"github.com/JieWaZi/transfer-mysql/canal"
	"github.com/JieWaZi/transfer-mysql/storage"
	"github.com/JieWaZi/transfer-mysql/utils"
)

var Config = Configuration{
	Canal: &canal.Canal{
		Host:      "",
		Port:      0,
		User:      "root",
		Password:  "OKpeng5699121@",
		Databases: []string{"binlog_test"},
		Tables:    []string{"user", "user1"},
	},
	BoltStorage:             &storage.BoltStorage{},
	HandlerRowEventPoolSize: 20,
}

type Configuration struct {
	/*---------Log配置---------------*/
	Log *utils.Log

	/*---------Canal配置---------------*/
	Canal *canal.Canal

	/*---------BoltStorage配置----------*/
	BoltStorage *storage.BoltStorage

	HandlerRowEventPoolSize uint32 `env:""`
}

func init() {
	utils.Set(utils.WithServiceName("srv-transfer"))
	utils.ReflectConf(&Config)
}
