package canal

import (
	"fmt"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/sirupsen/logrus"
)

type Canal struct {
	Host           string   `env:""`
	Port           int      `env:""`
	User           string   `env:""`
	Password       string   `env:""`
	Databases      []string `env:""`
	Tables         []string `env:""`
	BinlogFileName string   `env:""`
	BinlogPosition uint32   `env:""`

	UseBoltStoragePosition bool `env:""`

	canal *canal.Canal
}

func (c *Canal) GetCanal() *canal.Canal {
	return c.canal
}

func (c *Canal) SetDefaults() {
	if c.Host == "" {
		c.Host = "127.0.0.1"
	}

	if c.Port == 0 {
		c.Port = 3306
	}
	if c.User == "" {
		c.User = "root"
	}
	if c.Password == "" {
		c.Password = "123456"
	}
	if c.BinlogFileName == "" {
		c.BinlogFileName = "mysql-bin.000001"
	}
	if c.BinlogPosition == 0 {
		c.BinlogPosition = 1
	}
}

func (c *Canal) Init() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", c.Host, c.Port)
	cfg.User = c.User
	cfg.Password = c.Password
	cfg.Dump.Databases = c.Databases
	cfg.Dump.Tables = c.Tables
	canal, err := canal.NewCanal(cfg)
	if err != nil {
		logrus.Errorf("canal newCanal err:%s", err.Error())
		return err
	}
	c.canal = canal
	return nil
}

func (c *Canal) SetEventHandler(handler canal.EventHandler) {
	c.canal.SetEventHandler(handler)
}

func (c *Canal) RunFrom(pos *mysql.Position) error {
	if pos == nil {
		pos = &mysql.Position{
			Name: c.BinlogFileName,
			Pos:  c.BinlogPosition,
		}
	}
	return c.canal.RunFrom(*pos)
}

func (c *Canal) Close() {
	c.canal.Close()
}
