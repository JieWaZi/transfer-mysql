package main

import (
	"github.com/JieWaZi/transfer-mysql/global"
	"github.com/JieWaZi/transfer-mysql/service"
	"github.com/JieWaZi/transfer-mysql/utils"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	task, err := service.NewTask(
		service.WithCanal(global.Config.Canal),
		service.WithBoltDB(global.Config.BoltStorage))
	if err != nil {
		panic(err)
	}

	utils.Execute(func(cmd *cobra.Command, args []string) {
		go task.Run()
	})

	setupSignalHandler([]service.Service{task})
}

func setupSignalHandler(services []service.Service) {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	sig := <-sc
	logrus.Infof("Got signal [%s] to exit.", sig)

	for _, service := range services {
		service.Stop()
	}
	os.Exit(0)
}
