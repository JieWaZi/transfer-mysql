package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/JieWaZi/transfer-mysql/canal"
	"github.com/JieWaZi/transfer-mysql/storage"
	"github.com/google/uuid"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
)

type Task struct {
	taskID string

	ctx        context.Context
	cancelFunc context.CancelFunc

	handler *handler
	boltDB  *storage.BoltStorage
	canal   *canal.Canal

	running atomic.Bool
}

func NewTask(options ...TaskOption) (*Task, error) {
	t := &Task{
		taskID: uuid.New().String(),
	}
	t.ctx, t.cancelFunc = context.WithCancel(context.Background())

	for _, p := range options {
		if err := p(t); err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithCancel(t.ctx)
	t.handler = &handler{
		boltStorage:            t.boltDB,
		ctx:                    ctx,
		cancelFunc:             cancel,
		requestChan:            make(chan interface{}, 4096),
		useBoltStoragePosition: t.canal.UseBoltStoragePosition,
	}
	if t.canal == nil {
		return nil, errors.New("Canal is null, please init canal first ")
	}
	t.canal.SetEventHandler(t.handler)
	return t, nil
}

type TaskOption = func(t *Task) error

func WithCanal(canal *canal.Canal) TaskOption {
	return func(t *Task) error {
		if canal.GetCanal() == nil {
			canal.SetDefaults()
			if err := canal.Init(); err != nil {
				return err
			}
		}
		t.canal = canal
		return nil
	}
}

func WithBoltDB(boltDB *storage.BoltStorage) TaskOption {
	return func(t *Task) error {
		if boltDB.GetBoltStorage() == nil {
			if err := boltDB.Init(); err != nil {
				return err
			}
		}
		boltDB.RowRequestBucket = fmt.Sprintf("%s_%s", t.taskID, boltDB.RowRequestBucket)
		boltDB.PositionBucket = fmt.Sprintf("%s_%s", t.taskID, boltDB.PositionBucket)
		err := boltDB.CreateBucketIfNotExists(
			[]byte(boltDB.RowRequestBucket),
			[]byte(boltDB.PositionBucket),
			[]byte(boltDB.TaskBucket),
		)
		if err != nil {
			return err
		}
		t.boltDB = boltDB
		return nil
	}
}

func (t *Task) Run() (err error) {
	t.handler.startQueueListener()
	var position = &mysql.Position{
		Name: t.canal.BinlogFileName,
		Pos:  t.canal.BinlogPosition,
	}
	if t.canal.UseBoltStoragePosition {
		position, err = t.boltDB.GetPosition()
		if err != nil {
			logrus.Errorf("boltDB get position err:%s", err.Error())
			return err
		}
	}

	t.running.Store(true)
	logrus.Infof("transfer service start running name:%s position: %d", position.Name, position.Pos)
	err = t.canal.RunFrom(position)
	if err != nil {
		logrus.Errorf("canal run from err:%s", err.Error())
		t.running.Store(false)
		return err
	}

	return nil
}

func (t *Task) Stop() {
	logrus.Info("stop transfer service")
	t.handler.cancelFunc()
	if t.canal != nil {
		t.canal.Close()
	}
	t.running.Store(false)
}

func (t *Task) ReStart() (err error) {
	if !t.running.Load() {
		t.handler.startQueueListener()
		var position = &mysql.Position{
			Name: t.canal.BinlogFileName,
			Pos:  t.canal.BinlogPosition,
		}
		position, err = t.boltDB.GetPosition()
		if err != nil {
			logrus.Errorf("boltDB get position err:%s", err.Error())
			return err
		}

		t.running.Store(true)
		logrus.Infof("transfer service start running name:%s position: %d", position.Name, position.Pos)
		err = t.canal.RunFrom(position)
		if err != nil {
			logrus.Errorf("canal run from err:%s", err.Error())
			t.running.Store(false)
			return err
		}
	} else {
		return errors.New("the task is running")
	}

	return nil
}
