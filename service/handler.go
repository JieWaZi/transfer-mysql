package service

import (
	"context"
	"encoding/json"
	"github.com/JieWaZi/transfer-mysql/global"
	"github.com/JieWaZi/transfer-mysql/models"
	"github.com/JieWaZi/transfer-mysql/storage"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
	"time"
)

type handler struct {
	boltStorage *storage.BoltStorage
	ctx         context.Context
	cancelFunc  context.CancelFunc
	requestChan chan interface{}

	queueStarted           atomic.Bool
	useBoltStoragePosition bool
}

func (h *handler) OnRotate(r *replication.RotateEvent) error {
	logrus.Infof("[OnRotate] rotateEvent: nextPos:%d, name:%s", r.Position, string(r.NextLogName))
	h.requestChan <- models.PosRequest{
		Name:  string(r.NextLogName),
		Pos:   uint32(r.Position),
		Force: true,
	}
	return nil
}
func (h *handler) OnTableChanged(schema string, table string) error { return nil }
func (h *handler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	logrus.Infof("[OnDDL] nextPos:%s-%d, queryEvent:%s", nextPos.Name, nextPos.Pos, string(queryEvent.Query))
	h.requestChan <- models.PosRequest{
		Name:  nextPos.Name,
		Pos:   nextPos.Pos,
		Force: true,
	}
	h.requestChan <- queryEvent
	return nil
}
func (h *handler) OnRow(rowEvent *canal.RowsEvent) error {
	logrus.Infof("[OnRow] table:%s,action:%s,rows:%+v,logPos:%d", rowEvent.Table.Name, rowEvent.Action, rowEvent.Rows, rowEvent.Header.LogPos)
	h.requestChan <- rowEvent
	return nil
}
func (h *handler) OnXID(mysql.Position) error { return nil }
func (h *handler) OnGTID(mysql.GTIDSet) error { return nil }
func (h *handler) OnPosSynced(p mysql.Position, gid mysql.GTIDSet, force bool) error {
	logrus.Infof("[OnPosSynced] position:%s-%d,force:%+v", p.Name, p.Pos, force)
	h.requestChan <- models.PosRequest{
		Name:  p.Name,
		Pos:   p.Pos,
		Force: force,
	}
	return nil
}
func (h *handler) String() string { return "handler" }

func (h *handler) startQueueListener() {
	go func() {
		ticker := time.NewTicker(time.Microsecond * 100)
		defer ticker.Stop()
		position := mysql.Position{
			Name: global.Config.Canal.BinlogFileName,
			Pos:  global.Config.Canal.BinlogPosition,
		}
		// 启动时先获取最新的position
		pos, err := h.boltStorage.GetPosition()
		if err != nil {
			logrus.Infof("get position err:%s", err.Error())
			return
		}
		if pos != nil && h.useBoltStoragePosition {
			position = *pos
		}
		rowPool := make([]canal.RowsEvent, 0, global.Config.HandlerRowEventPoolSize)
		for {
			needFlushRowEvent := false
			needSavePos := false
			select {
			case req := <-h.requestChan:
				switch v := req.(type) {
				case models.PosRequest:
					if v.Force {
						err := h.savePos(mysql.Position{
							Name: v.Name,
							Pos:  v.Pos,
						})
						if err != nil {
							return
						}
					}
					position.Name = v.Name
					position.Pos = v.Pos
				case canal.RowsEvent:
					rowPool = append(rowPool, v)
					needFlushRowEvent = uint32(len(rowPool)) >= global.Config.HandlerRowEventPoolSize
				}
			case <-ticker.C:
				needSavePos = true
				needFlushRowEvent = true
			case <-h.ctx.Done():
				logrus.Infof("handler ctx cancel")
				return
			}

			if needFlushRowEvent {
				for i := range rowPool {
					data, err := json.Marshal(rowPool[i])
					if err != nil {
						return
					}
					h.boltStorage.AddRowRequest(data)
				}
			}
			if needSavePos {
				err := h.savePos(position)
				if err != nil {
					return
				}
			}
		}
	}()
}
func (h *handler) savePos(v mysql.Position) error {
	data, err := json.Marshal(v)
	if err != nil {
		logrus.Infof("marshal PosRequest err:%s", err.Error())
		h.cancelFunc()
		return err
	}
	err = h.boltStorage.SavePosition(data)
	if err != nil {
		logrus.Infof("savePosition err:%s", err.Error())
		h.cancelFunc()
		return err
	}

	return nil
}
