package storage

import (
	"encoding/json"
	"github.com/JieWaZi/transfer-mysql/utils"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
	"path/filepath"
)

type BoltStorage struct {
	BoltStoragePath string `env:""`
	BoltFilePath    string `env:""`
	BoltFileName    string `env:""`

	RowRequestBucket string `env:""`
	PositionBucket   string `env:""`
	TaskBucket       string `env:""`
	PositionKey      string `env:""`

	boltDB *bbolt.DB
}

func (b *BoltStorage) GetBoltStorage() *bbolt.DB {
	return b.boltDB
}

func (b *BoltStorage) SetDefaults() {
	if b.BoltStoragePath == "" {
		b.BoltStoragePath = filepath.Join(utils.GetCurrentDirectory(), "/store/data")
	}
	if b.BoltFilePath == "" {
		b.BoltFilePath = "bolt"
	}
	if b.BoltFileName == "" {
		b.BoltFileName = "data.db"
	}
	if b.RowRequestBucket == "" {
		b.RowRequestBucket = "RowRequest"
	}
	if b.PositionBucket == "" {
		b.PositionBucket = "Position"
	}
	if b.TaskBucket == "" {
		b.TaskBucket = "Task"
	}
	if b.PositionKey == "" {
		b.PositionKey = "bolt_position_key"
	}
}

func (b *BoltStorage) Init() error {
	boltStorePath := filepath.Join(b.BoltStoragePath, b.BoltFilePath)
	if err := utils.MkdirIsNotExit(boltStorePath); err != nil {
		logrus.Errorf("create blot path err:%s", err.Error())
		panic(err)
	}
	boltFilePath := filepath.Join(boltStorePath, b.BoltFileName)
	boltDB, err := bbolt.Open(boltFilePath, 0600, bbolt.DefaultOptions)
	if err != nil {
		logrus.Errorf("blot open err:%s", err.Error())
		panic(err)
	}
	b.boltDB = boltDB
	return nil
}

func (b *BoltStorage) CreateBucketIfNotExists(buckets ...[]byte) error {
	return b.boltDB.Update(func(tx *bbolt.Tx) error {
		for _, bucket := range buckets {
			tx.CreateBucketIfNotExists(bucket)
		}
		return nil
	})
}

func (b *BoltStorage) AddRowRequest(data []byte) error {
	return b.AddByBucketName([]byte(b.RowRequestBucket), data)
}

func (b *BoltStorage) AddRowRequestByKey(key, data []byte) error {
	return b.AddByBucketNameAndKey([]byte(b.RowRequestBucket), key, data)
}

func (b *BoltStorage) AddTaskByKey(key, data []byte) error {
	return b.AddByBucketNameAndKey([]byte(b.TaskBucket), key, data)
}

func (b *BoltStorage) GetRowRequest(key []byte) ([]byte, error) {
	return b.GetByKeyFromBucket([]byte(b.RowRequestBucket), key)
}

func (b *BoltStorage) GetPosition() (*mysql.Position, error) {
	var position mysql.Position
	pos, err := b.GetByKeyFromBucket([]byte(b.PositionBucket), []byte(b.PositionKey))
	if err != nil {
		return nil, err
	}
	if pos == nil || len(pos) == 0 {
		return nil, nil
	}
	err = json.Unmarshal(pos, &position)
	if err != nil {
		return nil, err
	}
	return &position, nil
}

func (b *BoltStorage) SavePosition(data []byte) error {
	return b.AddByBucketNameAndKey([]byte(b.PositionBucket), []byte(b.PositionKey), data)
}

func (b *BoltStorage) DeleteRowRequest(key []byte) error {
	return b.DeleteByKeyFromBucket([]byte(b.RowRequestBucket), key)
}

func (b *BoltStorage) AddByBucketName(bucketName, data []byte) error {
	return b.boltDB.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(bucketName)
		seq, _ := bt.NextSequence()
		return bt.Put(utils.Uint64ToBytes(seq), data)
	})
}

func (b *BoltStorage) AddByBucketNameAndKey(bucketName, key, data []byte) error {
	return b.boltDB.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(bucketName)
		return bt.Put(key, data)
	})
}

func (b *BoltStorage) BatchAddByBucketName(bucketName []byte, list [][]byte) error {
	return b.boltDB.Update(func(tx *bbolt.Tx) error {
		for i := range list {
			bt := tx.Bucket(bucketName)
			seq, _ := bt.NextSequence()
			err := bt.Put(utils.Uint64ToBytes(seq), list[i])
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BoltStorage) GetByKeyFromBucket(bucketName, key []byte) ([]byte, error) {
	var entity []byte
	err := b.boltDB.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(bucketName)
		entity = bt.Get(key)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return entity, nil
}

func (b *BoltStorage) DeleteByKeyFromBucket(bucketName, key []byte) error {
	return b.boltDB.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(bucketName)
		return bt.Delete(key)
	})
}

func (b *BoltStorage) Size(bucketName []byte) (int, error) {
	var size int
	err := b.boltDB.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(bucketName)
		size = bt.Stats().KeyN
		return nil
	})

	return size, err
}
