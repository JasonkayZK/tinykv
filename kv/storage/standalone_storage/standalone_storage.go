package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Done Your Data Here (1).
	Engines       *engine_util.Engines
	Logger        *log.Logger
	StoreAddr     string
	SchedulerAddr string
	DBPath        string // Directory to store the data in. Should exist and be writable.
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Done Your Code Here (1).
	logger := log.New()
	logger.SetLevel(log.StringToLogLevel(conf.LogLevel))

	return &StandAloneStorage{
		StoreAddr:     conf.StoreAddr,
		SchedulerAddr: conf.SchedulerAddr,
		Logger:        logger,
		DBPath:        conf.DBPath, // Directory to store the data in. Should exist and be writable.
	}
}

func (s *StandAloneStorage) Start() error {
	// Done Your Code Here (1).
	s.Logger.Infof("stand alone storage start...")

	s.Engines = engine_util.NewEngines(
		engine_util.CreateDB(s.DBPath, false),
		nil,
		s.DBPath,
		"",
	)

	s.Logger.Infof("Create badger db success: %v", *s)

	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Done Your Code Here (1).
	if s.Engines == nil {
		return nil
	}

	return s.Engines.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Done Your Code Here (1).
	return s.NewStandaloneReader()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Done Your Code Here (1).
	writeBatch := &engine_util.WriteBatch{}
	for _, item := range batch {
		switch item.Data.(type) {
		case storage.Put:
			writeBatch.SetCF(item.Cf(), item.Key(), item.Value())
		case storage.Delete:
			writeBatch.DeleteCF(item.Cf(), item.Key())
		}
	}
	return s.Engines.WriteKV(writeBatch)
}
