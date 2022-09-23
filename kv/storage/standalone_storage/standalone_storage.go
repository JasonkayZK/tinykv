package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"time"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Done Your Data Here (1).
	Db *badger.DB

	StoreAddr     string
	Raft          bool
	SchedulerAddr string
	LogLevel      string

	DBPath string // Directory to store the data in. Should exist and be writable.

	// raft_base_tick_interval is a base tick interval (ms).
	RaftBaseTickInterval     time.Duration
	RaftHeartbeatTicks       int
	RaftElectionTimeoutTicks int

	// Interval to gc unnecessary raft log (ms).
	RaftLogGCTickInterval time.Duration
	// When entry count exceed this value, gc will be forced trigger.
	RaftLogGcCountLimit uint64

	// Interval (ms) to check region whether you need to be split or not.
	SplitRegionCheckTickInterval time.Duration
	// delay time before deleting a stale peer
	SchedulerHeartbeatTickInterval      time.Duration
	SchedulerStoreHeartbeatTickInterval time.Duration

	// When region [a,e) size meets regionMaxSize, it will be split into
	// several regions [a,b), [b,c), [c,d), [d,e). And the size of [a,b),
	// [b,c), [c,d) will be regionSplitSize (maybe a little larger).
	RegionMaxSize   uint64
	RegionSplitSize uint64
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Done Your Code Here (1).

	return &StandAloneStorage{
		StoreAddr:     conf.StoreAddr,
		Raft:          conf.Raft,
		SchedulerAddr: conf.SchedulerAddr,
		LogLevel:      conf.LogLevel,

		DBPath: conf.DBPath, // Directory to store the data in. Should exist and be writable.

		// raft_base_tick_interval is a base tick interval (ms).
		RaftBaseTickInterval:     conf.RaftBaseTickInterval,
		RaftHeartbeatTicks:       conf.RaftHeartbeatTicks,
		RaftElectionTimeoutTicks: conf.RaftElectionTimeoutTicks,

		// Interval to gc unnecessary raft log (ms).
		RaftLogGCTickInterval: conf.RaftLogGCTickInterval,
		// When entry count exceed this value, gc will be forced trigger.
		RaftLogGcCountLimit: conf.RaftLogGcCountLimit,

		// Interval (ms) to check region whether you need to be split or not.
		SplitRegionCheckTickInterval: conf.SplitRegionCheckTickInterval,
		// delay time before deleting a stale peer
		SchedulerHeartbeatTickInterval:      conf.SchedulerHeartbeatTickInterval,
		SchedulerStoreHeartbeatTickInterval: conf.SchedulerStoreHeartbeatTickInterval,

		// When region [a,e) size meets regionMaxSize, it will be split into
		// several regions [a,b), [b,c), [c,d), [d,e). And the size of [a,b),
		// [b,c), [c,d) will be regionSplitSize (maybe a little larger).
		RegionMaxSize:   conf.RegionMaxSize,
		RegionSplitSize: conf.RegionSplitSize,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	db, err := InitDb(s.DBPath)
	if err != nil {
		log.Fatal(err)
	}
	s.Db = db

	log.Info("Create badger db success", *s)

	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	return nil
}
