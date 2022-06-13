package standalone_storage

import (
    "github.com/Connor1996/badger"
    "github.com/pingcap-incubator/tinykv/kv/config"
    "github.com/pingcap-incubator/tinykv/kv/storage"
    "github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
    "github.com/pingcap-incubator/tinykv/kv/util/engine_util"
    "github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
    "github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
    "log"
    "os"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
    StorageDB *badger.DB
    LogLevel  string
    DBPath    string
    Raft      bool
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
    return &StandAloneStorage{
        DBPath:   conf.DBPath,
        LogLevel: conf.LogLevel,
        Raft:     conf.Raft,
    }
}

func (s *StandAloneStorage) Start() error {
    opts := badger.DefaultOptions
    opts.Dir = s.DBPath
    opts.ValueDir = opts.Dir
    if err := os.MkdirAll(opts.Dir, os.ModePerm); err != nil {
        log.Fatal(err)
    }
    db, err := badger.Open(opts)
    if err != nil {
        log.Fatal(err)
    }
    s.StorageDB = db
    return nil
}

func (s *StandAloneStorage) Stop() error {

    err := s.StorageDB.Close()
    if err != nil {
        return err
    }
    if err := os.RemoveAll(s.DBPath); err != nil {
        return err
    }
    return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
    transaction := s.StorageDB.NewTransaction(false)
    reader := raft_storage.NewRegionReader(transaction, metapb.Region{StartKey: []byte{}, EndKey: []byte{}})
    return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
    writeBatch := new(engine_util.WriteBatch)
    for i := range batch {
        writeBatch.SetCF(batch[i].Cf(), batch[i].Key(), batch[i].Value())
    }
    err := writeBatch.WriteToDB(s.StorageDB)
    if err != nil {
        return err
    }
    return nil
}
