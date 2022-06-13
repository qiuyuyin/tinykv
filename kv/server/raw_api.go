package server

import (
    "context"
    "github.com/pingcap-incubator/tinykv/kv/storage"
    "github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
    reader, err := server.storage.Reader(nil)
    if err != nil {
        return nil, err
    }
    value, err := reader.GetCF(req.Cf, req.Key)
    reader.Close()
    if value == nil {
        return &kvrpcpb.RawGetResponse{
            RegionError: nil,
            Error:       "not found",
            Value:       nil,
            NotFound:    true,
        }, nil
    }
    return &kvrpcpb.RawGetResponse{
        RegionError: nil,
        Error:       "",
        Value:       value,
        NotFound:    false,
    }, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
    modifies := []storage.Modify{
        {
            Data: storage.Put{
                Cf:    req.Cf,
                Key:   req.Key,
                Value: req.Value,
            },
        },
    }
    err := server.storage.Write(nil, modifies)
    if err != nil {
        return nil, err
    }
    return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
    modifies := []storage.Modify{
        {
            Data: storage.Delete{
                Cf:  req.Cf,
                Key: req.Key,
            },
        },
    }
    err := server.storage.Write(nil, modifies)
    if err != nil {
        return nil, err
    }
    return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
    reader, err := server.storage.Reader(nil)
    if err != nil {
        return nil, err
    }
    kvPairs := make([]*kvrpcpb.KvPair, 0)
    cfIter := reader.IterCF(req.Cf)
    cfIter.Seek(req.StartKey)
    for i := 0; i < int(req.Limit); i++ {
        if !cfIter.Valid() {
            break
        }
        item := cfIter.Item()
        value, err := item.Value()
        if err != nil {
            return nil, err
        }
        kvPairs = append(kvPairs, &kvrpcpb.KvPair{Key: item.Key(), Value: value})
        cfIter.Next()
    }
    cfIter.Close()
    reader.Close()
    return &kvrpcpb.RawScanResponse{
        RegionError: nil,
        Error:       "",
        Kvs:         kvPairs,
    }, nil
}
