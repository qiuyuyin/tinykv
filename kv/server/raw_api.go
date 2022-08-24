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
	// Your Code Here (1).
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	resp.Value, err = reader.GetCF(req.Cf, req.Key)
	if resp.Value == nil {
		resp.NotFound = true
		return resp, err
	}
	return resp, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	modify := storage.Modify{Data: storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		return &kvrpcpb.RawPutResponse{}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	modify := storage.Modify{Data: storage.Delete{Key: req.Key, Cf: req.Cf}}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{}, nil
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
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
