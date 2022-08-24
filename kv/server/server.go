package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	kvrpcpb "github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	var resp = &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return RetGetError(resp, err)
	}
	defer reader.Close()
	// create new txn
	var txn = mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return RetGetError(resp, err)
	}
	// find the key has a lock, another txn is commit or has committed but not retrieve the second lock
	if lock != nil && req.Version >= lock.Ts {
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
		return resp, nil
	}
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return RetGetError(resp, err)
	}
	if value == nil {
		resp.NotFound = true
	}
	resp.Value = value
	return resp, nil
}

func RetGetError(resp *kvrpcpb.GetResponse, err error) (*kvrpcpb.GetResponse, error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	return nil, err
}
func RetPrewriteError(resp *kvrpcpb.PrewriteResponse, err error) (*kvrpcpb.PrewriteResponse, error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	return nil, err
}

func RetCommitError(resp *kvrpcpb.CommitResponse, err error) (*kvrpcpb.CommitResponse, error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	return nil, err
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	var resp = &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return RetPrewriteError(resp, err)
	}
	defer reader.Close()
	var mutationKeys = make([][]byte, 0, len(req.Mutations)) // get all op's key
	for _, mutation := range req.Mutations {
		mutationKeys = append(mutationKeys, mutation.Key)
	}
	// lock the data
	var txn = mvcc.NewMvccTxn(reader, req.StartVersion)
	var keyErrors = make([]*kvrpcpb.KeyError, 0)
	for _, key := range mutationKeys {
		// checkLock
		lock, err := txn.GetLock(key)
		if err != nil {
			return RetPrewriteError(resp, err)
		}
		if lock != nil {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: req.PrimaryLock,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				},
			})
		}
	}
	if len(keyErrors) != 0 {
		resp.Errors = keyErrors
		return resp, nil
	}
	for i := range mutationKeys {
		// checkLock
		write, committedTs, err := txn.MostRecentWrite(mutationKeys[i])
		if err != nil {
			return RetPrewriteError(resp, err)
		}
		if write != nil && req.StartVersion < committedTs {
			// err
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: committedTs,
					Key:        mutationKeys[i],
					Primary:    req.PrimaryLock,
				},
			})
		}
	}
	if len(keyErrors) != 0 {
		resp.Errors = keyErrors
		return resp, nil
	}
	for _, mutation := range req.Mutations {
		var lock *mvcc.Lock = &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
		}
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mutation.Key, mutation.Value)
			lock.Kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mutation.Key)
			lock.Kind = mvcc.WriteKindDelete
		default:
			return nil, fmt.Errorf("Not Find Your Op")
		}
		txn.PutLock(mutation.Key, lock)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return RetPrewriteError(resp, err)
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	var (
		startTs  = req.StartVersion
		commitTs = req.CommitVersion
	)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return RetCommitError(resp, err)
	}
	defer reader.Close()

	var txn = mvcc.NewMvccTxn(reader, startTs)
	var keys = req.Keys

	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	for _, key := range keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return RetCommitError(resp, err)
		}
		if lock == nil {
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return RetCommitError(resp, err)
			}
			// if already rollback
			if write != nil && write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Abort: "true",
				}
			}
			return resp, nil
		}
		// maybe prewrite not write into the log
		if lock.Ts != startTs {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "true",
			}
			return resp, nil
		}
		txn.PutWrite(key, commitTs, &mvcc.Write{
			StartTS: startTs,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return RetCommitError(resp, err)
	}
	return resp, nil
}

func RetScanError(resp *kvrpcpb.ScanResponse, err error) (*kvrpcpb.ScanResponse, error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	return nil, err
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return RetScanError(resp, err)
	}
	defer reader.Close()

	var txn = mvcc.NewMvccTxn(reader, req.Version)
	var scanner = mvcc.NewScanner(req.StartKey, txn)

	defer scanner.Close()

	var pairs = make([]*kvrpcpb.KvPair, 0)

	for i := uint32(0); i < req.Limit; i++ {
		key, value, err := scanner.Next()
		if key == nil && value == nil && err == nil {
			break
		}
		if err != nil {
			return RetScanError(resp, err)
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return RetScanError(resp, err)
		}
		if lock != nil && lock.Ts > req.Version {
			pairs = append(pairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					},
				},
			})
		} else if value != nil {
			pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Value: value})
		}
	}
	resp.Pairs = pairs
	return resp, nil
}

func RetCheckTxnError(resp *kvrpcpb.CheckTxnStatusResponse, err error) (*kvrpcpb.CheckTxnStatusResponse, error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	return nil, err
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	// CheckTxnStatus reports on the status of a transaction and may take action to
	// rollback expired locks.
	// If the transaction has previously been rolled back or committed, return that information.
	// If the TTL of the transaction is exhausted, abort that transaction and roll back the primary lock.
	var resp = &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return RetCheckTxnError(resp, err)
	}
	defer reader.Close()
	server.Latches.WaitForLatches([][]byte{req.PrimaryKey})
	defer server.Latches.ReleaseLatches([][]byte{req.PrimaryKey})

	var txn = mvcc.NewMvccTxn(reader, req.LockTs)

	write, ts, err := txn.CurrentWrite(req.PrimaryKey)

	if err != nil {
		return RetCheckTxnError(resp, err)
	}
	// the txn has committed
	if write != nil {
		if write.Kind == mvcc.WriteKindRollback {
			return resp, nil
		}
		resp.CommitVersion = ts
		return resp, nil
	}

	// get the current lock
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return RetCheckTxnError(resp, err)
	}
	if lock != nil {
		// judge if the lock has ttl
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
			txn.Rollback(req.PrimaryKey, true)
			resp.Action = kvrpcpb.Action_TTLExpireRollback
		}
	} else {
		txn.Rollback(req.PrimaryKey, false)
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return RetCheckTxnError(resp, err)
	}
	return resp, nil
}

func RetBatchRollbackTxnError(resp *kvrpcpb.BatchRollbackResponse, err error) (*kvrpcpb.BatchRollbackResponse, error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	return nil, err
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	var resp = &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return RetBatchRollbackTxnError(resp, err)
	}
	defer reader.Close()
	var batches = make([][]byte, 0)
	for _, key := range req.Keys {
		batches = append(batches, key)
	}
	server.Latches.WaitForLatches(batches)
	defer server.Latches.ReleaseLatches(batches)
	var txn = mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return RetBatchRollbackTxnError(resp, err)
		}
		if write != nil {
			// if not rollback, the command here is wrong
			if write.Kind != mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Abort: "true",
				}
				return resp, nil
			}
			continue
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return RetBatchRollbackTxnError(resp, err)
		}
		if lock != nil {
			// locked by another txn
			txn.Rollback(key, lock.Ts == txn.StartTS)
		} else {
			txn.Rollback(key, false)
		}
	}
	if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
		return RetBatchRollbackTxnError(resp, err)
	}
	return resp, nil
}

func RetResolveLockTxnError(resp *kvrpcpb.ResolveLockResponse, err error) (*kvrpcpb.ResolveLockResponse, error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	return nil, err
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	var resp = &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return RetResolveLockTxnError(resp, err)
	}
	defer reader.Close()
	var iter = reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	var secondaryKeys = make([][]byte, 0)
	for iter.Valid() {
		var item = iter.Item()
		value, err := item.Value()
		if err != nil {
			return RetResolveLockTxnError(resp, err)
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return RetResolveLockTxnError(resp, err)
		}
		if lock.Ts == req.StartVersion {
			secondaryKeys = append(secondaryKeys, item.Key())
		}
		iter.Next()
	}
	if len(secondaryKeys) == 0 {
		return resp, nil
	}

	if req.CommitVersion == 0 {
		r, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			Keys:         secondaryKeys,
			StartVersion: req.StartVersion,
		})
		resp.RegionError = r.RegionError
		resp.Error = r.Error
		return resp, err
	}

	if req.CommitVersion > 0 {
		r, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			CommitVersion: req.CommitVersion,
			Keys:          secondaryKeys,
			StartVersion:  req.StartVersion,
		})
		resp.RegionError = r.RegionError
		resp.Error = r.Error
		return resp, err
	}

	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	var resp = new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
