// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
    pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
    "github.com/pingcap/log"
    "github.com/pkg/errors"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
    // storage contains all stable entries since the last snapshot.
    storage Storage

    // committed is the highest log position that is known to be in
    // stable storage on a quorum of nodes.
    committed uint64

    // applied is the highest log position that the application has
    // been instructed to apply to its state machine.
    // Invariant: applied <= committed
    applied uint64

    // log entries with index <= stabled are persisted to storage.
    // It is used to record the logs that are not persisted by storage yet.
    // Everytime handling `Ready`, the unstabled logs will be included.
    stabled uint64

    // all entries that have not yet compact.
    entries []pb.Entry

    // the incoming unstable snapshot, if any.
    // (Used in 2C)
    pendingSnapshot *pb.Snapshot

    // Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {

    raftLog := RaftLog{}

    lastIndex, err := storage.LastIndex()
    if err != nil {
        log.Fatal(err.Error())
    }
    firstIndex, err := storage.FirstIndex()
    if err != nil {
        log.Fatal(err.Error())
    }
    raftLog.stabled = lastIndex
    raftLog.committed = firstIndex - 1
    raftLog.applied = firstIndex - 1
    raftLog.storage = storage
    raftLog.entries = []pb.Entry{}
    if err != nil {
        log.Fatal(err.Error())
    }
    return &raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
    // Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
    if len(l.entries) == 0 {
        return nil
    } else {
        return l.entries
    }
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
    return l.splitEntries(l.applied+1, l.committed+1)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
    if len(l.entries) == 0 {
        index, err := l.storage.LastIndex()
        if err != nil {
            panic(err)
        }
        return index

    } else {
        return l.stabled + uint64(len(l.entries))
    }
}

// Term return the term of the entry in the given index
// this i would be in [lastIndex,LastIndex]
func (l *RaftLog) Term(i uint64) (uint64, error) {
    // Your Code Here (2A).
    firstIndex, err := l.storage.FirstIndex()
    if err != nil {
        return 0, err
    }
    if i < firstIndex || i > l.LastIndex() {
        return 0, errors.New("index beyond the range")
    }
    // if the index is in the stable area, we should search in the storage
    if i <= l.stabled {
        return l.storage.Term(i)
    } else {
        return l.entries[i-l.stabled-1].Term, nil
    }

}

// splitEntries slice the log Entries in the range of [lo,hi)
func (l *RaftLog) splitEntries(lo uint64, hi uint64) []pb.Entry {
    if lo <= hi || hi-1 > l.LastIndex() {
        return nil
    }
    if lo > l.stabled {
        if hi-lo <= uint64(len(l.entries)) {
            return l.entries[lo:]
        } else {
            return l.entries[lo:hi]
        }
    } else if hi-1 <= l.stabled {
        entries, err := l.storage.Entries(lo, hi)
        if err != nil {
            return nil
        }
        return entries
    } else {
        entries := l.splitEntries(lo, l.stabled+1)
        splitEntries := l.splitEntries(l.stabled+1, hi)
        return append(entries, splitEntries...)
    }
}
