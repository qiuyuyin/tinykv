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
    "errors"
    "fmt"

    pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
    "github.com/pingcap/log"
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
    entries, err := storage.Entries(firstIndex, lastIndex+1)
    if err != nil {
        log.Fatal(err.Error())
    }
    raftLog.entries = entries
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
    l.storage.Snapshot()
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
    if l.stabled >= l.LastIndex() {
        return []pb.Entry{}
    }
    return l.entries[l.stabled:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
    // if the entries is null
    if len(l.entries) == 0 {
        return
    }
    firstIndex := l.entries[0].Index
    ents = l.entries[l.applied+1-firstIndex : l.committed+1-firstIndex]
    return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
    if l.lenEntries() > 0 {
        return l.entries[0].Index + l.lenEntries() - 1
    } else {

        // find it from snap
        lastIndex, _ := l.storage.LastIndex()
        return lastIndex
    }
}

// lenEntries return the length of entries
func (l *RaftLog) lenEntries() uint64 {
    return uint64(len(l.entries))
}

// Term return the term of the entry in the given index
func (l *RaftLog) splitEntries(i uint64) (ents []pb.Entry) {
    if l.lenEntries() == 0 {
        return
    } else {
        index := i - l.entries[0].Index
        ents = l.entries[index:]
        return
    }
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
    if i == 0 {
        return 0, nil
    }
    if l.lenEntries() == 0 {
        term, _ := l.storage.Term(i)
        return term, nil
    }
    if i >= l.entries[0].Index+l.lenEntries() || i < l.entries[0].Index {
        return 0, errors.New("beyond the range")
    }
    if l.lenEntries() == 0 {
        term, err := l.storage.Term(i)
        if err != nil {
            return 0, nil
        }
        return term, nil
    } else {
        if i-l.entries[0].Index > 1000 {
            fmt.Println(i)
        }
        return l.entries[i-l.entries[0].Index].Term, nil
    }
}
