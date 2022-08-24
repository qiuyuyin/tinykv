// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
	"time"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return string("balance-region")
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// get health region
	maxStoreDownTime := cluster.GetMaxStoreDownTime()
	stores := make([]*core.StoreInfo, 0)
	for _, s := range cluster.GetStores() {
		if s.IsUp() {
			if time.Since(s.GetLastHeartbeatTS()) < maxStoreDownTime {
				stores = append(stores, s)
			}
		}
	}
	if len(stores) <= 1 {
		return nil
	}

	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})

	var sourceRegion *core.RegionInfo
	notGetSource := true
	for sIndex := 0; sIndex < len(stores)-1; sIndex++ {
		cluster.GetPendingRegionsWithLock(stores[sIndex].GetID(), func(c core.RegionsContainer) {
			sourceRegion = c.RandomRegion(nil, nil)
			notGetSource = false
		})
		if notGetSource {
			cluster.GetFollowersWithLock(stores[sIndex].GetID(), func(c core.RegionsContainer) {
				notGetSource = false
				sourceRegion = c.RandomRegion(nil, nil)
			})
		}
		if notGetSource {
			cluster.GetLeadersWithLock(stores[sIndex].GetID(), func(c core.RegionsContainer) {
				sourceRegion = c.RandomRegion(nil, nil)
				notGetSource = false
			})
		}
		if notGetSource {
			continue
		}
		storesIds := sourceRegion.GetStoreIds()
		if len(storesIds) < cluster.GetMaxReplicas() {
			continue
		}
		for targetIndex := len(stores) - 1; targetIndex > sIndex; targetIndex-- {
			judge := stores[sIndex].GetRegionSize()-stores[targetIndex].GetRegionSize() >
				2*sourceRegion.GetApproximateSize()
			if _, ok := storesIds[stores[targetIndex].GetID()]; !ok && judge {
				newPeer, err := cluster.AllocPeer(stores[targetIndex].GetID())
				if err != nil {
					return nil
				}
				op, err := operator.CreateMovePeerOperator(s.GetType(),
					cluster,
					sourceRegion,
					operator.OpBalance,
					stores[sIndex].GetID(),
					stores[targetIndex].GetID(),
					newPeer.GetId())
				if err != nil {
					return nil
				}
				return op
			}
		}
		return nil
	}

	return nil
}
