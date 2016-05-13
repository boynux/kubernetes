/*
Copyright 2016 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workqueue

import (
	"sync"
)

type Priority int

const (
	Urgent Priority = 0
	Normal Priority = 1
)

var priorityList = []Priority{Urgent, Normal}

type PriorityInterface interface {
	Add(item interface{}, priority Priority)
	Get() (item interface{}, priority Priority, shutdown bool)
	Done(item interface{}, priority Priority)
}

type PriorityType map[Priority]Type

func NewPriorityQueue() PriorityInterface {
	return &map[Priority]Type{}
}

func (pq *PriorityType) Add(item interface{}, p Priority) {
	if pq[p] == nil {
		pq[p] = &Type{
			dirty:      set{},
			processing: set{},
			cond:       sync.NewCond(&sync.Mutex{}),
		}
	}

	pq[p].Add(item)
}

func (pq *PriorityType) Get() (interface{}, Priority, bool) {
	for _, index := range priorityList {
		if pq[index] != nil && pq[index].Len() != 0 {
			item, shutdown := pq[index].Get()
			return item, index, shutdown
		}
	}
	return nil, nil, true
}

func (pq *PriorityType) Done() (item interface{}, p Priority) {
	pq[p].Done(item)
	return
}
