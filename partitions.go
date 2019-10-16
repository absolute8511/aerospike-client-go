// Copyright 2013-2017 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"bytes"
	"strconv"
)

type Partitions [][]*Node

func newPartitions(partitionCount int, replicaCount int) Partitions {
	replicas := make([][]*Node, replicaCount)
	for i := range replicas {
		replicas[i] = make([]*Node, partitionCount)
	}

	return Partitions(replicas)
}

func (p Partitions) clone() Partitions {
	replicas := make([][]*Node, len(p))
	for i := range p {
		r := make([]*Node, len(p[i]))
		copy(r, p[i])
		replicas[i] = r
	}
	return replicas
}

/*

	partitionMap

*/

type partitionMap map[string]Partitions

// String implements stringer interface for partitionMap
func (pm partitionMap) clone() partitionMap {
	// Make deep copy of map.
	pmap := make(partitionMap, len(pm))
	for ns, replArr := range pm {
		pmap[ns] = replArr.clone()
	}
	return pmap
}

// String implements stringer interface for partitionMap
func (pm partitionMap) merge(other partitionMap) {
	// merge partitions; iterate over the new partition and update the old one
	for ns, replicaArray := range other {
		if pm[ns] == nil {
			pm[ns] = replicaArray.clone()
		} else {
			for i, nodeArray := range replicaArray {
				if len(pm[ns]) <= i {
					pm[ns] = append(pm[ns], make([]*Node, len(nodeArray)))
				} else if pm[ns][i] == nil {
					pm[ns][i] = make([]*Node, len(nodeArray))
				}

				// merge nodes into the partition map
				for j, node := range nodeArray {
					if node != nil {
						pm[ns][i][j] = node
					}
				}
			}
		}
	}
}

// String implements stringer interface for partitionMap
func (pm partitionMap) String() string {
	res := bytes.Buffer{}
	for ns, replicaArray := range pm {
		for i, nodeArray := range replicaArray {
			for j, node := range nodeArray {
				res.WriteString(ns)
				res.WriteString(",")
				res.WriteString(strconv.Itoa(i))
				res.WriteString(",")
				res.WriteString(strconv.Itoa(j))
				res.WriteString(",")
				if node != nil {
					res.WriteString(node.String())
				} else {
					res.WriteString("NIL")
				}
				res.WriteString("\n")
			}
		}
	}
	return res.String()
}
