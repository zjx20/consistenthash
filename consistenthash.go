/*
Copyright 2013 Google Inc.
Copyright 2017 zjx20

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

// Package consistenthash provides an implementation of a ring hash.
package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

type node struct {
	hash uint32
	key  string
}

type Map struct {
	hash     Hash
	replicas int
	nodes    []node // Sorted
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Returns true if there are no items available.
func (m *Map) IsEmpty() bool {
	return len(m.nodes) == 0
}

// Adds some keys to the hash.
func (m *Map) Add(keys ...string) {
	oriNodes := m.nodes
	m.nodes = make([]node, len(m.nodes)+len(keys)*m.replicas)
	copy(m.nodes, oriNodes)
	l := len(oriNodes)
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := m.hash([]byte(strconv.Itoa(i) + key))
			m.nodes[l] = node{hash: hash, key: key}
			l++
		}
	}
	sort.Slice(m.nodes, func(i, j int) bool {
		if m.nodes[i].hash != m.nodes[j].hash {
			return m.nodes[i].hash < m.nodes[j].hash
		}
		return m.nodes[i].key < m.nodes[j].key
	})
}

// Remove a key from the hash.
func (m *Map) Remove(key string) {
	newNodes := make([]node, 0, len(m.nodes))
	for _, n := range m.nodes {
		if n.key != key {
			newNodes = append(newNodes, n)
		}
	}
	m.nodes = newNodes
}

// Gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := m.hash([]byte(key))

	// Binary search for appropriate replica.
	idx := sort.Search(len(m.nodes), func(i int) bool {
		return m.nodes[i].hash >= hash
	})

	// Means we have cycled back to the first replica.
	if idx == len(m.nodes) {
		idx = 0
	}

	return m.nodes[idx].key
}
