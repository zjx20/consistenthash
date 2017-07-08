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
	"strconv"

	"github.com/bmaxa/trees/rb"
	"github.com/bmaxa/trees/tree"
)

type Hash func(data []byte) uint32

var _ tree.Key = (*node)(nil)

type node struct {
	hash uint32
	key  string
}

func (n *node) Less(than tree.Key) bool {
	m := than.(*node)
	if n.hash != m.hash {
		return n.hash < m.hash
	}
	return n.key < m.key
}

func floor(t *tree.Tree, key tree.Key) tree.Iterator {
	n, tmp := t.Root, (*tree.Node)(nil)
	for n != nil {
		if n.Key.Less(key) {
			n = n.Right
		} else {
			tmp = n
			n = n.Left
		}
	}
	return tree.NewIter(tmp)
}

type Map struct {
	hash     Hash
	replicas int
	nodes    *rb.RB
	keys     map[string][]*node // for removal
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		nodes:    rb.New(),
		keys:     make(map[string][]*node),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Returns true if there are no items available.
func (m *Map) IsEmpty() bool {
	return m.nodes.Size() == 0
}

// Adds some keys to the hash.
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		if _, ok := m.keys[key]; ok {
			// Already exists
			continue
		}
		l := make([]*node, m.replicas)
		for i := 0; i < m.replicas; i++ {
			hash := m.hash([]byte(strconv.Itoa(i) + key))
			n := &node{hash: hash, key: key}
			l[i] = n
			m.nodes.Insert(tree.Item{
				Key:   n,
				Value: nil,
			})
		}
		m.keys[key] = l
	}
}

// Remove a key from the hash.
func (m *Map) Remove(key string) {
	if l, ok := m.keys[key]; ok {
		for _, n := range l {
			m.nodes.Delete(n)
		}
		delete(m.keys, key)
	}
}

// Gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}
	hash := m.hash([]byte(key))
	n := node{hash: hash, key: key}
	iter := floor(&m.nodes.Tree, &n)
	if iter == m.nodes.End() {
		iter = m.nodes.Begin()
	}
	return iter.Node().Key.(*node).key
}
