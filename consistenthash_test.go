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

package consistenthash

import (
	"fmt"
	"hash/crc32"
	"math"
	"reflect"
	"runtime"
	"strconv"
	"testing"

	"github.com/OneOfOne/xxhash"
)

func TestHashing(t *testing.T) {
	// Override the hash function to return easier to reason about values. Assumes
	// the keys can be converted to an integer.
	hash := New(3, func(key []byte) uint32 {
		i, err := strconv.Atoi(string(key))
		if err != nil {
			panic(err)
		}
		return uint32(i)
	})

	// Given the above hash function, this will give replicas with "hashes":
	// 2, 4, 6, 12, 14, 16, 22, 24, 26
	hash.Add("6", "4", "2")

	testCases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"27": "2",
	}

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

	// Adds 8, 18, 28
	hash.Add("8")

	// 27 should now map to 8.
	testCases["27"] = "8"

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

	// Remove 8
	hash.Remove("8")

	// 27 should map to 2
	testCases["27"] = "2"

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}
}

func TestEmpty(t *testing.T) {
	hash := New(1, nil)
	v := hash.Get("foo")
	if v != "" {
		t.Errorf("Should return empty string")
	}
}

func TestConsistency(t *testing.T) {
	hash1 := New(1, nil)
	hash2 := New(1, nil)

	hash1.Add("Bill", "Bob", "Bonny")
	hash2.Add("Bob", "Bonny", "Bill")

	if hash1.Get("Ben") != hash2.Get("Ben") {
		t.Errorf("Fetching 'Ben' from both hashes should be the same")
	}

	hash2.Add("Becky", "Ben", "Bobby")

	if hash1.Get("Ben") != hash2.Get("Ben") ||
		hash1.Get("Bob") != hash2.Get("Bob") ||
		hash1.Get("Bonny") != hash2.Get("Bonny") {
		t.Errorf("Direct matches should always return the same entry")
	}
}

func TestCollision(t *testing.T) {
	v := "collision value"
	words := map[string]struct{}{}
	collisionHashFunc := func(data []byte) uint32 {
		s := string(data)
		if s == "2foo" || s == "0bar" {
			words[s] = struct{}{}
			// simulate hash collision
			return crc32.ChecksumIEEE([]byte(v))
		}
		return crc32.ChecksumIEEE(data)
	}

	hash1 := New(3, collisionHashFunc)
	hash2 := New(3, collisionHashFunc)

	// The order of the keys should not affect the hash result.
	hash1.Add("foo", "bar")
	hash2.Add("bar", "foo")

	// Make sure the test case is valid.
	if len(words) != 2 {
		t.Errorf("The test case doesn't match the current implementation.")
	}

	if hash1.Get(v) != hash2.Get(v) {
		t.Errorf("Hashes should be the same")
	}
}

func avg(a []float64) (sum float64) {
	for i := range a {
		sum += a[i]
	}
	return sum / float64(len(a))
}

func stdDev(a []float64) (total float64) {
	prom := avg(a)
	for i := range a {
		total += (a[i] - prom) * (a[i] - prom)
	}
	total = total / float64(len(a))
	return math.Sqrt(total)
}

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func testBalance(t *testing.T, n int, nodes int, replicas int, fn Hash) {
	hash := New(replicas, fn)
	c := map[string]float64{}
	var keys []string
	for i := 0; i < nodes; i++ {
		node := fmt.Sprintf("node-%d", i)
		keys = append(keys, node)
		c[node] = 0
	}
	hash.Add(keys...)
	for i := 0; i < n; i++ {
		v := hash.Get(fmt.Sprintf("key-%d", i))
		c[v] += 1.0
	}

	var result []float64
	max, min := float64(-1), float64(-1)
	for _, v := range c {
		result = append(result, v)
		if max < v || max < 0 {
			max = v
		}
		if min > v || min < 0 {
			min = v
		}
	}
	t.Logf("  nodes = %-6dreplicas = %-6dAvg: %-9.2f Stddev: %-9.2f Max: %-9.2f Min: %-9.2f",
		nodes, replicas, avg(result), stdDev(result), max, min)
}

func testBalanceSuite(t *testing.T, fn Hash) {
	nArr := []int{1000, 50000, 200000}
	replicasArr := []int{10, 50, 128, 512}
	nodesArr := []int{5, 16, 128, 512}
	t.Logf("Testing balance with hash func: %s", getFunctionName(fn))
	for _, n := range nArr {
		t.Logf(" with n: %d", n)
		for _, replicas := range replicasArr {
			for _, nodes := range nodesArr {
				testBalance(t, n, nodes, replicas, fn)
			}
		}
	}
}

func TestBalance(t *testing.T) {
	testBalanceSuite(t, nil)
}

func TestBalanceXxhash(t *testing.T) {
	testBalanceSuite(t, xxhash.Checksum32)
}

func BenchmarkGet8(b *testing.B)          { benchmarkGet(b, 8, nil) }
func BenchmarkGet32(b *testing.B)         { benchmarkGet(b, 32, nil) }
func BenchmarkGet128(b *testing.B)        { benchmarkGet(b, 128, nil) }
func BenchmarkGet512(b *testing.B)        { benchmarkGet(b, 512, nil) }
func BenchmarkGet2048(b *testing.B)       { benchmarkGet(b, 2048, nil) }
func BenchmarkXxhashGet8(b *testing.B)    { benchmarkGet(b, 8, xxhash.Checksum32) }
func BenchmarkXxhashGet32(b *testing.B)   { benchmarkGet(b, 32, xxhash.Checksum32) }
func BenchmarkXxhashGet128(b *testing.B)  { benchmarkGet(b, 128, xxhash.Checksum32) }
func BenchmarkXxhashGet512(b *testing.B)  { benchmarkGet(b, 512, xxhash.Checksum32) }
func BenchmarkXxhashGet2048(b *testing.B) { benchmarkGet(b, 2048, xxhash.Checksum32) }

func benchmarkGet(b *testing.B, shards int, fn Hash) {
	hash := New(50, fn)

	var buckets []string
	for i := 0; i < shards; i++ {
		buckets = append(buckets, fmt.Sprintf("shard-%d", i))
	}

	hash.Add(buckets...)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hash.Get(buckets[i&(shards-1)])
	}
}
