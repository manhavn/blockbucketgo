# blockbucketgo

A tiny embedded key/value store backed by a single file.

`blockbucketgo` stores items as `(key []byte, data []byte)` in a file and provides simple operations:

- Put / Get / Delete by key
- Batch put
- List / pagination
- Find from a key
- “Queue style” consume via `ListLockDelete`

> Package docs: https://pkg.go.dev/github.com/manhavn/blockbucketgo

## Install

```bash
go get github.com/manhavn/blockbucketgo
```

## Quick start

```go
package main

import (
	"fmt"
	"os"

	"github.com/manhavn/blockbucketgo"
)

func main() {
	b := blockbucketgo.New("data.db")
	defer b.Close()

	key := []byte("test-key-001-99999999999999")
	val := []byte("test data value: 0123456789 abcdefgh")

	n, err := b.Set(blockbucketgo.Item{Key: key, Data: val})
	fmt.Println("Set:", n, err)

	k2, v2 := b.Get(key)
	fmt.Println("Get:", string(k2), "==>", string(v2))

	n, err = b.Delete(key)
	fmt.Println("Delete:", n, err == nil)

	_ = os.Remove("data.db")
}
```

## Batch insert

```go
items := []blockbucketgo.Item{ {Key: []byte("k1"), Data: []byte("v1")}, {Key: []byte("k2"), Data: []byte("v2")} }
count := b.SetMany(items)
fmt.Println("inserted:", count)
```

## Listing and pagination

```go
limit := uint8(10)

first := b.List(limit) // first page
next := b.ListNext(limit, 10) // skip N items
more := b.FindNext([]byte("k2"), limit, true) // from key (optionally after key)
```

## Queue-style consume (ListLockDelete)

`ListLockDelete(limit)` is intended for “consume-and-delete” workloads.
A typical pattern:

1. Producers push items with `SetMany` (or `Set`)
2. Consumers periodically call `ListLockDelete(limit)` to take a small batch

```go
limit := uint8(3)
batch := b.ListLockDelete(limit)
for _, it := range batch { fmt.Println(string(it.Key), "=>", string(it.Data)) }
```

## Notes

- Keys and values are `[]byte`. You control encoding (string/JSON/msgpack/...).
- Always call `Close()` to flush and release file handles.

## DEMO

```go
package main

import (
	"fmt"
	"os"

	"github.com/manhavn/blockbucketgo"
)

func main() {
	bucket := blockbucketgo.New("data.db")
	defer bucket.Close()

	testKey := []byte("test-key-001-99999999999999")
	testValue := []byte("test data value: 0123456789 abcdefgh")
	n, err := bucket.Set(blockbucketgo.Item{Key: testKey, Data: testValue})
	fmt.Println("bucket.Set ", n, err)

	keyBlock, valueBlock := bucket.Get(testKey)
	fmt.Println("bucket.Get ", string(keyBlock), "==>", string(valueBlock))

	n, err = bucket.Delete(testKey)
	fmt.Println("bucket.Delete ", n, err == nil)

	var listData []blockbucketgo.Item
	for i := 0; i < 10; i++ {
		testItemKey := []byte(fmt.Sprintf("test-key-00%d-99999999999999", i))
		listData = append(listData, blockbucketgo.Item{Key: testItemKey, Data: testValue})
		testValue = append(testValue, '@')
	}
	count := bucket.SetMany(listData)
	fmt.Println("bucket.SetMany", count)

	var limit uint8 = 10
	listBlock := bucket.List(limit)
	fmt.Println("bucket.List ", len(listBlock))

	var skip uint = 5
	listBlock = bucket.ListNext(limit, skip)
	fmt.Println("bucket.ListNext ", len(listBlock))

	onlyAfterKey := true
	onlyAfterKey = false
	listBlock = bucket.FindNext(testKey, limit, onlyAfterKey)
	fmt.Println("bucket.FindNext ", len(listBlock))

	alsoDeleteTheFoundBlock := false
	alsoDeleteTheFoundBlock = true
	err = bucket.DeleteTo(testKey, alsoDeleteTheFoundBlock)
	fmt.Println("bucket.DeleteTo ", err == nil)

	limit = 3
	listBlock = bucket.ListLockDelete(limit)
	fmt.Println("bucket.ListLockDelete ", listBlock)

	data, err := os.ReadFile("data.db")
	if err != nil {
		return
	}
	fmt.Println("os.ReadFile ", data)

	_ = os.RemoveAll("data.db")
}
```

# DEMO queue app

```go
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/manhavn/blockbucketgo"
)

func main() {
	go func() {
		for {
			addQueue()
			time.Sleep(time.Second * 20)
		}
	}()

	for {
		runQueue()
		time.Sleep(time.Second * 2)
	}
}

func addQueue() {
	bucket := blockbucketgo.New("data.db")
	defer bucket.Close()

	timeNow := time.Now()
	var listData []blockbucketgo.Item
	for i := 0; i < 10; i++ {
		testItemKey := []byte(fmt.Sprintf("test-key-%d-%s", i, timeNow.Format(time.RFC3339)))
		testItemValue := []byte(fmt.Sprintf("test-data-%d-%s", i, timeNow.Format(time.RFC3339)))
		listData = append(listData, blockbucketgo.Item{Key: testItemKey, Data: testItemValue})
	}
	count := bucket.SetMany(listData)

	info, err := os.Stat("data.db")
	if err != nil {
		return
	}
	fmt.Printf("Queue added %d, Bucket file size %d bytes\n", count, info.Size())
}

func runQueue() {
	bucket := blockbucketgo.New("data.db")
	defer bucket.Close()

	var limit uint8 = 3
	// listBlock := bucket.List(limit)
	listBlock := bucket.ListLockDelete(limit)

	// var endKey []byte
	for i := 0; i < len(listBlock); i++ {
		item := listBlock[i]
		key := item.Key
		value := item.Data
		// endKey = key

		fmt.Println(string(key), " ==> ", string(value))
		time.Sleep(time.Second / 2)
	}
	// _ = bucket.DeleteTo(endKey, true)

	info, err := os.Stat("data.db")
	if err != nil {
		return
	}
	fmt.Printf("Bucket file size %d bytes\n", info.Size())
}
```
