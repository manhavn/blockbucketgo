# SETUP

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

	data, err := os.ReadFile("data.db")
	fmt.Println("os.ReadFile ", data, err)

	_ = os.RemoveAll("data.db")
}
```
