package blockbucketgo_test

import (
	"fmt"
	"os"

	"github.com/manhavn/blockbucketgo"
)

func ExampleBucket_Set_and_Get() {
	path := "example.db"
	defer os.Remove(path)

	b := blockbucketgo.New(path)
	defer b.Close()

	_, _ = b.Set(blockbucketgo.Item{
		Key:  []byte("k1"),
		Data: []byte("v1"),
	})

	k, v := b.Get([]byte("k1"))
	fmt.Println(string(k), string(v))

	// Output:
	// k1 v1
}

func ExampleBucket_ListLockDelete_queueStyle() {
	path := "queue.db"
	defer os.Remove(path)

	b := blockbucketgo.New(path)
	defer b.Close()

	_ = b.SetMany([]blockbucketgo.Item{
		{Key: []byte("job-1"), Data: []byte("A")},
		{Key: []byte("job-2"), Data: []byte("B")},
		{Key: []byte("job-3"), Data: []byte("C")},
	})

	batch := b.ListLockDelete(2)
	for _, it := range batch {
		fmt.Println(string(it.Key), string(it.Data))
	}

	// Output:
	// job-1 A
	// job-2 B
}
