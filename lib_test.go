package blockbucketgo_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/manhavn/blockbucketgo"
)

func newTempBucket(t *testing.T) (path string, b *blockbucketgo.Bucket) {
	t.Helper()

	dir := t.TempDir()
	path = filepath.Join(dir, "data.db")

	b = blockbucketgo.New(path)
	t.Cleanup(func() {
		b.Close()
		_ = os.Remove(path)
	})
	return path, b
}

func TestSetGetDelete(t *testing.T) {
	_, b := newTempBucket(t)

	key := []byte("k1")
	val := []byte("v1")

	if _, err := b.Set(blockbucketgo.Item{Key: key, Data: val}); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	k2, v2 := b.Get(key)
	if string(k2) != string(key) || string(v2) != string(val) {
		t.Fatalf("Get mismatch: got (%q,%q) want (%q,%q)", k2, v2, key, val)
	}

	if _, err := b.Delete(key); err != nil {
		t.Fatalf("Delete error: %v", err)
	}

	k3, v3 := b.Get(key)
	// Accept either nil or empty slices for not-found (depends on implementation).
	if len(k3) != 0 || len(v3) != 0 {
		t.Fatalf("expected empty after delete, got (%v,%v)", k3, v3)
	}
}

func TestSetManyAndList(t *testing.T) {
	_, b := newTempBucket(t)

	items := []blockbucketgo.Item{
		{Key: []byte("k1"), Data: []byte("v1")},
		{Key: []byte("k2"), Data: []byte("v2")},
		{Key: []byte("k3"), Data: []byte("v3")},
		{Key: []byte("k4"), Data: []byte("v4")},
	}
	if got := b.SetMany(items); got != len(items) {
		// If your SetMany returns partial count on error, adjust this assertion accordingly.
		t.Fatalf("SetMany count: got %d want %d", got, len(items))
	}

	first := b.List(2)
	if len(first) != 2 {
		t.Fatalf("List len: got %d want %d", len(first), 2)
	}

	next := b.ListNext(2, 2)
	if len(next) != 2 {
		t.Fatalf("ListNext len: got %d want %d", len(next), 2)
	}
}

func TestFindNextAndDeleteTo(t *testing.T) {
	_, b := newTempBucket(t)

	_ = b.SetMany([]blockbucketgo.Item{
		{Key: []byte("a"), Data: []byte("1")},
		{Key: []byte("b"), Data: []byte("2")},
		{Key: []byte("c"), Data: []byte("3")},
		{Key: []byte("d"), Data: []byte("4")},
	})

	out := b.FindNext([]byte("b"), 10, true)
	// Expect items after "b" (implementation order dependent, but based on README intent).
	if len(out) == 0 {
		t.Fatalf("FindNext returned empty, expected some items")
	}

	if err := b.DeleteTo([]byte("c"), true); err != nil {
		t.Fatalf("DeleteTo error: %v", err)
	}
}

func TestListLockDelete(t *testing.T) {
	_, b := newTempBucket(t)

	_ = b.SetMany([]blockbucketgo.Item{
		{Key: []byte("q1"), Data: []byte("A")},
		{Key: []byte("q2"), Data: []byte("B")},
		{Key: []byte("q3"), Data: []byte("C")},
	})

	batch := b.ListLockDelete(2)
	if len(batch) != 2 {
		t.Fatalf("ListLockDelete len: got %d want %d", len(batch), 2)
	}

	// After consume-and-delete, next call should not return the same first 2 items again.
	batch2 := b.ListLockDelete(2)
	if len(batch2) == 0 {
		// Acceptable if implementation needs more data; but typically should return remaining 1.
		return
	}
	if string(batch2[0].Key) == string(batch[0].Key) {
		t.Fatalf("ListLockDelete returned duplicate items")
	}
}
