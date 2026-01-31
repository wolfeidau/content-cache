package metadb

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// BenchmarkPutMetaUpdate measures update performance with varying DB sizes.
// The key metric is whether time scales with entry count (O(n)) or stays constant (O(1)).
func BenchmarkPutMetaUpdate(b *testing.B) {
	for _, n := range []int{100, 500, 1000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			db := NewBoltDB()
			dbPath := filepath.Join(b.TempDir(), "bench.db")
			if err := db.Open(dbPath); err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			ctx := context.Background()

			// Pre-populate
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("pkg-%d", i)
				if err := db.PutMeta(ctx, "npm", key, []byte("data"), time.Hour); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("pkg-%d", i%n)
				if err := db.PutMeta(ctx, "npm", key, []byte("updated"), time.Hour); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkTouchBlobUpdate measures touch performance with varying DB sizes.
func BenchmarkTouchBlobUpdate(b *testing.B) {
	for _, n := range []int{100, 500, 1000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
			currentTime := baseTime
			db := NewBoltDB(WithNow(func() time.Time { return currentTime }))
			dbPath := filepath.Join(b.TempDir(), "bench.db")
			if err := db.Open(dbPath); err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			ctx := context.Background()

			// Pre-populate
			for i := 0; i < n; i++ {
				currentTime = baseTime.Add(time.Duration(i) * time.Minute)
				hash := fmt.Sprintf("hash-%d", i)
				entry := &BlobEntry{
					Hash:       hash,
					Size:       1024,
					CachedAt:   currentTime,
					LastAccess: currentTime,
					RefCount:   1,
				}
				if err := db.PutBlob(ctx, entry); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				currentTime = baseTime.Add(time.Duration(n+i) * time.Minute)
				hash := fmt.Sprintf("hash-%d", i%n)
				if err := db.TouchBlob(ctx, hash); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
