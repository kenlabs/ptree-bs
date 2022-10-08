package tree

import (
	"context"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/zeebo/assert"
	"math/rand"
	"testing"
	"time"
)

func newTestNodeStore() *NodeStore {
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(ds)
	ns, _ := NewNodeStore(bs, &storeConfig{cacheSize: 1 << 16})
	return ns
}

func TestCreateStaticMapAndGet(t *testing.T) {
	ctx := context.Background()
	ns := newTestNodeStore()
	testdata := RandomTuplePairs(10000)

	SetGlobalChunkConfig(&ChunkConfig{
		ChunkStrategy: RollingHash,
		RollingHashCfg: &RollingHashConfig{
			RollingHashWindow: 67,
		},
	})

	ck, err := NewEmptyChunker(ctx, ns)
	assert.NoError(t, err)
	for _, pair := range testdata {
		err = ck.AddPair(ctx, pair[0], pair[1])
		assert.NoError(t, err)
	}
	root, err := ck.Done(ctx)
	assert.NoError(t, err)
	//t.Log(root)

	st := NewStaticProllyTree(root, ns)

	for i := 0; i < 1000; i++ {
		idx := rand.Intn(10000)
		val, err := st.Get(ctx, testdata[idx][0])
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, val, testdata[idx][1])
	}

	for i := 0; i < 1000; i++ {
		ok, err := st.Has(ctx, testdata[i][0])
		if err != nil {
			t.Fatal(err)
		}
		assert.True(t, ok)
	}
}

func TestBenchmarkStaticTree(t *testing.T) {
	type testCase struct {
		dataLength int
		CacheSize  int
	}

	testCases := []testCase{
		{
			1000,
			0,
		},
		{
			10000,
			0,
		},
		{
			30000,
			1 << 24,
		},
		{
			30000,
			0,
		},
	}

	for _, ts := range testCases {
		testdata := RandomTuplePairs(ts.dataLength)
		ctx := context.Background()
		testDbDir := t.TempDir()
		ds, err := leveldb.NewDatastore(testDbDir, nil)
		assert.NoError(t, err)
		bs := blockstore.NewBlockstore(ds)
		ns, err := NewNodeStore(bs, &storeConfig{cacheSize: ts.CacheSize})
		assert.NoError(t, err)

		startTime := time.Now()

		ck, err := NewEmptyChunker(ctx, ns)
		assert.NoError(t, err)
		for _, pair := range testdata {
			err = ck.AddPair(ctx, pair[0], pair[1])
			assert.NoError(t, err)
		}
		root, err := ck.Done(ctx)
		assert.NoError(t, err)

		st := NewStaticProllyTree(root, ns)

		costTime := time.Since(startTime)
		t.Logf("%#v cost time: %v", ts, costTime)

		for i := 0; i < ts.dataLength/100; i++ {
			idx := rand.Intn(ts.dataLength)
			val, err := st.Get(ctx, testdata[idx][0])
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, val, testdata[idx][1])
		}

		for i := 0; i < ts.dataLength/100; i++ {
			idx := rand.Intn(ts.dataLength)
			ok, err := st.Has(ctx, testdata[idx][0])
			if err != nil {
				t.Fatal(err)
			}
			assert.True(t, ok)
		}

		_ = ds.Close()
	}

}
