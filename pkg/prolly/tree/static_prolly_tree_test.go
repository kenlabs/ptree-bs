package tree

import (
	"context"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/zeebo/assert"
	"math/rand"
	"testing"
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
