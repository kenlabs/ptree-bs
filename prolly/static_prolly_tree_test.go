package prolly

import (
	"context"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/zeebo/assert"
	"math/rand"
	"ptree-bs/prolly/tree"
	"testing"
)

func newTestNodeStore() *tree.NodeStore {
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(ds)
	ns := tree.NewNodeStore(bs)
	return ns
}

func TestCreateStaticMapAndGet(t *testing.T) {
	ctx := context.Background()
	ns := newTestNodeStore()
	testdata := RandomTuplePairs(100000)

	ck, err := tree.NewEmptyChunker(ctx, ns)
	assert.NoError(t, err)
	for _, pair := range testdata {
		err = ck.AddPair(ctx, pair[0], pair[1])
		assert.NoError(t, err)
	}
	root, err := ck.Done(ctx)
	assert.NoError(t, err)
	t.Log(root)

	st := NewStaticProllyTree(root, ns)

	for i := 0; i < 10000; i++ {
		idx := rand.Intn(100000)
		val, err := st.Get(ctx, testdata[idx][0])
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, val, testdata[idx][1])
	}

}
