package tree

import (
	"context"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/zeebo/assert"
	"testing"
)

func TestNodeStoreWriteRead(t *testing.T) {
	bs := blockstore.NewBlockstore(datastore.NewMapDatastore())
	ns := NewNodeStore(bs)
	ctx := context.Background()

	n := &Node{
		Keys:     [][]byte{[]byte("123key")},
		Values:   [][]byte{[]byte("123val")},
		Size:     1234,
		Level:    -1,
		Subtrees: []uint64{uint64(1234)},
	}
	c, err := ns.Write(ctx, n)
	assert.NoError(t, err)
	n2, err := ns.Read(ctx, c)
	assert.NoError(t, err)
	assert.DeepEqual(t, n, n2)

}
