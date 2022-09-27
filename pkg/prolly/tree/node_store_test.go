package tree

import (
	"context"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	_ "github.com/multiformats/go-multicodec"
	"github.com/zeebo/assert"
	"ptree-bs/pkg/prolly/tree/schema"

	"testing"
)

//func TestNodeStoreWriteRead(t *testing.T) {
//	bs := blockstore.NewBlockstore(datastore.NewMapDatastore())
//	ns := NewNodeStore(bs)
//	ctx := context.Background()
//
//	n := Node{
//		Keys:     [][]byte{[]byte("123key")},
//		Values:   [][]byte{[]byte("123val")},
//		Size:     1234,
//		Level:    -1,
//		Subtrees: []uint64{uint64(1234)},
//	}
//	c, err := ns.Write(ctx, n)
//	assert.NoError(t, err)
//	n2, err := ns.Read(ctx, c)
//	assert.NoError(t, err)
//	assert.DeepEqual(t, n, n2)
//
//}

func TestIPLDNodeStoreLoad(t *testing.T) {
	c1, err := LinkProto.Sum([]byte("link1"))
	assert.NoError(t, err)
	var lnk1 ipld.Link
	lnk1 = cidlink.Link{Cid: c1}

	nd := &schema.ProllyNode{
		Keys:       [][]byte{[]byte("123k")},
		Values:     [][]byte{[]byte("123v")},
		Links:      []*ipld.Link{&lnk1},
		Size:       199999,
		Level:      199998,
		Count:      25000,
		Subtrees:   []uint64{1, 2, 5},
		Totalcount: 1,
	}

	bs := blockstore.NewBlockstore(datastore.NewMapDatastore())
	ns := NewNodeStore(bs)
	ctx := context.Background()

	c, err := ns.Write(ctx, *nd)
	assert.NoError(t, err)

	inode, err := ns.Read(ctx, c)
	assert.NoError(t, err)

	assert.Equal(t, nd.Keys, inode.Keys)
	assert.Equal(t, nd.Values, inode.Values)
	assert.Equal(t, nd.Level, inode.Level)
	assert.Equal(t, nd.Links, inode.Links)
	assert.Equal(t, nd.Size, inode.Size)
	assert.Equal(t, nd.Count, inode.Count)
	assert.Equal(t, nd.Totalcount, inode.Totalcount)
	assert.Equal(t, nd.Subtrees, inode.Subtrees)
}
