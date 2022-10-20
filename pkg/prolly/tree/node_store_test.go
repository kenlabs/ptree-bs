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

func TestIPLDNodeStoreLoad(t *testing.T) {
	bs := blockstore.NewBlockstore(datastore.NewMapDatastore())
	ns, err := NewNodeStore(bs, &storeConfig{cacheSize: 1 << 10})
	assert.NoError(t, err)

	c1, err := schema.LinkProto.Sum([]byte("link1"))
	assert.NoError(t, err)
	var lnk1 ipld.Link
	lnk1 = cidlink.Link{Cid: c1}
	cfg := schema.DefaultChunkConfig()
	cfgCid, err := ns.WriteChunkConfig(context.Background(), *cfg)
	assert.NoError(t, err)

	nd := &schema.ProllyNode{
		Keys:   [][]byte{[]byte("123k")},
		Values: [][]byte{[]byte("123v")},
		Links:  []*ipld.Link{&lnk1},
		Level:  199998,
		Count:  25000,
		Cfg:    cfgCid,
	}

	ctx := context.Background()

	c, err := ns.Write(ctx, *nd)
	assert.NoError(t, err)

	inode, err := ns.Read(ctx, c)
	assert.NoError(t, err)

	_cfg, err := ns.ReadChunkCfg(context.Background(), inode.Cfg)
	assert.NoError(t, err)

	assert.Equal(t, nd.Keys, inode.Keys)
	assert.Equal(t, nd.Values, inode.Values)
	assert.Equal(t, nd.Level, inode.Level)
	assert.Equal(t, nd.Links, inode.Links)
	assert.Equal(t, nd.Count, inode.Count)
	assert.Equal(t, nd.Cfg, inode.Cfg)
	assert.True(t, _cfg.Equal(cfg))
}
