package tree

import (
	"context"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
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
	cfg := schema.DefaultChunkConfig()
	cfgCid, err := ns.WriteChunkConfig(context.Background(), *cfg, nil)
	assert.NoError(t, err)

	nd := &schema.ProllyNode{
		Keys:        [][]byte{[]byte("123k")},
		Values:      []cid.Cid{c1},
		Level:       199998,
		ChunkConfig: cfgCid,
	}

	ctx := context.Background()

	c, err := ns.WriteNode(ctx, *nd, nil)
	assert.NoError(t, err)

	inode, err := ns.ReadNode(ctx, c)
	assert.NoError(t, err)

	_cfg, err := ns.ReadChunkCfg(context.Background(), inode.ChunkConfig)
	assert.NoError(t, err)

	assert.Equal(t, nd.Keys, inode.Keys)
	assert.Equal(t, nd.Values, inode.Values)
	assert.Equal(t, nd.Level, inode.Level)
	assert.Equal(t, nd.ChunkConfig, inode.ChunkConfig)
	assert.True(t, _cfg.Equal(cfg))
}
