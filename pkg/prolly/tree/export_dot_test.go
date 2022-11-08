package tree

import (
	"context"
	"github.com/stretchr/testify/assert"
	"ptree-bs/pkg/prolly/tree/schema"
	"testing"
)

func TestCreateStaticMapAndGetAndOutputDot(t *testing.T) {
	ctx := context.Background()
	ns := newTestNodeStore()
	testdata := RandomStringTuplePairs(20)

	// special config, make tree deeper to display
	cfg := &schema.ChunkConfig{
		MinChunkSize:  1 << 3,
		MaxChunkSize:  1 << 8,
		ChunkStrategy: schema.KeySplitter,
		KeySplitterCfg: &schema.KeySplitterConfig{
			K: 4,
			L: 100,
		},
		RollingHashCfg: &schema.RollingHashConfig{
			RollingHashWindow: 67,
		},
	}

	ck, err := NewEmptyChunker(ctx, ns, cfg)
	assert.NoError(t, err)
	for _, pair := range testdata {
		err = ck.AddPair(ctx, pair[0], pair[1])
		assert.NoError(t, err)
	}
	root, err := ck.Done(ctx)
	assert.NoError(t, err)
	//t.Log(root)

	st, err := LoadProllyTreeFromRootNode(root, ns)
	assert.NoError(t, err)

	_, err = ExportTreeToDot(ctx, st, false, "Tree1")
	assert.NoError(t, err)

	mutTree := st.Mutate()

	t.Logf("delete key :%s", testdata[0][0])
	err = mutTree.Delete(ctx, testdata[0][0])
	assert.NoError(t, err)

	st, err = mutTree.Tree(ctx)
	assert.NoError(t, err)

	_, err = ExportTreeToDot(ctx, st, false, "Tree2")
	assert.NoError(t, err)
}
