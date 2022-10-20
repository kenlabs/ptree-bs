package tree

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/assert"
	"testing"
)

func TestMutablePTreeWriteAndGet(t *testing.T) {
	ctx := context.Background()
	ns := newTestNodeStore()

	data := make([][2][]byte, 1000)
	for i := range data {
		v := []byte(string(rune(i * 2)))
		data[i][0], data[i][1] = v, v
	}

	ck, err := NewEmptyChunker(ctx, ns, chunkSplitterCfg)
	assert.NoError(t, err)

	for _, pair := range data {
		err = ck.AddPair(ctx, pair[0], pair[1])
		assert.NoError(t, err)
	}

	root, err := ck.Done(ctx)
	assert.NoError(t, err)

	originPTree := NewStaticProllyTree(root, ns)

	inserts := make([][2][]byte, len(data))
	for i := range data {
		inserts[i][0] = []byte(string(rune(i*2 + 1)))
		inserts[i][1] = []byte(string(rune(i*2 + 1)))
	}

	var st StaticTree
	for _, ins := range inserts {
		mut := originPTree.Mutate()
		err = mut.Put(ctx, ins[0], ins[1])
		assert.NoError(t, err)

		st = materializePTree(t, mut)

		ok, err := st.Has(ctx, ins[0])
		assert.NoError(t, err)
		assert.True(t, ok)

		value, err := st.Get(ctx, ins[0])
		assert.NoError(t, err)
		assert.Equal(t, value, ins[1])
	}
}

// validates edit provider and materializes map
func materializePTree(t *testing.T, mut *MutableTree) StaticTree {
	ctx := context.Background()

	// ensure edits are provided in order
	err := mut.ApplyPending(ctx)
	require.NoError(t, err)
	iter := mut.mutations()
	prev, _ := iter.NextMutation(ctx)
	require.NotNil(t, prev)
	for {
		next, _ := iter.NextMutation(ctx)
		if next == nil {
			break
		}
		cmp := DefaultBytesCompare(prev, next)
		assert.True(t, cmp < 0)
		prev = next
	}

	m, err := mut.Tree(ctx)
	assert.NoError(t, err)
	return m
}
