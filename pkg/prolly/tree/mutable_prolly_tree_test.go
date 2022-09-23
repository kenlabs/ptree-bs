package tree

import (
	"context"
	"github.com/ipfs/go-cid"
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

	ck, err := NewEmptyChunker(ctx, ns)
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

		assert.Equal(t, len(data)+1, st.Count())

		ok, err := st.Has(ctx, ins[0])
		assert.NoError(t, err)
		assert.True(t, ok)

		value, err := st.Get(ctx, ins[0])
		assert.NoError(t, err)
		assert.Equal(t, value, ins[1])
	}
}

func TestMPW(t *testing.T) {
	ctx := context.Background()
	ns := newTestNodeStore()

	data := make([][2][]byte, 422)
	for i := range data {
		v := []byte(string(rune(i * 2)))
		data[i][0], data[i][1] = v, v
	}

	ck, err := NewEmptyChunker(ctx, ns)
	assert.NoError(t, err)

	for _, pair := range data {
		err = ck.AddPair(ctx, pair[0], pair[1])
		assert.NoError(t, err)
	}

	root, err := ck.Done(ctx)
	assert.NoError(t, err)

	originPTree := NewStaticProllyTree(root, ns)
	inserts := make([][2][]byte, 2)
	for i := 0; i < 2; i++ {
		inserts[i][0] = []byte(string(rune(i*2 + 1)))
		inserts[i][1] = []byte(string(rune(i*2 + 1)))
	}

	var st StaticTree

	t.Log(bytesToCid(originPTree.Root.getValue(0)))
	t.Logf("%p\n", &originPTree.Root)

	t.Log(originPTree.Count())
	mut := originPTree.Mutate()
	err = mut.Put(ctx, inserts[0][0], inserts[0][1])
	t.Log(mut.tree.Count())
	assert.NoError(t, err)

	st, err = mut.Tree(ctx)
	assert.NoError(t, err)

	t.Log(bytesToCid(originPTree.Root.getValue(0)))
	t.Logf("%p\n", &originPTree.Root)

	assert.Equal(t, len(data)+1, st.Count())

	ok, err := st.Has(ctx, inserts[0][0])
	assert.NoError(t, err)
	assert.True(t, ok)

	value, err := st.Get(ctx, inserts[0][0])
	assert.NoError(t, err)
	assert.Equal(t, value, inserts[0][1])

	// second insert
	mut = originPTree.Mutate()
	err = mut.Put(ctx, inserts[1][0], inserts[1][1])
	t.Log(mut.tree.Count())
	assert.NoError(t, err)

	st, err = mut.Tree(ctx)
	assert.NoError(t, err)

	t.Log(bytesToCid(originPTree.Root.getValue(0)))
	t.Logf("%p\n", &originPTree.Root)

	assert.Equal(t, len(data)+1, st.Count())

	ok, err = st.Has(ctx, inserts[1][0])
	assert.NoError(t, err)
	assert.True(t, ok)

	value, err = st.Get(ctx, inserts[1][0])
	assert.NoError(t, err)
	assert.Equal(t, value, inserts[1][1])

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

func bytesToCid(data []byte) cid.Cid {
	_, c, err := cid.CidFromBytes(data)
	if err != nil {
		panic(err.Error())
	}
	return c
}
