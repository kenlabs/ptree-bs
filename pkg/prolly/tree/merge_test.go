package tree

import (
	"context"
	"github.com/zeebo/assert"
	"testing"
)

func TestMergeWithoutOverlap(t *testing.T) {
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

	st1, err := LoadProllyTreeFromRootNode(root, ns)
	assert.NoError(t, err)

	data2 := make([][2][]byte, 1000)
	for i := range data2 {
		v := []byte(string(rune(i*2 + 1)))
		data2[i][0], data2[i][1] = v, v
	}
	ck2, err := NewEmptyChunker(ctx, ns, chunkSplitterCfg)
	assert.NoError(t, err)
	for _, pair := range data2 {
		err = ck2.AddPair(ctx, pair[0], pair[1])
		assert.NoError(t, err)
	}
	root2, err := ck2.Done(ctx)
	assert.NoError(t, err)
	t.Log(root2)

	st2, err := LoadProllyTreeFromRootNode(root2, ns)
	assert.NoError(t, err)

	newTree, err := MergeStaticTrees(ctx, st1, st2)
	assert.NoError(t, err)
	assert.Equal(t, newTree.ChunkCfg, st1.ChunkCfg)
	assert.Equal(t, newTree.ChunkCfg, st2.ChunkCfg)
	assert.Equal(t, st1.ChunkCfg, st2.ChunkCfg)

	t.Log(newTree)

	totalData := append(data, data2...)
	for i := range totalData {
		v, err := newTree.Get(ctx, totalData[i][0])
		assert.NoError(t, err)
		assert.Equal(t, v, totalData[i][1])
	}
}

func TestMergeWithOverlap(t *testing.T) {
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

	st1, err := LoadProllyTreeFromRootNode(root, ns)
	assert.NoError(t, err)

	data2 := make([][2][]byte, 1500)
	for i := range data2 {
		k := []byte(string(rune(i * 2)))
		v := []byte(string(rune(i*3 + 1)))
		data2[i][0], data2[i][1] = k, v
	}
	ck2, err := NewEmptyChunker(ctx, ns, chunkSplitterCfg)
	assert.NoError(t, err)
	for _, pair := range data2 {
		err = ck2.AddPair(ctx, pair[0], pair[1])
		assert.NoError(t, err)
	}
	root2, err := ck2.Done(ctx)
	assert.NoError(t, err)

	st2, err := LoadProllyTreeFromRootNode(root2, ns)
	assert.NoError(t, err)

	newTree, err := MergeStaticTrees(ctx, st1, st2)
	assert.NoError(t, err)

	for i := range data2 {
		v, err := newTree.Get(ctx, data2[i][0])
		assert.NoError(t, err)
		assert.Equal(t, v, data2[i][1])
	}
}
