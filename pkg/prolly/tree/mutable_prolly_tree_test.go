package tree

import (
	"context"
	leveldb "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/assert"
	"math/rand"
	"testing"
	"time"
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

func TestBenchmarkMutableTree(t *testing.T) {
	type testCase struct {
		staticTreeLen int
		insertLen     int
		cacheSize     int
	}

	testCases := []testCase{
		{
			3000,
			3000,
			1 << 22,
		},
		{
			3000,
			3000,
			0,
		},
		{
			20000,
			10000,
			1 << 24,
		},
		{
			20000,
			10000,
			0,
		},
	}

	for _, tc := range testCases {
		testdata := RandomTuplePairs(tc.staticTreeLen)
		ctx := context.Background()
		testDbDir := t.TempDir()
		ds, err := leveldb.NewDatastore(testDbDir, nil)
		assert.NoError(t, err)
		bs := blockstore.NewBlockstore(ds)
		ns, err := NewNodeStore(bs, &storeConfig{cacheSize: tc.cacheSize})
		assert.NoError(t, err)

		stime := time.Now()
		ck, err := NewEmptyChunker(ctx, ns)
		assert.NoError(t, err)

		for _, pair := range testdata {
			err = ck.AddPair(ctx, pair[0], pair[1])
			assert.NoError(t, err)
		}

		root, err := ck.Done(ctx)
		assert.NoError(t, err)

		originPTree := NewStaticProllyTree(root, ns)
		insertsData := RandomTuplePairs(tc.insertLen)

		var st StaticTree
		mut := originPTree.Mutate()
		for _, ins := range insertsData {
			err = mut.Put(ctx, ins[0], ins[1])
			assert.NoError(t, err)

		}
		st = materializePTree(t, mut)
		costTime := time.Since(stime)
		t.Logf("%#v costs time: %v", tc, costTime)

		totalData := append(testdata, insertsData...)
		for i := 0; i < len(totalData)/10; i++ {
			idx := rand.Intn(len(totalData))

			ok, err := st.Has(ctx, totalData[idx][0])
			assert.NoError(t, err)
			assert.True(t, ok)

			value, err := st.Get(ctx, totalData[idx][0])
			assert.NoError(t, err)
			assert.Equal(t, value, totalData[idx][1])
		}

		_ = ds.Close()
	}
}
