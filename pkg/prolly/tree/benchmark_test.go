package tree

import (
	"context"
	leveldb "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"ptree-bs/pkg/prolly/tree/schema"
	"testing"
	"time"
)

var (
	chunkSplitterCfg    = schema.DefaultChunkConfig()
	chunkRollingHashCfg = &schema.ChunkConfig{
		MinChunkSize:  1 << 9,
		MaxChunkSize:  1 << 14,
		ChunkStrategy: schema.RollingHash,
		RollingHashCfg: &schema.RollingHashConfig{
			RollingHashWindow: 67,
		},
	}
)

func createStaticTreeFromData(ctx context.Context, t *testing.T, cacheSize int, data [][2][]byte, cfg *schema.ChunkConfig) (*StaticTree, *leveldb.Datastore) {
	testDbDir := t.TempDir()
	ds, err := leveldb.NewDatastore(testDbDir, nil)
	assert.NoError(t, err)
	bs := blockstore.NewBlockstore(ds)
	ns, err := NewNodeStore(bs, &storeConfig{cacheSize: cacheSize})
	assert.NoError(t, err)

	startTime := time.Now()

	ck, err := NewEmptyChunker(ctx, ns, cfg)
	assert.NoError(t, err)
	for _, pair := range data {
		err = ck.AddPair(ctx, pair[0], pair[1])
		assert.NoError(t, err)
	}
	root, err := ck.Done(ctx)
	assert.NoError(t, err)

	st := NewStaticProllyTree(root, ns)

	costTime := time.Since(startTime)
	t.Logf("Creating tree(size:%d) with cache(size:%d) and %s costs time: %v", len(data), cacheSize, cfg.ChunkStrategy, costTime)

	return st, ds
}

func TestBuildStaticTree(t *testing.T) {
	type testCase struct {
		dataLength int
		CacheSize  int
	}

	testCases := []testCase{
		{
			1000,
			0,
		},
		{
			10000,
			0,
		},
		{
			100000,
			1 << 20,
		},
		{
			100000,
			0,
		},
	}

	for _, ts := range testCases {
		testdata := RandomTuplePairs(ts.dataLength)
		ctx := context.Background()

		st, ds := createStaticTreeFromData(ctx, t, ts.CacheSize, testdata, chunkSplitterCfg)

		for i := 0; i < ts.dataLength/1000; i++ {
			idx := rand.Intn(ts.dataLength)
			val, err := st.Get(ctx, testdata[idx][0])
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, val, testdata[idx][1])
		}

		for i := 0; i < ts.dataLength/1000; i++ {
			idx := rand.Intn(ts.dataLength)
			ok, err := st.Has(ctx, testdata[idx][0])
			if err != nil {
				t.Fatal(err)
			}
			assert.True(t, ok)
		}

		_ = ds.Close()
	}
}

func Test500KStaticTreeRead50KWithoutCache(t *testing.T) {
	// build statictree from 500000 pairs of kvs
	dataLength := 500000
	ctx := context.Background()

	//	key := make([]byte, (testRand.Int63()%30)+15)
	//	val := make([]byte, (testRand.Int63()%30)+15)
	//  avg size of k/v is 30 bytes
	testdata := RandomTuplePairs(dataLength)

	st, ds := createStaticTreeFromData(ctx, t, 0, testdata, chunkSplitterCfg)

	readStartTime := time.Now()
	for i := 0; i < dataLength/10; i++ {
		idx := rand.Intn(dataLength)
		val, err := st.Get(ctx, testdata[idx][0])
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, val, testdata[idx][1])
	}
	readCostTime := time.Since(readStartTime)
	t.Logf("rand reading %d data costs time: %v", dataLength/10, readCostTime)

	_ = ds.Close()
}

func Test500KStaticTreeRead50KWithoutCacheRollingHash(t *testing.T) {
	dataLength := 500000
	ctx := context.Background()

	testdata := RandomTuplePairs(dataLength)

	st, ds := createStaticTreeFromData(ctx, t, 0, testdata, chunkRollingHashCfg)

	readStartTime := time.Now()
	for i := 0; i < dataLength/10; i++ {
		idx := rand.Intn(dataLength)
		val, err := st.Get(ctx, testdata[idx][0])
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, val, testdata[idx][1])
	}
	readCostTime := time.Since(readStartTime)
	t.Logf("rand reading %d data costs time: %v", dataLength/10, readCostTime)

	_ = ds.Close()
}

func Test500KStaticTreeRead50KWith16KCache(t *testing.T) {
	dataLength := 500000
	testdata := RandomTuplePairs(dataLength)
	ctx := context.Background()

	st, ds := createStaticTreeFromData(ctx, t, 1<<14, testdata, chunkSplitterCfg)

	readStartTime := time.Now()
	for i := 0; i < dataLength/10; i++ {
		idx := rand.Intn(dataLength)
		val, err := st.Get(ctx, testdata[idx][0])
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, val, testdata[idx][1])
	}
	readCostTime := time.Since(readStartTime)
	t.Logf("rand reading %d data costs time: %v", dataLength/10, readCostTime)

	_ = ds.Close()
}

func Test500KStaticTreeRead50KWith16KCacheRollingHash(t *testing.T) {
	dataLength := 500000
	testdata := RandomTuplePairs(dataLength)
	ctx := context.Background()

	st, ds := createStaticTreeFromData(ctx, t, 1<<14, testdata, chunkRollingHashCfg)

	readStartTime := time.Now()
	for i := 0; i < dataLength/10; i++ {
		idx := rand.Intn(dataLength)
		val, err := st.Get(ctx, testdata[idx][0])
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, val, testdata[idx][1])
	}
	readCostTime := time.Since(readStartTime)
	t.Logf("rand reading %d data costs time: %v", dataLength/10, readCostTime)

	_ = ds.Close()
}

func Test500KStaticTreeRead50KWith4KCache(t *testing.T) {
	dataLength := 500000
	testdata := RandomTuplePairs(dataLength)
	ctx := context.Background()

	st, ds := createStaticTreeFromData(ctx, t, 1<<12, testdata, chunkSplitterCfg)

	readStartTime := time.Now()
	for i := 0; i < dataLength/10; i++ {
		idx := rand.Intn(dataLength)
		val, err := st.Get(ctx, testdata[idx][0])
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, val, testdata[idx][1])
	}
	readCostTime := time.Since(readStartTime)
	t.Logf("rand reading %d data costs time: %v", dataLength/10, readCostTime)

	_ = ds.Close()
}

func Test500KStaticTreeRead50KWith4KCacheRollingHash(t *testing.T) {
	dataLength := 500000
	testdata := RandomTuplePairs(dataLength)
	ctx := context.Background()

	st, ds := createStaticTreeFromData(ctx, t, 1<<12, testdata, chunkRollingHashCfg)

	readStartTime := time.Now()
	for i := 0; i < dataLength/10; i++ {
		idx := rand.Intn(dataLength)
		val, err := st.Get(ctx, testdata[idx][0])
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, val, testdata[idx][1])
	}
	readCostTime := time.Since(readStartTime)
	t.Logf("rand reading %d data costs time: %v", dataLength/10, readCostTime)

	_ = ds.Close()
}

func TestCreateTreeAndMutateRandom(t *testing.T) {
	type testCase struct {
		staticTreeLen int
		insertLen     int
		cacheSize     int
	}

	testCases := []testCase{
		{
			1000,
			500,
			1 << 15,
		},
		{
			1000,
			500,
			0,
		},
		{
			10000,
			5000,
			1 << 18,
		},
		{
			10000,
			5000,
			0,
		},
		{
			100000,
			50000,
			0,
		},
		{
			100000,
			50000,
			1 << 24,
		},
	}

	for i := 0; i < 2; i++ {
		var cfg *schema.ChunkConfig
		if i == 1 {
			cfg = chunkRollingHashCfg
		} else {
			cfg = chunkSplitterCfg
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
			ck, err := NewEmptyChunker(ctx, ns, cfg)
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
			t.Logf("%#v strategy: %s costs time: %v", tc, cfg.ChunkStrategy, costTime)

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
}
