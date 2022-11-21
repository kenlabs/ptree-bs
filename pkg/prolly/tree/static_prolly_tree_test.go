package tree

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	car2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/zeebo/assert"
	"math/rand"
	"os"
	"testing"
)

func newTestNodeStore() *NodeStore {
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(ds)
	ns, _ := NewNodeStore(bs, &storeConfig{cacheSize: 1 << 16})
	return ns
}

// ExploreRecursiveWithStopNode builds a selector that recursively syncs a DAG
// until the link stopLnk is seen. It prevents from having to sync DAGs from
// scratch with every update.
func ExploreRecursiveWithStopNode(limit selector.RecursionLimit, sequence ipld.Node, stopLnk ipld.Link) ipld.Node {
	if sequence == nil {
		np := basicnode.Prototype__Any{}
		ssb := selectorbuilder.NewSelectorSpecBuilder(np)
		sequence = ssb.ExploreAll(ssb.ExploreRecursiveEdge()).Node()
	}
	np := basicnode.Prototype__Map{}
	return fluent.MustBuildMap(np, 1, func(na fluent.MapAssembler) {
		// RecursionLimit
		na.AssembleEntry(selector.SelectorKey_ExploreRecursive).CreateMap(3, func(na fluent.MapAssembler) {
			na.AssembleEntry(selector.SelectorKey_Limit).CreateMap(1, func(na fluent.MapAssembler) {
				switch limit.Mode() {
				case selector.RecursionLimit_Depth:
					na.AssembleEntry(selector.SelectorKey_LimitDepth).AssignInt(limit.Depth())
				case selector.RecursionLimit_None:
					na.AssembleEntry(selector.SelectorKey_LimitNone).CreateMap(0, func(na fluent.MapAssembler) {})
				default:
					panic("Unsupported recursion limit type")
				}
			})
			// Sequence
			na.AssembleEntry(selector.SelectorKey_Sequence).AssignNode(sequence)

			// Stop condition
			if stopLnk != nil {
				cond := fluent.MustBuildMap(basicnode.Prototype__Map{}, 1, func(na fluent.MapAssembler) {
					na.AssembleEntry(string(selector.ConditionMode_Link)).AssignLink(stopLnk)
				})
				na.AssembleEntry(selector.SelectorKey_StopAt).AssignNode(cond)
			}
		})
	})
}

func TestCreateStaticMapAndGet(t *testing.T) {
	ctx := context.Background()
	ns := newTestNodeStore()
	testdata := RandomTuplePairs(10000)

	ck, err := NewEmptyChunker(ctx, ns, chunkSplitterCfg)
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

	for i := 0; i < 1000; i++ {
		idx := rand.Intn(10000)
		val, err := st.Get(ctx, testdata[idx][0])
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, val, testdata[idx][1])
	}

	for i := 0; i < 1000; i++ {
		ok, err := st.Has(ctx, testdata[i][0])
		if err != nil {
			t.Fatal(err)
		}
		assert.True(t, ok)
	}

	assert.NoError(t, err)

	// generated the new fixtures when version changes
	t.SkipNow()
	rootCid, err := ns.WriteNode(ctx, root, nil)
	assert.NoError(t, err)
	buf := new(bytes.Buffer)

	size, err := car2.TraverseV1(ctx, ns.lsys, rootCid, ExploreRecursiveWithStopNode(selector.RecursionLimitDepth(int64(root.Level)), nil, nil), buf)
	assert.NoError(t, err)
	carFile, err := os.OpenFile("./fixtures/10000RandCids/tree.car", os.O_CREATE|os.O_WRONLY, 0666)
	defer carFile.Close()
	assert.NoError(t, err)
	n, err := carFile.Write(buf.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, n, size)

	data, err := json.Marshal(testdata)
	assert.NoError(t, err)
	dataFile, err := os.OpenFile("./fixtures/10000RandCids/data", os.O_CREATE|os.O_WRONLY, 0666)
	defer dataFile.Close()
	assert.NoError(t, err)
	_, err = dataFile.Write(data)
	assert.NoError(t, err)
}
