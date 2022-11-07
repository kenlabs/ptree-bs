package tree

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	car2 "github.com/ipld/go-car/v2"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/zeebo/assert"
	"os"
	"testing"
)

type fixtureSet struct {
	testData [][2][]byte
	tree     *StaticTree
	rootCid  cid.Cid
	carSize  int
}

func TestFixtures(t *testing.T) {
	dirs, err := os.ReadDir("./fixtures/")
	assert.NoError(t, err)
	for _, dir := range dirs {
		fixtureName := dir.Name()
		if !dir.IsDir() {
			continue
		}
		t.Run(fixtureName, func(t *testing.T) {
			fset, err := loadFixture(fixtureName)
			assert.NoError(t, err)
			verifyTree(t, fset)
		})
	}

}

func loadFixture(dir string) (*fixtureSet, error) {
	var data [][2][]byte
	dataSrc, err := os.ReadFile("./fixtures/" + dir + "/data")
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(dataSrc, &data)
	if err != nil {
		return nil, err
	}

	treeSrc, err := os.ReadFile("./fixtures/" + dir + "/tree.car")
	if err != nil {
		return nil, err
	}
	ns := newTestNodeStore()
	ch, err := car.LoadCar(context.Background(), ns.bs, bytes.NewBuffer(treeSrc))
	if err != nil {
		return nil, err
	}
	if len(ch.Roots) != 1 {
		panic("invalid root cid number")
	}

	tree, err := LoadProllyTreeFromRootCid(ch.Roots[0], ns)
	if err != nil {
		return nil, err
	}

	return &fixtureSet{
		testData: data,
		tree:     tree,
		rootCid:  ch.Roots[0],
		carSize:  len(treeSrc),
	}, nil
}

func verifyTree(t *testing.T, fset *fixtureSet) {
	ctx := context.Background()
	ns := newTestNodeStore()

	// build tree.car from the same data and config
	ck, err := NewEmptyChunker(ctx, ns, fset.tree.ChunkCfg)
	assert.NoError(t, err)
	for _, pair := range fset.testData {
		err = ck.AddPair(ctx, pair[0], pair[1])
		assert.NoError(t, err)
	}
	root, err := ck.Done(ctx)
	assert.NoError(t, err)

	st, err := LoadProllyTreeFromRootNode(root, ns)
	assert.NoError(t, err)

	for i := 0; i < len(fset.testData); i++ {
		val, err := st.Get(ctx, fset.testData[i][0])
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, val, fset.testData[i][1])
	}

	newRootCid, err := ns.WriteNode(ctx, root, nil)
	assert.NoError(t, err)
	assert.Equal(t, fset.rootCid, newRootCid)

	buf := new(bytes.Buffer)
	size, err := car2.TraverseV1(ctx, ns.lsys, newRootCid, selectorparse.CommonSelector_ExploreAllRecursively, buf)
	assert.NoError(t, err)
	assert.Equal(t, int(size), fset.carSize)
}
