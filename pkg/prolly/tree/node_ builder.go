package tree

import (
	"context"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"ptree-bs/pkg/prolly/tree/schema"
	"sync"
)

type novelNode struct {
	node      schema.ProllyNode
	addr      cid.Cid
	lastKey   []byte
	treeCount uint64
}

type nodeBuilder struct {
	keys, values [][]byte
	size, level  int
	subtrees     []uint64
	cfg          cid.Cid
}

func newNodeBuilder(level int, cfgCid cid.Cid) *nodeBuilder {
	nb := &nodeBuilder{
		level: level,
		cfg:   cfgCid,
	}
	return nb
}

func (nb *nodeBuilder) hasCapacity(key, value []byte) bool {
	sum := nb.size + len(key) + len(value)
	return sum <= int(schema.MaxNodeSize)
}

func (nb *nodeBuilder) addItems(key, value []byte, subtree uint64) {
	if nb.keys == nil {
		nb.keys = getItemSlices()
		nb.values = getItemSlices()
		nb.subtrees = getSubtreeSlice()
	}
	nb.keys = append(nb.keys, key)
	nb.values = append(nb.values, value)
	nb.size += len(key) + len(value)
	nb.subtrees = append(nb.subtrees, subtree)
}

func (nb *nodeBuilder) count() int {
	return len(nb.keys)
}

func (nb *nodeBuilder) build() (node schema.ProllyNode) {
	_keys := make([][]byte, len(nb.keys))
	_subtrees := make([]uint64, len(nb.subtrees))
	copy(_keys, nb.keys)
	copy(_subtrees, nb.subtrees)
	n := schema.ProllyNode{
		Keys:       _keys,
		Values:     nil,
		Links:      nil,
		Level:      nb.level,
		Count:      uint16(len(nb.keys)),
		Subtrees:   _subtrees,
		Totalcount: schema.SumSubtrees(_subtrees),
		Cfg:        nb.cfg,
	}
	if nb.level == 0 {
		_vals := make([][]byte, len(nb.values))
		copy(_vals, nb.values)
		n.Values = _vals
	} else {
		lnks := make([]*ipld.Link, len(nb.values))
		for i, cidBytes := range nb.values {
			n, c, err := cid.CidFromBytes(cidBytes)
			if err != nil {
				panic(err.Error())
			}
			if n != CidBytesLen {
				panic("wrong cid bytes length")
			}
			var lnk ipld.Link = cidlink.Link{Cid: c}
			lnks[i] = &lnk
		}
		n.Links = lnks
	}

	nb.recycleBuffers()
	nb.size = 0
	return n
}

func (nb *nodeBuilder) recycleBuffers() {
	putItemSlices(nb.keys[:0])
	putItemSlices(nb.values[:0])
	putSubtreeSlice(nb.subtrees[:0])
	nb.keys = nil
	nb.values = nil
	nb.subtrees = nil
}

func writeNewNode(ctx context.Context, ns *NodeStore, nb *nodeBuilder) (*novelNode, error) {
	node := nb.build()

	addr, err := ns.Write(ctx, node)
	if err != nil {
		return nil, err
	}

	var lastKey []byte
	if node.ItemCount() > 0 {
		k := node.GetKey(node.ItemCount() - 1)
		lastKey = make([]byte, len(k))
		copy(lastKey, k)
	}

	treeCount := uint64(node.TreeCount())

	return &novelNode{
		node:      node,
		addr:      addr,
		lastKey:   lastKey,
		treeCount: treeCount,
	}, nil
}

const nodeBuilderListSize = 256

var itemsPool = sync.Pool{
	New: func() any {
		return make([][]byte, 0, nodeBuilderListSize)
	},
}

func getItemSlices() [][]byte {
	sl := itemsPool.Get().([][]byte)
	return sl[:0]
}

func putItemSlices(sl [][]byte) {
	itemsPool.Put(sl[:0])
}

var subtreePool = sync.Pool{
	New: func() any {
		return make([]uint64, 0, nodeBuilderListSize)
	},
}

func getSubtreeSlice() []uint64 {
	sl := subtreePool.Get().([]uint64)
	return sl[:0]
}

func putSubtreeSlice(sl []uint64) {
	subtreePool.Put(sl[:0])
}
