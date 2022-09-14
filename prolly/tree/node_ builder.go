package tree

import (
	"context"
	"github.com/ipfs/go-cid"
	"sync"
)

type novelNode struct {
	node      *Node
	addr      cid.Cid
	lastKey   []byte
	treeCount uint64
}

type nodeBuilder struct {
	keys, values [][]byte
	size, level  int
	subtrees     []uint64
}

func newNodeBuilder(level int) *nodeBuilder {
	nb := &nodeBuilder{
		level: level,
	}
	return nb
}

func (nb *nodeBuilder) hasCapacity(key, value []byte) bool {
	sum := nb.size + len(key) + len(value)
	return sum <= int(MaxNodeSize)
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

func (nb *nodeBuilder) build() (node *Node) {
	n := &Node{
		Keys:     nb.keys,
		Values:   nb.values,
		Size:     nb.size,
		Level:    nb.level,
		Subtrees: nb.subtrees,
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
	if node.Count() > 0 {
		k := node.GetKey(node.Count() - 1)
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
