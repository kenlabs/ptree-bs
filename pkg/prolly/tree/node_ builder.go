package tree

import (
	"context"
	"github.com/ipfs/go-cid"
	"ptree-bs/pkg/prolly/tree/schema"
	"sync"
)

// temporal node struct in tree building procedure, it describes a ProllyNode and contains the information we need
type novelNode struct {
	node    schema.ProllyNode
	addr    cid.Cid
	lastKey []byte
}

type nodeBuilder struct {
	keys, values [][]byte
	size, level  int
	chunkCfg     cid.Cid
}

func newNodeBuilder(level int, cfgCid cid.Cid) *nodeBuilder {
	nb := &nodeBuilder{
		level:    level,
		chunkCfg: cfgCid,
	}
	return nb
}

func (nb *nodeBuilder) hasCapacity(key, value []byte) bool {
	sum := nb.size + len(key) + len(value)
	return sum <= int(schema.MaxNodeSize)
}

func (nb *nodeBuilder) addItems(key, value []byte) {
	if nb.keys == nil {
		nb.keys = getItemSlices()
		nb.values = getItemSlices()
	}
	nb.keys = append(nb.keys, key)
	nb.values = append(nb.values, value)
	nb.size += len(key) + len(value)
}

func (nb *nodeBuilder) count() int {
	return len(nb.keys)
}

func (nb *nodeBuilder) build() (node schema.ProllyNode) {
	// prevent from pointer pollution
	_keys := make([][]byte, len(nb.keys))
	copy(_keys, nb.keys)
	n := schema.ProllyNode{
		Keys:        _keys,
		Values:      nil,
		Links:       nil,
		Level:       nb.level,
		ChunkConfig: nb.chunkCfg,
	}
	if nb.level == 0 {
		// prevent from pointer pollution
		_vals := make([][]byte, len(nb.values))
		copy(_vals, nb.values)
		n.Values = _vals
	} else {
		cids := make([]cid.Cid, len(nb.values))
		for i, cidBytes := range nb.values {
			n, c, err := cid.CidFromBytes(cidBytes)
			if err != nil {
				panic(err.Error())
			}
			// todo: if linkProto can be defined by user, the condition may be removed
			if n != schema.CidBytesLen {
				panic("wrong cid bytes length")
			}
			cids[i] = c
		}
		n.Links = cids
	}

	nb.recycleBuffers()
	nb.size = 0
	return n
}

func (nb *nodeBuilder) recycleBuffers() {
	putItemSlices(nb.keys[:0])
	putItemSlices(nb.values[:0])
	nb.keys = nil
	nb.values = nil
}

func writeNewNode(ctx context.Context, ns *NodeStore, nb *nodeBuilder) (*novelNode, error) {
	node := nb.build()

	// write ProllyNode to block store(by link system)
	addr, err := ns.WriteNode(ctx, node, nil)
	if err != nil {
		return nil, err
	}

	var lastKey []byte
	if node.ItemCount() > 0 {
		k := node.GetKey(node.ItemCount() - 1)
		lastKey = make([]byte, len(k))
		// pointer pollution
		copy(lastKey, k)
	}

	return &novelNode{
		node:    node,
		addr:    addr,
		lastKey: lastKey,
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
