// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
		Level:       nb.level,
		ChunkConfig: nb.chunkCfg,
	}
	cids := make([]cid.Cid, len(nb.values))

	for i, cidBytes := range nb.values {
		n, c, err := cid.CidFromBytes(cidBytes)
		if err != nil {
			panic(err.Error())
		}
		// todo: if linkProto can be defined by user, the condition may be removed
		// !=0 because the indexed CIDs are not decided by ProllyTree
		if n != schema.CidBytesLen && nb.level != 0 {
			panic("wrong cid bytes length")
		}
		cids[i] = c
	}
	n.Values = cids

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
