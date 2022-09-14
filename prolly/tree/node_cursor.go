package tree

import (
	"context"
	"github.com/ipfs/go-cid"
)

type ItemSearchFn func(item []byte, nd *Node) (idx int)

type Cursor struct {
	nd       *Node
	idx      int
	parent   *Cursor
	subtrees []uint64
	ns       *NodeStore
}

func (cur *Cursor) CurrentKey() []byte {
	return cur.nd.GetKey(cur.idx)
}

func (cur *Cursor) CurrentValue() []byte {
	return cur.nd.getValue(cur.idx)
}

func (cur *Cursor) CurrentRef() cid.Cid {
	return cur.nd.getAddress(cur.idx)
}

func (cur *Cursor) isLeaf() bool {
	return cur.level() == 0
}

func (cur *Cursor) level() uint64 {
	return uint64(cur.nd.Level)
}

func (cur *Cursor) skipToNodeStart() {
	cur.idx = 0
}

func (cur *Cursor) skipToNodeEnd() {
	lastKeyIdx := cur.nd.Count() - 1
	cur.idx = lastKeyIdx
}

func (cur *Cursor) keepInBounds() {
	if cur.idx < 0 {
		cur.skipToNodeStart()
	}
	lastKeyIdx := cur.nd.Count() - 1
	if cur.idx > lastKeyIdx {
		cur.skipToNodeEnd()
	}
}

func (cur *Cursor) Valid() bool {
	return cur.nd.Count() != 0 &&
		cur.idx >= 0 &&
		cur.idx < cur.nd.Count()
}

func NewLeafCursorAtItem(ctx context.Context, ns *NodeStore, nd *Node, item []byte, search ItemSearchFn) (*Cursor, error) {
	cur := &Cursor{nd: nd, parent: nil, ns: ns}

	cur.idx = search(item, cur.nd)

	var err error
	for !cur.isLeaf() {
		cur.keepInBounds()

		cur.nd, err = ns.Read(ctx, cur.CurrentRef())
		if err != nil {
			return cur, err
		}

		cur.idx = search(item, cur.nd)
	}

	return cur, nil
}
