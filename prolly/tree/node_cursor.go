package tree

import (
	"context"
	"github.com/ipfs/go-cid"
	"sort"
)

type ItemSearchFn func(item []byte, nd *Node) (idx int)

type CompareFn func(left, right []byte) int

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

func (cur *Cursor) CurrentSubtreeSize() uint64 {
	if cur.isLeaf() {
		return 1
	}
	if cur.subtrees == nil {
		cur.subtrees = cur.nd.getSubtreeCounts()
	}
	return cur.subtrees[cur.idx]
}

func (cur *Cursor) atNodeEnd() bool {
	lastKeyIdx := cur.nd.Count() - 1
	return cur.idx == lastKeyIdx
}

func (cur *Cursor) isLeaf() bool {
	return cur.level() == 0
}

func (cur *Cursor) level() uint64 {
	return uint64(cur.nd.Level)
}

func (cur *Cursor) seek(ctx context.Context, key []byte, cp CompareFn) error {
	inBounds := true
	if cur.parent != nil {
		inBounds = inBounds && cp(key, cur.firstKey()) >= 0
		inBounds = inBounds && cp(key, cur.lastKey()) <= 0
	}

	if !inBounds {
		err := cur.parent.seek(ctx, key, cp)
		if err != nil {
			return err
		}
		cur.parent.keepInBounds()

		cur.nd, err = cur.ns.Read(ctx, cur.parent.CurrentRef())
		if err != nil {
			return err
		}
	}

	cur.idx = cur.search(key, cp)

	return nil
}

func (cur *Cursor) search(item []byte, cp CompareFn) int {
	idx := sort.Search(cur.nd.Count(), func(i int) bool {
		return cp(item, cur.nd.GetKey(i)) <= 0
	})

	return idx
}

func (cur *Cursor) firstKey() []byte {
	return cur.nd.GetKey(0)
}

func (cur *Cursor) lastKey() []byte {
	lastKeyIdx := cur.nd.Count() - 1
	return cur.nd.GetKey(lastKeyIdx)
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

func (cur *Cursor) invalidate() {
	cur.idx = cur.nd.Count()
}

func (cur *Cursor) hasNext() bool {
	return cur.idx < cur.nd.Count()-1
}

func (cur *Cursor) OutOfBounds() bool {
	return cur.idx < 0 || cur.idx >= cur.nd.Count()
}

func (cur *Cursor) fetchNode(ctx context.Context) error {
	if cur.parent == nil {
		panic("invalid action")
	}
	var err error
	cur.nd, err = cur.ns.Read(ctx, cur.parent.CurrentRef())
	if err != nil {
		return err
	}
	cur.idx = -1 // wait to set
	return nil
}

func (cur *Cursor) Advance(ctx context.Context) error {
	if cur.hasNext() {
		cur.idx++
		return nil
	}

	if cur.parent == nil {
		cur.invalidate()
		return nil
	}

	err := cur.parent.Advance(ctx)
	if err != nil {
		return err
	}

	if cur.parent.OutOfBounds() {
		cur.invalidate()
		return nil
	}

	err = cur.fetchNode(ctx)
	if err != nil {
		return err
	}

	cur.skipToNodeStart()
	cur.subtrees = nil // lazy load

	return nil
}

func compareCursors(left, right *Cursor) int {
	diff := 0
	for {
		d := left.idx - right.idx
		if d != 0 {
			diff = d
		}

		if left.parent == nil || right.parent == nil {
			break
		}
		left, right = left.parent, right.parent
	}
	return diff
}

func (cur *Cursor) Compare(other *Cursor) int {
	return compareCursors(cur, other)
}

func (cur *Cursor) Clone() *Cursor {
	_cur := &Cursor{
		nd:  cur.nd,
		idx: cur.idx,
		ns:  cur.ns,
	}

	if cur.parent != nil {
		_cur.parent = cur.parent.Clone()
	}

	return _cur
}

func (cur *Cursor) copy(other *Cursor) {
	cur.nd = other.nd
	cur.idx = other.idx
	cur.ns = other.ns

	if cur.parent != nil {
		if other.parent == nil {
			panic("invalid")
		}
		cur.parent.copy(other.parent)
	} else {
		if other.parent != nil {
			panic("invalid")
		}
	}
}

func NewCursorFromCompareFn(ctx context.Context, ns *NodeStore, n *Node, item []byte, compare CompareFn) (*Cursor, error) {
	return NewCursorAtItem(ctx, ns, n, item, func(item []byte, nd *Node) (idx int) {
		return sort.Search(nd.Count(), func(i int) bool {
			return compare(item, nd.GetKey(i)) <= 0
		})
	})
}

func NewCursorAtItem(ctx context.Context, ns *NodeStore, nd *Node, item []byte, search ItemSearchFn) (*Cursor, error) {
	cur := &Cursor{nd: nd, ns: ns}

	cur.idx = search(item, cur.nd)
	for !cur.isLeaf() {

		// stay in bounds for internal nodes
		cur.keepInBounds()

		nd, err := ns.Read(ctx, cur.CurrentRef())
		if err != nil {
			return cur, err
		}

		parent := cur
		cur = &Cursor{nd: nd, parent: parent, ns: ns}

		cur.idx = search(item, cur.nd)
	}

	return cur, nil
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
