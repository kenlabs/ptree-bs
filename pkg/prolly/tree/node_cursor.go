package tree

import (
	"context"
	"github.com/ipfs/go-cid"
	"ptree-bs/pkg/prolly/tree/schema"
	"sort"
)

type ItemSearchFn func(item []byte, nd schema.ProllyNode) (idx int)

type CompareFn func(left, right []byte) int

type Cursor struct {
	nd     schema.ProllyNode
	idx    int
	parent *Cursor
	ns     *NodeStore
}

func (cur *Cursor) CurrentKey() []byte {
	return cur.nd.GetKey(cur.idx)
}

func (cur *Cursor) CurrentValue() []byte {
	return cur.nd.GetValue(cur.idx)
}

// CurrentRef returns cid for current idx
func (cur *Cursor) CurrentRef() cid.Cid {
	return cur.nd.GetAddress(cur.idx)
}

// move cursor to the end of current node
func (cur *Cursor) atNodeEnd() bool {
	lastKeyIdx := cur.nd.ItemCount() - 1
	return cur.idx == lastKeyIdx
}

func (cur *Cursor) isLeaf() bool {
	return cur.level() == 0
}

func (cur *Cursor) level() uint64 {
	return uint64(cur.nd.Level)
}

// Seek updates the cursor's node to one whose range spans the key's value, or the last
// node if the key is greater than all existing keys.
// If a node does not contain the key, we recurse upwards to the parent cursor. If the
// node contains a key, we recurse downwards into child nodes.
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

		cur.nd, err = cur.ns.ReadNode(ctx, cur.parent.CurrentRef())
		if err != nil {
			return err
		}
	}

	cur.idx = cur.search(key, cp)

	return nil
}

func (cur *Cursor) search(item []byte, cp CompareFn) int {
	idx := sort.Search(cur.nd.ItemCount(), func(i int) bool {
		return cp(item, cur.nd.GetKey(i)) <= 0
	})

	return idx
}

func (cur *Cursor) firstKey() []byte {
	return cur.nd.GetKey(0)
}

func (cur *Cursor) lastKey() []byte {
	lastKeyIdx := cur.nd.ItemCount() - 1
	return cur.nd.GetKey(lastKeyIdx)
}

func (cur *Cursor) skipToNodeStart() {
	cur.idx = 0
}

func (cur *Cursor) skipToNodeEnd() {
	lastKeyIdx := cur.nd.ItemCount() - 1
	cur.idx = lastKeyIdx
}

func (cur *Cursor) keepInBounds() {
	if cur.idx < 0 {
		cur.skipToNodeStart()
	}
	lastKeyIdx := cur.nd.ItemCount() - 1
	if cur.idx > lastKeyIdx {
		cur.skipToNodeEnd()
	}
}

func (cur *Cursor) Valid() bool {
	return cur.nd.ItemCount() != 0 &&
		cur.idx >= 0 &&
		cur.idx < cur.nd.ItemCount()
}

func (cur *Cursor) invalidateAtEnd() {
	cur.idx = cur.nd.ItemCount()
}

func (cur *Cursor) invalidateAtStart() {
	cur.idx = -1
}

func (cur *Cursor) hasNext() bool {
	return cur.idx < cur.nd.ItemCount()-1
}

func (cur *Cursor) OutOfBounds() bool {
	return cur.idx < 0 || cur.idx >= cur.nd.ItemCount()
}

func (cur *Cursor) fetchNode(ctx context.Context) error {
	if cur.parent == nil {
		panic("invalid action")
	}
	var err error
	cur.nd, err = cur.ns.ReadNode(ctx, cur.parent.CurrentRef())
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
		cur.invalidateAtEnd()
		return nil
	}

	err := cur.parent.Advance(ctx)
	if err != nil {
		return err
	}

	if cur.parent.OutOfBounds() {
		cur.invalidateAtEnd()
		return nil
	}

	err = cur.fetchNode(ctx)
	if err != nil {
		return err
	}

	cur.skipToNodeStart()

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

// Compare returns the highest relative index difference
// between two cursor trees. A parent has a higher precedence
// than its child.
//
// Ex:
//
// cur:   L3 -> 4, L2 -> 2, L1 -> 5, L0 -> 2
// other: L3 -> 4, L2 -> 2, L1 -> 5, L0 -> 4
//
//	res => -2 (from level 0)
//
// cur:   L3 -> 4, L2 -> 2, L1 -> 5, L0 -> 2
// other: L3 -> 4, L2 -> 3, L1 -> 5, L0 -> 4
//
//	res => -1 (from level 2)
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

func NewCursorFromCompareFn(ctx context.Context, ns *NodeStore, n schema.ProllyNode, item []byte, compare CompareFn) (*Cursor, error) {
	return NewCursorAtItem(ctx, ns, n, item, func(item []byte, nd schema.ProllyNode) (idx int) {
		return sort.Search(nd.ItemCount(), func(i int) bool {
			return compare(item, nd.GetKey(i)) <= 0
		})
	})
}

func NewCursorAtItem(ctx context.Context, ns *NodeStore, nd schema.ProllyNode, item []byte, search ItemSearchFn) (*Cursor, error) {
	cur := &Cursor{nd: nd, ns: ns}

	cur.idx = search(item, cur.nd)
	for !cur.isLeaf() {

		// stay in bounds for internal nodes
		cur.keepInBounds()

		nd, err := ns.ReadNode(ctx, cur.CurrentRef())
		if err != nil {
			return cur, err
		}

		parent := cur
		cur = &Cursor{nd: nd, parent: parent, ns: ns}

		cur.idx = search(item, cur.nd)
	}

	return cur, nil
}

func NewLeafCursorAtItem(ctx context.Context, ns *NodeStore, nd schema.ProllyNode, item []byte, search ItemSearchFn) (*Cursor, error) {
	cur := &Cursor{nd: nd, parent: nil, ns: ns}

	cur.idx = search(item, cur.nd)

	var err error
	for !cur.isLeaf() {
		cur.keepInBounds()

		cur.nd, err = ns.ReadNode(ctx, cur.CurrentRef())
		if err != nil {
			return cur, err
		}

		cur.idx = search(item, cur.nd)
	}

	return cur, nil
}

func NewCursorAtStart(ctx context.Context, ns *NodeStore, nd schema.ProllyNode) (*Cursor, error) {
	cur := &Cursor{nd: nd, ns: ns}
	var leaf bool
	var err error
	leaf = cur.isLeaf()
	if err != nil {
		return nil, err
	}
	for !leaf {
		nd, err = ns.ReadNode(ctx, cur.CurrentRef())
		if err != nil {
			return nil, err
		}

		parent := cur
		cur = &Cursor{nd: nd, parent: parent, ns: ns}
		leaf = cur.isLeaf()
		if err != nil {
			return nil, err
		}
	}
	return cur, nil
}

func NewCursorAtEnd(ctx context.Context, ns *NodeStore, nd schema.ProllyNode) (*Cursor, error) {
	cur := &Cursor{nd: nd, ns: ns}
	cur.skipToNodeEnd()

	var leaf bool
	var err error
	leaf = cur.isLeaf()
	if err != nil {
		return nil, err
	}
	for !leaf {
		nd, err = ns.ReadNode(ctx, cur.CurrentRef())
		if err != nil {
			return nil, err
		}

		parent := cur
		cur = &Cursor{nd: nd, parent: parent, ns: ns}
		cur.skipToNodeEnd()
		leaf = cur.isLeaf()
		if err != nil {
			return nil, err
		}
	}
	return cur, nil
}

// NewCursorPastEnd arrives the last index then advance() again
func NewCursorPastEnd(ctx context.Context, ns *NodeStore, nd schema.ProllyNode) (*Cursor, error) {
	cur, err := NewCursorAtEnd(ctx, ns, nd)
	if err != nil {
		return nil, err
	}

	err = cur.Advance(ctx)
	if err != nil {
		return nil, err
	}
	if cur.idx != cur.nd.ItemCount() {
		panic("invalid cursor index")
	}

	return cur, nil
}
