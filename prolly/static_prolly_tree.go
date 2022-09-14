package prolly

import (
	"bytes"
	"context"
	"fmt"
	"ptree-bs/prolly/tree"
)

var (
	KeyNotFound = fmt.Errorf("KeyNotFound")
)

type StaticTree struct {
	root *tree.Node
	ns   *tree.NodeStore
}

func DefaultBytesCompare(left, right []byte) int {
	return bytes.Compare(left, right)
}

// searchNode returns the smallest index where nd[i] >= query
// Adapted from search.Sort to inline comparison.
func searchNode(query []byte, nd *tree.Node) int {
	n := int(nd.Count())
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(i-1) == false, f(j) == true.
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		less := DefaultBytesCompare(query, nd.Keys[h]) <= 0
		// i ≤ h < j
		if !less {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	// i == j, f(i-1) == false, and
	// f(j) (= f(i)) == true  =>  answer is i.
	return i
}

func NewStaticProllyTree(node *tree.Node, ns *tree.NodeStore) *StaticTree {
	return &StaticTree{
		root: node,
		ns:   ns,
	}
}

func (st *StaticTree) Mutate() *MutableProllyTree {
	return NewMutableProllyTree(st)
}

func (st *StaticTree) Get(ctx context.Context, key []byte) ([]byte, error) {
	cur, err := tree.NewLeafCursorAtItem(ctx, st.ns, st.root, key, searchNode)
	if err != nil {
		return nil, err
	}

	//var key []byte
	//var value []byte
	if cur.Valid() {
		keyFound := cur.CurrentKey()
		if DefaultBytesCompare(key, keyFound) == 0 {
			value := cur.CurrentValue()
			return value, nil
		} else {
			return nil, KeyNotFound
		}
	} else {
		return nil, fmt.Errorf("invalid cursor")
	}
}

func (st *StaticTree) Has(ctx context.Context, key []byte) (bool, error) {
	cur, err := tree.NewLeafCursorAtItem(ctx, st.ns, st.root, key, searchNode)
	if err != nil {
		return false, err
	}

	if cur.Valid() {
		ok := DefaultBytesCompare(key, cur.CurrentKey()) == 0
		return ok, nil
	}

	return false, fmt.Errorf("invalid cursor")
}
