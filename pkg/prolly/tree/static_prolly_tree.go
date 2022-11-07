package tree

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	"ptree-bs/pkg/prolly/tree/schema"
)

var (
	KeyNotFound = fmt.Errorf("KeyNotFound")
)

type StaticTree struct {
	Root     schema.ProllyNode
	Ns       *NodeStore
	ChunkCfg *schema.ChunkConfig
}

func DefaultBytesCompare(left, right []byte) int {
	return bytes.Compare(left, right)
}

// searchNode returns the smallest index where nd[i] >= query
// Adapted from search.Sort to inline comparison.
func searchNode(query []byte, nd schema.ProllyNode) int {
	n := nd.ItemCount()
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

func LoadProllyTreeFromRootNode(node schema.ProllyNode, ns *NodeStore) (*StaticTree, error) {
	// load chunk config from the ProllyNode's ChunkConfig cid
	cfg, err := ns.ReadChunkCfg(context.Background(), node.ChunkConfig)
	if err != nil {
		return nil, err
	}

	return &StaticTree{
		Root:     node,
		Ns:       ns,
		ChunkCfg: &cfg,
	}, nil
}

func LoadProllyTreeFromRootCid(rootCid cid.Cid, ns *NodeStore) (*StaticTree, error) {
	ctx := context.Background()
	rootNode, err := ns.ReadNode(ctx, rootCid)
	if err != nil {
		return nil, err
	}
	return LoadProllyTreeFromRootNode(rootNode, ns)
}

func (st *StaticTree) Mutate() *MutableTree {
	return NewMutableProllyTree(st)
}

func (st *StaticTree) Get(ctx context.Context, key []byte) ([]byte, error) {
	// create cursor and try to find the key(maybe not exist)
	cur, err := NewLeafCursorAtItem(ctx, st.Ns, st.Root, key, searchNode)
	if err != nil {
		return nil, err
	}

	if cur.Valid() {
		keyFound := cur.CurrentKey()
		// if not exist, the key is the closest key bigger than it or invalid
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
	cur, err := NewLeafCursorAtItem(ctx, st.Ns, st.Root, key, searchNode)
	if err != nil {
		return false, err
	}

	if cur.Valid() {
		ok := DefaultBytesCompare(key, cur.CurrentKey()) == 0
		return ok, nil
	}

	return false, fmt.Errorf("invalid cursor")
}
