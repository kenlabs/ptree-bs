package tree

import (
	"fmt"
	"github.com/ipfs/go-cid"
	"math"
)

const (
	MaxNodeSize = uint64(math.MaxUint16)
)

type Node struct {
	Keys, Values [][]byte
	Size, Level  int
	Subtrees     []uint64
}

func (nd *Node) Count() int {
	return len(nd.Keys)
}

func sumSubtrees(subtrees []uint64) (sum uint64) {
	for i := range subtrees {
		sum += subtrees[i]
	}
	return
}

// todo overflow?
func (nd *Node) TreeCount() int {
	return int(sumSubtrees(nd.Subtrees))
}

func (nd *Node) IsLeaf() bool {
	return nd.Level == 0
}

func (nd *Node) getValue(i int) []byte {
	return nd.Values[i]
}

func (nd *Node) GetKey(i int) []byte {
	return nd.Keys[i]
}

func (nd *Node) getAddress(i int) cid.Cid {
	l, c, err := cid.CidFromBytes(nd.getValue(i))
	if err != nil {
		panic(err)
	}
	if l != CidBytesLen {
		panic(fmt.Errorf("invalid cid length: %d, expected: %d", l, CidBytesLen))
	}
	return c
}
