package tree

import (
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"math"
)

const (
	MaxNodeSize = uint64(math.MaxUint16)
)

func sumSubtrees(subtrees []uint64) (sum uint64) {
	for i := range subtrees {
		sum += subtrees[i]
	}
	return
}

type ProllyNode struct {
	Keys       [][]byte
	Values     [][]byte
	Links      []*ipld.Link
	Size       int
	Level      int
	Count      uint16
	Subtrees   []uint64
	Totalcount uint64
}

func (nd *ProllyNode) ItemCount() int {
	return int(nd.Count)
}

func (nd *ProllyNode) TreeCount() int {
	return int(nd.Totalcount)
}

func (nd *ProllyNode) IsLeaf() bool {
	return nd.Level == 0
}

func (nd *ProllyNode) GetKey(i int) []byte {
	return nd.Keys[i]
}

func (nd *ProllyNode) getValue(i int) []byte {
	if nd.Level == 0 {
		return nd.Values[i]
	} else {
		return (*nd.Links[i]).(cidlink.Link).Cid.Bytes()
	}
}

func (nd *ProllyNode) getAddress(i int) cid.Cid {
	c := (*nd.Links[i]).(cidlink.Link).Cid
	if c.ByteLen() != CidBytesLen {
		panic("invalid cid length")
	}
	return c
}

func (nd *ProllyNode) getSubtreeCounts() []uint64 {
	return nd.Subtrees
}
