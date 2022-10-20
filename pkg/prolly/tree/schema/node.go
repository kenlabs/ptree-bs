package schema

import (
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"math"
)

const (
	MaxNodeSize = uint64(math.MaxUint16)
)

func SumSubtrees(subtrees []uint64) (sum uint64) {
	for i := range subtrees {
		sum += subtrees[i]
	}
	return
}

type ProllyNode struct {
	// raw keys for leaf nodes, cids for branch nodes
	Keys [][]byte
	// raw values for leaf nodes, nil for branch
	Values [][]byte
	// cid links for branch nodes, nil for leaf
	Links []*ipld.Link
	// 0 for leaf
	Level int
	// number of k/v in the node
	Count uint16
	Cfg   cid.Cid
}

func (nd *ProllyNode) ItemCount() int {
	return int(nd.Count)
}

func (nd *ProllyNode) IsLeaf() bool {
	return nd.Level == 0
}

func (nd *ProllyNode) GetKey(i int) []byte {
	return nd.Keys[i]
}

func (nd *ProllyNode) GetValue(i int) []byte {
	if nd.Level == 0 {
		return nd.Values[i]
	} else {
		return (*nd.Links[i]).(cidlink.Link).Cid.Bytes()
	}
}

func (nd *ProllyNode) GetAddress(i int) cid.Cid {
	c := (*nd.Links[i]).(cidlink.Link).Cid
	if c.ByteLen() != CidBytesLen {
		panic("invalid cid length")
	}
	return c
}

const CidBytesLen = 20

var LinkProto = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.DagCbor),
		MhType:   uint64(multicodec.Sha2_256),
		MhLength: 16,
	},
}
