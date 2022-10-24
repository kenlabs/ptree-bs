package schema

import (
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"math"
)

const (
	MaxNodeSize = uint64(math.MaxUint16)
)

type ProllyNode struct {
	// raw keys(keys/values input from users) for leaf node. For branch nodes, the key is last key in the child node,
	// if data(k/v pairs) are sorted and increase, it's the biggest key in the child node
	Keys [][]byte
	// raw values for leaf nodes. For branch nodes, it's null.
	Values [][]byte
	// null for leaf nodes. For branch nodes, it's the cid of the child node. So (key, link) is the the last key and cid
	// about the child node. Key is used for searching and cid is used for loading the child node from local storage or
	// network
	Links []cid.Cid
	// 0 for leaf nodes, and add 1 for parent level
	Level int
	// chunk strategy(ChunkConfig) about how the prolly tree is built. We should mutate the tree with the same strategy, or may lead to
	// the worst performance and even unknown error, it's the same with merge action
	ChunkConfig cid.Cid
}

func (nd *ProllyNode) ItemCount() int {
	return len(nd.Keys)
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
		return nd.Links[i].Bytes()
	}
}

func (nd *ProllyNode) GetAddress(i int) cid.Cid {
	c := nd.Links[i]
	// todo: if linkProto can be defined by user, the condition may be removed
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
