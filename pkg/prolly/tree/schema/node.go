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
	// For branch nodes, they are cids of children nodes. For leaf nodes, they are cids indexed(reference of real data)
	Values []cid.Cid
	// null for leaf nodes. For branch nodes, it's the cid of the child node. So (key, link) is the the last key and cid
	// about the child node. Key is used for searching and cid is used for loading the child node from local storage or
	// network
	// 0 for leaf nodes, and add 1 for parent level
	Level int
	// chunk strategy(ChunkConfig) about how the prolly tree is built. We should mutate the tree with the same strategy, or may lead to
	// the worst performance and even unknown error, it's the same with merge action
	ChunkConfig cid.Cid
}

// ItemCount returns the number of key/value pairs in the node
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
	return nd.Values[i].Bytes()
}

func (nd *ProllyNode) GetAddress(i int) cid.Cid {
	if nd.Level == 0 {
		panic("can not get address in leaf node")
	}
	c := nd.Values[i]
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
