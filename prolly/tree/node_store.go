package tree

import (
	"context"
	"encoding/json"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
)

var LinkProto = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.DagCbor),
		MhType:   uint64(multicodec.Sha2_256),
		MhLength: 16,
	},
}

const CidBytesLen = 20

type NodeStore struct {
	bs blockstore.Blockstore
}

func NewNodeStore(bs blockstore.Blockstore) *NodeStore {
	return &NodeStore{bs: bs}
}

func (ns *NodeStore) Write(ctx context.Context, nd *Node) (cid.Cid, error) {
	nodeBytes, err := json.Marshal(nd)
	if err != nil {
		return cid.Undef, err
	}

	c, err := LinkProto.Sum(nodeBytes)
	if err != nil {
		return cid.Undef, err
	}

	block, err := blocks.NewBlockWithCid(nodeBytes, c)
	if err != nil {
		return cid.Undef, err
	}

	err = ns.bs.Put(ctx, block)
	if err != nil {
		return cid.Undef, err
	}

	return c, nil
}

func (ns *NodeStore) Read(ctx context.Context, c cid.Cid) (*Node, error) {
	nodeBlock, err := ns.bs.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	nodeBytes := nodeBlock.RawData()
	n := &Node{}
	err = json.Unmarshal(nodeBytes, n)
	if err != nil {
		return nil, err
	}
	return n, nil
}
