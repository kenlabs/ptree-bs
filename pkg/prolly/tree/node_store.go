package tree

import (
	"context"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"ptree-bs/pkg/prolly/tree/linksystem"
	"ptree-bs/pkg/prolly/tree/schema"
)

type NodeStore struct {
	bs   blockstore.Blockstore
	lsys *ipld.LinkSystem
}

func NewNodeStore(bs blockstore.Blockstore) *NodeStore {
	lsys := linksystem.MkLinkSystem(bs)
	return &NodeStore{
		bs:   bs,
		lsys: &lsys,
	}
}

func (ns *NodeStore) Write(ctx context.Context, nd schema.ProllyNode) (cid.Cid, error) {
	ipldNode, err := nd.ToNode()
	if err != nil {
		return cid.Undef, err
	}
	lnk, err := ns.lsys.Store(ipld.LinkContext{Ctx: ctx}, schema.LinkProto, ipldNode)
	if err != nil {
		return cid.Undef, err
	}

	return lnk.(cidlink.Link).Cid, nil
}

func (ns *NodeStore) Read(ctx context.Context, c cid.Cid) (schema.ProllyNode, error) {
	nd, err := ns.lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c}, schema.ProllyNodePrototype)
	if err != nil {
		return schema.ProllyNode{}, err
	}
	inode, err := schema.UnwrapProllyNode(nd)
	if err != nil {
		return schema.ProllyNode{}, err
	}

	return *inode, nil
}
