package tree

import (
	"context"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"ptree-bs/pkg/prolly/tree/linksystem"
	"ptree-bs/pkg/prolly/tree/schema"
)

const CidBytesLen = 20

type storeConfig struct {
	cacheSize int
}

type NodeStore struct {
	bs    blockstore.Blockstore
	lsys  *ipld.LinkSystem
	cache *lru.Cache
}

func NewNodeStore(bs blockstore.Blockstore, cfg *storeConfig) (*NodeStore, error) {
	lsys := linksystem.MkLinkSystem(bs)
	ns := &NodeStore{
		bs:   bs,
		lsys: &lsys,
	}
	if cfg == nil {
		return ns, nil
	}
	if cfg.cacheSize != 0 {
		var err error
		ns.cache, err = lru.New(cfg.cacheSize)
		if err != nil {
			return nil, err
		}
	}
	return ns, nil
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
	c := lnk.(cidlink.Link).Cid

	go func() {
		if ns.cache != nil {
			ns.cache.Add(c, nd)
		}
	}()

	return c, nil
}

func (ns *NodeStore) Read(ctx context.Context, c cid.Cid) (schema.ProllyNode, error) {
	var inCache bool
	if ns.cache != nil {
		var res interface{}
		res, inCache = ns.cache.Get(c)
		if inCache {
			return res.(schema.ProllyNode), nil
		}
	}
	nd, err := ns.lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c}, schema.ProllyNodePrototype.Representation())
	if err != nil {
		return schema.ProllyNode{}, err
	}

	inode, err := schema.UnwrapProllyNode(nd)
	if err != nil {
		return schema.ProllyNode{}, err
	}

	return *inode, nil
}

func (ns *NodeStore) Close() {
}
