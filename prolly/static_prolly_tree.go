package prolly

import (
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"ptree/prolly/tree"
)

type StaticTree struct {
	root *tree.Node
	bs   blockstore.Blockstore
}

func NewStaticProllyTree(node *tree.Node, bs blockstore.Blockstore) *StaticTree {
	return &StaticTree{
		root: node,
		bs:   bs,
	}
}
