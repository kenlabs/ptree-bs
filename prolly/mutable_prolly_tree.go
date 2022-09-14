package prolly

import "ptree-bs/prolly/skip"

type MutableProllyTree struct {
	edits *skip.List
	tree  *StaticTree
}

func NewMutableProllyTree(st *StaticTree) *MutableProllyTree {
	return &MutableProllyTree{
		edits: skip.NewSkipList(func(left, right []byte) int {
			return DefaultBytesCompare(left, right)
		}),
		tree: st,
	}
}
