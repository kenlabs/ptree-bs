package prolly

import (
	"context"
	"github.com/ipfs/go-log/v2"
	"ptree-bs/prolly/skip"
	"ptree-bs/prolly/tree"
)

var mplog = log.Logger("mutableTree")

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

func (mp *MutableProllyTree) Map(ctx context.Context) (*StaticTree, error) {
	if err := mp.ApplyPending(ctx); err != nil {
		return nil, err
	}
	tr := mp.tree

	root, err := tree.ApplyMutations(ctx, tr.ns, tr.root, mp.mutations(), DefaultBytesCompare)
	if err != nil {
		return nil, err
	}

	return &StaticTree{
		root: root,
		ns:   tr.ns,
	}, nil

}

func (mp *MutableProllyTree) Put(_ context.Context, key, value []byte) error {
	mp.edits.Put(key, value)
	return nil
}

func (mp *MutableProllyTree) Delete(_ context.Context, key []byte) error {
	mp.edits.Put(key, nil)
	return nil
}

func (mp *MutableProllyTree) Get(ctx context.Context, key []byte) ([]byte, error) {
	value, ok := mp.edits.Get(key)
	if ok {
		if value == nil {
			mplog.Infof("key %v has been delete in pending", key)
		}
		return value, nil
	}

	return mp.tree.Get(ctx, key)
}

func (mp *MutableProllyTree) Has(ctx context.Context, key []byte) (bool, error) {
	value, ok := mp.edits.Get(key)
	if ok {
		return value != nil, nil
	}
	return mp.tree.Has(ctx, key)
}

func (mp *MutableProllyTree) ApplyPending(ctx context.Context) error {
	mp.edits.Checkpoint()
	return nil
}

func (mp *MutableProllyTree) mutations() *tree.MutationIter {
	return &tree.MutationIter{Iter: mp.edits.IterAtStart()}
}
