package prolly

import (
	"context"
	"github.com/ipfs/go-log/v2"
	"ptree-bs/prolly/skip"
	"ptree-bs/prolly/tree"
)

var mplog = log.Logger("mutableTree")

type MutableTree struct {
	edits *skip.List
	tree  StaticTree
}

func NewMutableProllyTree(st *StaticTree) *MutableTree {
	_root := st.root
	_ns := st.ns
	newSt := StaticTree{
		root: _root,
		ns:   _ns,
	}
	return &MutableTree{
		edits: skip.NewSkipList(func(left, right []byte) int {
			return DefaultBytesCompare(left, right)
		}),
		tree: newSt,
	}
}

func (mp *MutableTree) Tree(ctx context.Context) (StaticTree, error) {
	if err := mp.ApplyPending(ctx); err != nil {
		return StaticTree{}, err
	}
	tr := mp.tree.Copy()

	root, err := tree.ApplyMutations(ctx, tr.ns, tr.root, mp.mutations(), DefaultBytesCompare)
	if err != nil {
		return StaticTree{}, err
	}

	return StaticTree{
		root: root,
		ns:   tr.ns,
	}, nil

}

func (mp *MutableTree) Put(_ context.Context, key, value []byte) error {
	mp.edits.Put(key, value)
	return nil
}

func (mp *MutableTree) Delete(_ context.Context, key []byte) error {
	mp.edits.Put(key, nil)
	return nil
}

func (mp *MutableTree) Get(ctx context.Context, key []byte) ([]byte, error) {
	value, ok := mp.edits.Get(key)
	if ok {
		if value == nil {
			mplog.Infof("key %v has been delete in pending", key)
		}
		return value, nil
	}

	return mp.tree.Get(ctx, key)
}

func (mp *MutableTree) Has(ctx context.Context, key []byte) (bool, error) {
	value, ok := mp.edits.Get(key)
	if ok {
		return value != nil, nil
	}
	return mp.tree.Has(ctx, key)
}

func (mp *MutableTree) ApplyPending(ctx context.Context) error {
	mp.edits.Checkpoint()
	return nil
}

func (mp *MutableTree) mutations() *tree.MutationIter {
	return &tree.MutationIter{Iter: mp.edits.IterAtStart()}
}
