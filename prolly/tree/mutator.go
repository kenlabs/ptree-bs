package tree

import (
	"context"
	"ptree-bs/prolly/skip"
)

type MutationIter struct {
	Iter *skip.ListIter
}

func (it *MutationIter) NextMutation(context.Context) ([]byte, []byte) {
	k, v := it.Iter.Current()
	if k == nil {
		return nil, nil
	}
	it.Iter.Advance()
	return k, v
}

func (it *MutationIter) Close() error {
	return nil
}

func ApplyMutations(ctx context.Context, ns *NodeStore, root *Node, edits *MutationIter, compare CompareFn) (*Node, error) {
	newKey, newValue := edits.NextMutation(ctx)
	if newKey == nil {
		// no update
		return root, nil
	}

	cur, err := NewCursorFromCompareFn(ctx, ns, root, newKey, compare)
	if err != nil {
		return nil, err
	}

	ck, err := newChunker()
}
