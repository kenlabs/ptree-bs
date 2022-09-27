package tree

import (
	"bytes"
	"context"
	"ptree-bs/pkg/prolly/skip"
	"ptree-bs/pkg/prolly/tree/schema"
)

type MutationIter interface {
	NextMutation(ctx context.Context) (key, value []byte)
	Close() error
}

type OrderedListIter struct {
	Iter *skip.ListIter
}

func (it *OrderedListIter) NextMutation(context.Context) ([]byte, []byte) {
	k, v := it.Iter.Current()
	if k == nil {
		return nil, nil
	}
	it.Iter.Advance()
	return k, v
}

func (it *OrderedListIter) Close() error {
	return nil
}

func ApplyMutations(ctx context.Context, ns *NodeStore, root schema.ProllyNode, edits MutationIter, compare CompareFn) (schema.ProllyNode, error) {
	newKey, newValue := edits.NextMutation(ctx)
	if newKey == nil {
		// no update
		return root, nil
	}

	cur, err := NewCursorFromCompareFn(ctx, ns, root, newKey, compare)
	if err != nil {
		return schema.ProllyNode{}, err
	}

	ck, err := newChunker(ctx, cur.Clone(), 0, ns)
	if err != nil {
		return schema.ProllyNode{}, err
	}

	for newKey != nil {
		err = cur.seek(ctx, newKey, compare)
		if err != nil {
			return schema.ProllyNode{}, err
		}

		var oldValue []byte
		if cur.Valid() {
			if compare(newKey, cur.CurrentKey()) == 0 {
				oldValue = cur.CurrentValue()
			}
		}

		if equalValues(newValue, oldValue) {
			newKey, newValue = edits.NextMutation(ctx)
			continue
		}

		err = ck.AdvanceTo(ctx, cur)
		if err != nil {
			return schema.ProllyNode{}, err
		}

		if oldValue == nil {
			err = ck.AddPair(ctx, newKey, newValue)
		} else {
			if newValue != nil {
				err = ck.UpdatePair(ctx, newKey, newValue)
			} else {
				err = ck.DeletePair(ctx, newKey, newValue)
			}
		}
		if err != nil {
			return schema.ProllyNode{}, err
		}

		newKey, newValue = edits.NextMutation(ctx)
	}

	return ck.Done(ctx)
}

func equalValues(left, right []byte) bool {
	return bytes.Equal(left, right)
}
