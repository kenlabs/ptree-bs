package tree

import (
	"bytes"
	"context"
	"io"
)

type DiffType byte

const (
	AddedDiff    DiffType = 0
	ModifiedDiff DiffType = 1
	// todo not use now
	//RemovedDiff  DiffType = 2
)

type Diff struct {
	Key      []byte
	From, To []byte
	Type     DiffType
}

type Differ struct {
	base, new         *Cursor
	baseStop, newStop *Cursor
	order             CompareFn
}

func (df *Differ) Next(ctx context.Context) (Diff, error) {
	var err error
	for df.base.Valid() && df.base.Compare(df.baseStop) < 0 && df.new.Valid() && df.new.Compare(df.newStop) < 0 {
		bk := df.base.CurrentKey()
		nk := df.new.CurrentKey()
		cmp := df.order(bk, nk)

		switch {
		case cmp < 0:
			// todo: we only add or update the prolly tree now
			if err = df.base.Advance(ctx); err != nil {
				return Diff{}, err
			}
		case cmp > 0:
			return sendAdded(ctx, df.new)
		case cmp == 0:
			if !equalCursorValues(df.base, df.new) {
				return sendModified(ctx, df.base, df.new)
			}

			if err = skipCommon(ctx, df.base, df.new); err != nil {
				return Diff{}, err
			}
		}
	}

	if df.new.Valid() && df.new.Compare(df.newStop) < 0 {
		return sendAdded(ctx, df.new)
	}

	return Diff{}, io.EOF

}

func sendAdded(ctx context.Context, add *Cursor) (diff Diff, err error) {
	diff = Diff{
		Type: AddedDiff,
		Key:  add.CurrentKey(),
		To:   add.CurrentValue(),
	}

	if err = add.Advance(ctx); err != nil {
		return Diff{}, err
	}
	return
}

func sendModified(ctx context.Context, base, new *Cursor) (diff Diff, err error) {
	diff = Diff{
		Type: ModifiedDiff,
		Key:  base.CurrentKey(),
		From: base.CurrentValue(),
		To:   new.CurrentValue(),
	}

	if err = base.Advance(ctx); err != nil {
		return Diff{}, err
	}
	if err = new.Advance(ctx); err != nil {
		return Diff{}, err
	}
	return
}

func equalCursorValues(left, right *Cursor) bool {
	return bytes.Equal(left.CurrentValue(), right.CurrentValue())
}

func equalItems(left, right *Cursor) bool {
	return bytes.Equal(left.CurrentKey(), right.CurrentKey()) &&
		bytes.Equal(left.CurrentValue(), right.CurrentValue())
}

func skipCommon(ctx context.Context, from, to *Cursor) (err error) {
	// track when |from.parent| and |to.parent| change
	// to avoid unnecessary comparisons.
	parentsAreNew := true

	for from.Valid() && to.Valid() {
		if !equalItems(from, to) {
			// found the next difference
			return nil
		}

		if parentsAreNew {
			if equalParents(from, to) {
				// if our parents are equal, we can search for differences
				// faster at the next highest tree Level.
				if err = skipCommonParents(ctx, from, to); err != nil {
					return err
				}
				continue
			}
			parentsAreNew = false
		}

		// if one of the cursors is at the end of its node, it will
		// need to Advance its parent and fetch a new node. In this
		// case we need to Compare parents again.
		parentsAreNew = from.atNodeEnd() || to.atNodeEnd()

		if err = from.Advance(ctx); err != nil {
			return err
		}
		if err = to.Advance(ctx); err != nil {
			return err
		}
	}

	return err
}

func skipCommonParents(ctx context.Context, from, to *Cursor) (err error) {
	err = skipCommon(ctx, from.parent, to.parent)
	if err != nil {
		return err
	}

	if from.parent.Valid() {
		if err = from.fetchNode(ctx); err != nil {
			return err
		}
		from.skipToNodeStart()
	} else {
		from.invalidateAtEnd()
	}

	if to.parent.Valid() {
		if err = to.fetchNode(ctx); err != nil {
			return err
		}
		to.skipToNodeStart()
	} else {
		to.invalidateAtEnd()
	}

	return
}

func equalParents(left, right *Cursor) (eq bool) {
	if left.parent != nil && right.parent != nil {
		eq = equalItems(left.parent, right.parent)
	}
	return
}
