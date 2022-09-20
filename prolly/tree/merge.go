package tree

import (
	"context"
	"ptree-bs/prolly"
)

func MergeStaticTrees(ctx context.Context, base *prolly.StaticTree, new *prolly.StaticTree) (prolly.StaticTree, error) {
	root, err := Merge(ctx, base.ns, base.root, new.root)
	if err != nil {
		return prolly.StaticTree{}, err
	}

	return prolly.StaticTree{
		root: root,
		ns:   base.ns,
	}, nil
}

func Merge(ctx context.Context, ns *NodeStore, base Node, new Node, order CompareFn) (Node, error) {
	df, err := DifferFromRoots(ctx, ns, base, new, order)
	if err != nil {
		return Node{}, err
	}

}

func DifferFromRoots(ctx context.Context, ns *NodeStore, base, new Node, order CompareFn) (Differ, error) {
	bc, err := NewCursorAtStart(ctx, ns, base)
	if err != nil {
		return Differ{}, err
	}

	nc, err := NewCursorAtStart(ctx, ns, new)
	if err != nil {
		return Differ{}, err
	}

	bs, err := NewCursorPastEnd(ctx, ns, base)
	if err != nil {
		return Differ{}, err
	}

	newStop, err := NewCursorPastEnd(ctx, ns, base)
	if err != nil {
		return Differ{}, err
	}

	return Differ{
		base:     bc,
		new:      nc,
		baseStop: bs,
		newStop:  newStop,
		order:    order,
	}, nil

}
