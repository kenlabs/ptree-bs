package tree

import (
	"context"
	"golang.org/x/sync/errgroup"
	"io"
	//"ptree-bs/prolly"
)

type patchBuffer struct {
	buf chan patch
}

type patch [2][]byte

var _ MutationIter = patchBuffer{}

func newPatchBuffer(sz int) patchBuffer {
	return patchBuffer{buf: make(chan patch, sz)}
}

func (pb patchBuffer) sendPatch(ctx context.Context, diff Diff) error {
	p := patch{diff.Key, diff.To}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case pb.buf <- p:
		return nil
	}
}

// NextMutation implements MutationIter.
func (pb patchBuffer) NextMutation(ctx context.Context) ([]byte, []byte) {
	var p patch
	select {
	case p = <-pb.buf:
		return p[0], p[1]
	case <-ctx.Done():
		return nil, nil
	}
}

func (pb patchBuffer) Close() error {
	close(pb.buf)
	return nil
}

func sendPatches(ctx context.Context, differ Differ, buf patchBuffer) error {
	var end bool
	var err error
	patch, err := differ.Next(ctx)
	if err == io.EOF {
		err, end = nil, true
	}
	if err != nil {
		return err
	}

	for !end {
		err = buf.sendPatch(ctx, patch)
		if err != nil {
			return err
		}

		patch, err = differ.Next(ctx)
		if err == io.EOF {
			err, end = nil, true
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func MergeStaticTrees(ctx context.Context, base *StaticTree, new *StaticTree) (StaticTree, error) {
	root, err := Merge(ctx, base.Ns, base.Root, new.Root, DefaultBytesCompare)
	if err != nil {
		return StaticTree{}, err
	}

	return StaticTree{
		Root: root,
		Ns:   base.Ns,
	}, nil
}

func Merge(ctx context.Context, ns *NodeStore, base ProllyNode, new ProllyNode, order CompareFn) (ProllyNode, error) {
	var result ProllyNode

	df, err := DifferFromRoots(ctx, ns, base, new, order)
	if err != nil {
		return ProllyNode{}, err
	}

	eg, ctx := errgroup.WithContext(ctx)
	patches := newPatchBuffer(1024)

	eg.Go(func() (err error) {
		defer func() {
			if cerr := patches.Close(); err == nil {
				err = cerr
			}
		}()
		err = sendPatches(ctx, df, patches)
		return
	})

	eg.Go(func() error {
		result, err = ApplyMutations(ctx, ns, base, patches, DefaultBytesCompare)
		return err
	})

	if err = eg.Wait(); err != nil {
		return ProllyNode{}, err
	}

	return result, nil
}

func DifferFromRoots(ctx context.Context, ns *NodeStore, base, new ProllyNode, order CompareFn) (Differ, error) {
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
