package tree

import (
	"context"
	"fmt"
)

type Chunker struct {
	cur    *Cursor
	parent *Chunker
	level  int
	done   bool

	splitter nodeSplitter
	builder  *nodeBuilder

	ns *NodeStore
}

func NewEmptyChunker(ctx context.Context, ns *NodeStore) (*Chunker, error) {
	return newChunker(ctx, 0, ns)
}

func newChunker(ctx context.Context, cur *Cursor, level int, ns *NodeStore) (*Chunker, error) {
	splitter := defaultSplitterFactory(uint8(level % 256))
	builider := newNodeBuilder(level)

	c := &Chunker{
		cur:      cur,
		parent:   nil,
		level:    level,
		splitter: splitter,
		builder:  builider,
		ns:       ns,
	}

	if cur != nil {
		if err := c.processPrefix(ctx); err != nil {
			return nil, err
		}
	}

	return c, nil
}

func (c *Chunker) processPrefix(ctx context.Context) error {
	if c.cur.parent != nil && c.parent == nil {
		if err := c.createParentChunker(ctx); err != nil {
			return err
		}
	}

	idx := c.cur.idx
	c.cur.skipToNodeStart()

	for c.cur.idx < idx {
		_, err := c.append(ctx,
			c.cur.CurrentKey(),
			c.cur.CurrentValue(),
			c.cur.CurrentSubtreeSize())
		if err != nil {
			return err
		}

		err = c.cur.Advance(ctx)
	}

}

func (c *Chunker) AddPair(ctx context.Context, key, value []byte) error {
	_, err := c.append(ctx, key, value, 1)
	return err
}

func (c *Chunker) append(ctx context.Context, key, value []byte, subtree uint64) (bool, error) {
	// When adding new key-value pairs to an in-progress chunk, we must enforce 3 invariants
	// (1) Key-value pairs are stored in the same Node.
	// (2) The total Size of a Node's data cannot exceed |MaxVectorOffset|.
	// (3) Internal Nodes (Level > 0) must contain at least 2 key-value pairs (4 node []bytes).
	//     Infinite recursion can occur if internal nodes contain a single novelNode with a key
	//     large enough to trigger a chunk boundary. Forming a chunk boundary after a single
	//     key will lead to an identical novelNode in the nextMutation Level in the tree, triggering
	//     the same state infinitely. This problem can only occur at levels 2 and above,
	//     but we enforce this constraint for all internal nodes of the tree.

	// (3)
	degenerate := !c.isLeaf() && c.builder.count() == 1

	// (2)
	overSize := !c.builder.hasCapacity(key, value)

	if degenerate && overSize {
		panic("invalid node")
	}

	if overSize {
		err := c.handleBoundary(ctx)
		if err != nil {
			return false, err
		}
	}

	c.builder.addItems(key, value, subtree)

	err := c.splitter.Append(key, value)
	if err != nil {
		return false, err
	}

	degenerate = !c.isLeaf() && c.builder.count() == 1

	// if gen boundary but degenerate, ignore it
	if c.splitter.CrossedBoundary() && !degenerate {
		err = c.handleBoundary(ctx)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

func (c *Chunker) appendToParent(ctx context.Context, novel *novelNode) (bool, error) {
	if c.parent == nil {
		if err := c.createParentChunker(ctx); err != nil {
			return false, err
		}
	}

	return c.parent.append(ctx, novel.lastKey, novel.addr.Bytes(), novel.treeCount)
}

func (c *Chunker) handleBoundary(ctx context.Context) error {
	if !(c.builder.count() > 0) {
		panic("invalid items count")
	}
	novel, err := writeNewNode(ctx, c.ns, c.builder)
	if err != nil {
		return err
	}

	if _, err = c.appendToParent(ctx, novel); err != nil {
		return err
	}

	c.splitter.Reset()

	return nil
}

func (c *Chunker) createParentChunker(ctx context.Context) error {
	if c.parent != nil {
		panic("impossible action")
	}

	var err error
	var parent *Cursor
	if c.cur != nil && c.cur.parent != nil {
		parent = c.cur.parent
	}

	c.parent, err = newChunker(ctx, parent, c.level+1, c.ns)
	if err != nil {
		return err
	}

	return nil
}

func (c *Chunker) anyPending() bool {
	if c.builder.count() > 0 {
		return true
	}

	if c.parent != nil {
		return c.parent.anyPending()
	}

	return false
}

func (c *Chunker) isLeaf() bool {
	return c.level == 0
}

func (c *Chunker) Done(ctx context.Context) (*Node, error) {
	if c.done {
		return nil, fmt.Errorf("repeated done for a chunker")
	}
	c.done = true

	if c.parent != nil && c.parent.anyPending() {
		if c.builder.count() > 0 {
			if err := c.handleBoundary(ctx); err != nil {
				return nil, err
			}
		}

		return c.parent.Done(ctx)
	}

	if c.isLeaf() || c.builder.count() > 1 {
		novel, err := writeNewNode(ctx, c.ns, c.builder)
		return novel.node, err
	}

	return getCanonicalRoot(ctx, c.ns, c.builder)
}

func getCanonicalRoot(ctx context.Context, ns *NodeStore, builder *nodeBuilder) (*Node, error) {
	cnt := builder.count()
	if cnt != 1 {
		return nil, fmt.Errorf("invalid count")
	}
	nd := builder.build()
	childAddr := nd.getAddress(0)

	for {
		child, err := ns.Read(ctx, childAddr)
		if err != nil {
			return nil, err
		}

		if child.IsLeaf() || child.Count() > 1 {
			return child, nil
		}
		childAddr = child.getAddress(0)
	}

}
