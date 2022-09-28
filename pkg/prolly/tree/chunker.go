package tree

import (
	"context"
	"fmt"
	"ptree-bs/pkg/prolly/tree/schema"
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
	return newChunker(ctx, nil, 0, ns)
}

func newChunker(ctx context.Context, cur *Cursor, level int, ns *NodeStore) (*Chunker, error) {
	var splitter nodeSplitter
	switch chunkCfg.ChunkStrategy {
	case KeySplitter:
		splitter = defaultSplitterFactory(uint8(level % 256))
	case RollingHash:
		splitter = newRollingHashSplitter(uint8(levelSalt[level%256]))
	default:
		panic(fmt.Errorf("unsupported chunk strategy: %s", chunkCfg.ChunkStrategy))
	}

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
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Chunker) AddPair(ctx context.Context, key, value []byte) error {
	_, err := c.append(ctx, key, value, 1)
	return err
}

func (c *Chunker) UpdatePair(ctx context.Context, key, value []byte) error {
	if err := c.skip(ctx); err != nil {
		return err
	}
	_, err := c.append(ctx, key, value, 1)
	return err
}

func (c *Chunker) DeletePair(ctx context.Context, _, _ []byte) error {
	return c.skip(ctx)
}

func (c *Chunker) skip(ctx context.Context) error {
	err := c.cur.Advance(ctx)
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

func (c *Chunker) finalizeCursor(ctx context.Context) error {
	for c.cur.Valid() {
		ok, err := c.append(ctx,
			c.cur.CurrentKey(),
			c.cur.CurrentValue(),
			c.cur.CurrentSubtreeSize())
		if err != nil {
			return err
		}
		if ok && c.cur.atNodeEnd() {
			break
		}

		err = c.cur.Advance(ctx)
		if err != nil {
			return err
		}
	}

	if c.cur.parent != nil {
		err := c.cur.parent.Advance(ctx)
		if err != nil {
			return err
		}

		c.cur.nd = schema.ProllyNode{}
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

func (c *Chunker) Done(ctx context.Context) (schema.ProllyNode, error) {
	if c.done {
		return schema.ProllyNode{}, fmt.Errorf("repeated done for a chunker")
	}
	c.done = true

	if c.cur != nil {
		if err := c.finalizeCursor(ctx); err != nil {
			return schema.ProllyNode{}, err
		}
	}

	if c.parent != nil && c.parent.anyPending() {
		if c.builder.count() > 0 {
			if err := c.handleBoundary(ctx); err != nil {
				return schema.ProllyNode{}, err
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

func getCanonicalRoot(ctx context.Context, ns *NodeStore, builder *nodeBuilder) (schema.ProllyNode, error) {
	cnt := builder.count()
	if cnt != 1 {
		return schema.ProllyNode{}, fmt.Errorf("invalid count")
	}
	nd := builder.build()
	childAddr := nd.GetAddress(0)

	for {
		child, err := ns.Read(ctx, childAddr)
		if err != nil {
			return schema.ProllyNode{}, err
		}

		if child.IsLeaf() || child.ItemCount() > 1 {
			return child, nil
		}
		childAddr = child.GetAddress(0)
	}

}

func (c *Chunker) AdvanceTo(ctx context.Context, next *Cursor) error {
	cmp := c.cur.Compare(next)
	if cmp == 0 {
		return nil
	} else if cmp > 0 {
		for c.cur.Compare(next) > 0 {
			if err := next.Advance(ctx); err != nil {
				return err
			}
		}
		return nil
	}

	split, err := c.append(ctx, c.cur.CurrentKey(), c.cur.CurrentValue(), c.cur.CurrentSubtreeSize())
	if err != nil {
		return err
	}

	for !(split && c.cur.atNodeEnd()) {
		err = c.cur.Advance(ctx)
		if err != nil {
			return err
		}
		if cmp = c.cur.Compare(next); cmp >= 0 {
			// we caught up before synchronizing
			return nil
		}
		split, err = c.append(ctx, c.cur.CurrentKey(), c.cur.CurrentValue(), c.cur.CurrentSubtreeSize())
		if err != nil {
			return err
		}
	}

	if c.cur.parent == nil || next.parent == nil {
		c.cur.copy(next)
		return nil
	}

	if c.cur.parent.Compare(next.parent) == 0 {
		c.cur.copy(next)
		return nil
	}

	err = c.cur.parent.Advance(ctx)
	if err != nil {
		return err
	}
	c.cur.invalidateAtEnd()

	err = c.parent.AdvanceTo(ctx, next.parent)
	if err != nil {
		return err
	}

	c.cur.copy(next)

	err = c.processPrefix(ctx)
	if err != nil {
		return err
	}
	return nil
}
