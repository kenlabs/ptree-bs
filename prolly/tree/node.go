package tree

import "math"

const (
	MaxNodeSize = uint64(math.MaxUint16)
)

type Node struct {
	Keys, Values [][]byte
	Size, Level  int
	Subtrees     []uint64
}

func (nd *Node) Count() int {
	return len(nd.Keys)
}

func sumSubtrees(subtrees []uint64) (sum uint64) {
	for i := range subtrees {
		sum += subtrees[i]
	}
	return
}

// todo overflow?
func (nd *Node) TreeCount() int {
	return int(sumSubtrees(nd.Subtrees))
}
