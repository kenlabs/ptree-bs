package schema

import (
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/zeebo/assert"
	"testing"
)

func TestGenIPLDNode(t *testing.T) {
	c1, err := LinkProto.Sum([]byte("link1"))
	assert.NoError(t, err)
	var lnk1 ipld.Link
	lnk1 = cidlink.Link{Cid: c1}

	nd := &ProllyNode{
		Keys:       [][]byte{[]byte("123k")},
		Values:     [][]byte{[]byte("123v")},
		Links:      []*ipld.Link{&lnk1},
		Size:       0,
		Level:      0,
		Count:      0,
		Subtrees:   []uint64{1, 2, 5},
		Totalcount: 1,
	}

	_, err = nd.ToNode()
	assert.NoError(t, err)

	t.Log(nd.Keys)
	t.Log(nd.Values)
	t.Log(nd.GetAddress(0))
	t.Log(string(nd.GetKey(0)))
	t.Log(nd.Totalcount)
}
