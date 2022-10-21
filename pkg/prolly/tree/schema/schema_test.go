package schema

import (
	"github.com/ipfs/go-cid"
	"github.com/zeebo/assert"
	"testing"
)

func TestGenIPLDNode(t *testing.T) {
	c1, err := LinkProto.Sum([]byte("link1"))
	assert.NoError(t, err)

	nd := &ProllyNode{
		Keys:   [][]byte{[]byte("123k")},
		Values: [][]byte{[]byte("123v")},
		Links:  []cid.Cid{c1},
		Level:  0,
		Count:  0,
		Cfg:    cid.Undef,
	}

	_, err = nd.ToNode()
	assert.NoError(t, err)

	t.Log(nd.Keys)
	t.Log(nd.Values)
	t.Log(nd.GetAddress(0))
	t.Log(string(nd.GetKey(0)))
}
