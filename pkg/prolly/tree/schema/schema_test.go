package schema

import (
	"github.com/ipfs/go-cid"
	"github.com/zeebo/assert"
	"testing"
)

func TestGenIPLDNode(t *testing.T) {
	cid1, _ := LinkProto.Sum([]byte("123v"))

	nd := &ProllyNode{
		Keys:        [][]byte{[]byte("123k")},
		Values:      []cid.Cid{cid1},
		Level:       0,
		ChunkConfig: cid.Undef,
	}

	_, err := nd.ToNode()
	assert.NoError(t, err)

	assert.Equal(t, nd.GetKey(0), []byte("123k"))
	assert.Equal(t, nd.GetValue(0), cid1.Bytes())
}
