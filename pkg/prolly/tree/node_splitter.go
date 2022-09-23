package tree

import (
	"crypto/sha512"
	"encoding/binary"
	"github.com/zeebo/xxh3"
	"math"
)

var levelSalt = [...]uint64{
	saltFromLevel(1),
	saltFromLevel(2),
	saltFromLevel(3),
	saltFromLevel(4),
	saltFromLevel(5),
	saltFromLevel(6),
	saltFromLevel(7),
	saltFromLevel(8),
	saltFromLevel(9),
	saltFromLevel(10),
	saltFromLevel(11),
	saltFromLevel(12),
	saltFromLevel(13),
	saltFromLevel(14),
	saltFromLevel(15),
}

const (
	minChunkSize = 1 << 9
	maxChunkSize = 1 << 14
)

var defaultSplitterFactory splitterFactory = newKeySplitter

// splitterFactory makes a nodeSplitter.
type splitterFactory func(level uint8) nodeSplitter

// nodeSplitter decides where []byte streams should be split into chunks.
type nodeSplitter interface {
	// Append provides more nodeItems to the splitter. Splitter's make chunk
	// boundary decisions based on the []byte contents. Upon return, callers
	// can use CrossedBoundary() to see if a chunk boundary has crossed.
	Append(key, values []byte) error

	// CrossedBoundary returns true if the provided nodeItems have caused a chunk
	// boundary to be crossed.
	CrossedBoundary() bool

	// Reset resets the state of the splitter.
	Reset()
}

// keySplitter is a nodeSplitter that makes chunk boundary decisions on the hash of
// the key of a []byte pair. In contrast to the rollingHashSplitter, keySplitter
// tries to create chunks that have an average number of []byte pairs, rather than
// an average number of Bytes. However, because the target number of []byte pairs
// is computed directly from the chunk size and count, the practical difference in
// the distribution of chunk sizes is minimal.
//
// keySplitter uses a dynamic threshold modeled on a weibull distribution
// (https://en.wikipedia.org/wiki/Weibull_distribution). As the size of the current
// trunk increases, it becomes easier to pass the threshold, reducing the likelihood
// of forming very large or very small chunks.
type keySplitter struct {
	count, size     uint32
	crossedBoundary bool

	salt uint64
}

const (
	targetSize float64 = 4096
	maxUint32  float64 = math.MaxUint32

	// weibull params
	K = 4.

	// TODO: seems like this should be targetSize / math.Gamma(1 + 1/K).
	L = targetSize
)

func newKeySplitter(level uint8) nodeSplitter {
	return &keySplitter{
		salt: levelSalt[level],
	}
}

var _ splitterFactory = newKeySplitter

func (ks *keySplitter) Append(key, value []byte) error {
	// todo(andy): account for key/value offsets, vtable, etc.
	thisSize := uint32(len(key) + len(value))
	ks.size += thisSize

	if ks.size < minChunkSize {
		return nil
	}
	if ks.size > maxChunkSize {
		ks.crossedBoundary = true
		return nil
	}

	h := xxHash32(key, ks.salt)
	ks.crossedBoundary = weibullCheck(ks.size, thisSize, h)
	return nil
}

func (ks *keySplitter) CrossedBoundary() bool {
	return ks.crossedBoundary
}

func (ks *keySplitter) Reset() {
	ks.size = 0
	ks.crossedBoundary = false
}

// weibullCheck returns true if we should split
// at |hash| for a given record inserted into a
// chunk of Size |Size|, where the record's Size
// is |thisSize|. |Size| is the Size of the chunk
// after the record is inserted, so includes
// |thisSize| in it.
//
// weibullCheck attempts to form chunks whose
// sizes match the weibull distribution.
//
// The logic is as follows: given that we haven't
// split on any of the records up to |Size - thisSize|,
// the probability that we should split on this record
// is (CDF(end) - CDF(start)) / (1 - CDF(start)), or,
// the precentage of the remaining portion of the CDF
// that this record actually covers. We split is |hash|,
// treated as a uniform random number between [0,1),
// is less than this percentage.
func weibullCheck(size, thisSize, hash uint32) bool {
	startx := float64(size - thisSize)
	start := -math.Expm1(-math.Pow(startx/L, K))

	endx := float64(size)
	end := -math.Expm1(-math.Pow(endx/L, K))

	p := float64(hash) / maxUint32
	d := 1 - start
	if d <= 0 {
		return true
	}
	target := (end - start) / d
	return p < target
}

func xxHash32(b []byte, salt uint64) uint32 {
	return uint32(xxh3.HashSeed(b, salt))
}

func saltFromLevel(level uint8) (salt uint64) {
	full := sha512.Sum512([]byte{level})
	return binary.LittleEndian.Uint64(full[:8])
}
