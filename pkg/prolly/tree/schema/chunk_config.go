package schema

const (
	DefaultMinChunkSize = 1 << 9
	DefaultMaxChunkSize = 1 << 14
)

type ChunkStrategy string

const (
	KeySplitter ChunkStrategy = "keySplitter"
	RollingHash ChunkStrategy = "rollingHash"
)

// Chunk Config for prolly tree, it includes some global setting, the splitter method you choose and specific configs about
// the splitter
type ChunkConfig struct {
	MinChunkSize   uint32
	MaxChunkSize   uint32
	ChunkStrategy  ChunkStrategy
	KeySplitterCfg *KeySplitterConfig
	RollingHashCfg *RollingHashConfig
}

// chunk config about key splitter,
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
type KeySplitterConfig struct {
	K float64
	L float64
}

// rollingHashSplitter is a nodeSplitter that makes chunk boundary decisions using
// a rolling value hasher that processes Item pairs in a byte-wise fashion.
//
// rollingHashSplitter uses a dynamic hash pattern designed to constrain the chunk
// Size distribution by reducing the likelihood of forming very large or very small
// chunks. As the Size of the current chunk grows, rollingHashSplitter changes the
// target pattern to make it easier to match. The result is a chunk Size distribution
type RollingHashConfig struct {
	// The window Size to use for computing the rolling hash. This is way more than necessary assuming random data
	// (two bytes would be sufficient with a target chunk Size of 4k). The benefit of a larger window is it allows
	// for better distribution on input with lower entropy. At a target chunk Size of 4k, any given byte changing
	// has roughly a 1.5% chance of affecting an existing boundary, which seems like an acceptable trade-off. The
	// choice of a prime number provides better distribution for repeating input.
	RollingHashWindow uint32
}

func DefaultChunkConfig() *ChunkConfig {
	return &ChunkConfig{
		MinChunkSize:  DefaultMinChunkSize,
		MaxChunkSize:  DefaultMaxChunkSize,
		ChunkStrategy: KeySplitter,
		KeySplitterCfg: &KeySplitterConfig{
			K: 4,
			L: 4096,
		},
		RollingHashCfg: &RollingHashConfig{
			RollingHashWindow: 67,
		},
	}
}

func (cfg *ChunkConfig) Equal(config *ChunkConfig) bool {
	if cfg.ChunkStrategy != config.ChunkStrategy || cfg.MaxChunkSize != config.MaxChunkSize || cfg.MinChunkSize != config.MinChunkSize {
		return false
	}
	if cfg.ChunkStrategy == KeySplitter {
		return cfg.KeySplitterCfg.L == config.KeySplitterCfg.L &&
			cfg.KeySplitterCfg.K == config.KeySplitterCfg.K
	} else {
		return cfg.RollingHashCfg.RollingHashWindow == config.RollingHashCfg.RollingHashWindow
	}
}
