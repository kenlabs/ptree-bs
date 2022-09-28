package tree

const (
	DefaultMinChunkSize = 1 << 9
	DefaultMaxChunkSize = 1 << 14
)

type ChunkStrategy string

const (
	KeySplitter ChunkStrategy = "keySplitter"
	RollingHash ChunkStrategy = "rollingHash"
)

type ChunkConfig struct {
	MinChunkSize   uint32
	MaxChunkSize   uint32
	ChunkStrategy  ChunkStrategy
	KeySplitterCfg *KeySplitterConfig
	RollingHashCfg *RollingHashConfig
}

type KeySplitterConfig struct {
	TargetSize float64
	K          float64
	L          float64
}

type RollingHashConfig struct {
	RollingHashWindow uint32
}

var chunkCfg = &ChunkConfig{
	MinChunkSize:  DefaultMinChunkSize,
	MaxChunkSize:  DefaultMaxChunkSize,
	ChunkStrategy: KeySplitter,
	KeySplitterCfg: &KeySplitterConfig{
		TargetSize: 4096,
		K:          4,
		L:          4096,
	},
	RollingHashCfg: &RollingHashConfig{
		RollingHashWindow: 67,
	},
}

func SetGlobalChunkConfig(cfg *ChunkConfig) {
	if cfg == nil {
		panic("can not set config nil")
	}
	chunkCfg = cfg
}
