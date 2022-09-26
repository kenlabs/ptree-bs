package config

const (
	DefaultMinChunkSize = 1 << 9
	DefaultMaxChunkSize = 1 << 14
)

type ChunkStrategy string

const (
	KeySplitter ChunkStrategy = "keySplitter"
	RollingHash ChunkStrategy = "rollingHash"
)

func DefaultChunkConfig() *ChunkConfig {
	return &ChunkConfig{
		MinChunkSize: DefaultMinChunkSize,
		MaxChunkSize: DefaultMaxChunkSize,
		Strategy:     KeySplitter,
	}
}

type ChunkConfig struct {
	MinChunkSize uint32
	MaxChunkSize uint32
	Strategy     ChunkStrategy
}
