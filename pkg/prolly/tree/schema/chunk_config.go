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
	// The window Size to use for computing the rolling hash. This is way more than necessary assuming random data
	// (two bytes would be sufficient with a target chunk Size of 4k). The benefit of a larger window is it allows
	// for better distribution on input with lower entropy. At a target chunk Size of 4k, any given byte changing
	// has roughly a 1.5% chance of affecting an existing boundary, which seems like an acceptable trade-off. The
	// choice of a prime number provides better distribution for repeating input.
	RollingHashWindow uint32
}

//var chunkCfg = &ChunkConfig{
//	MinChunkSize:  DefaultMinChunkSize,
//	MaxChunkSize:  DefaultMaxChunkSize,
//	ChunkStrategy: KeySplitter,
//	KeySplitterCfg: &KeySplitterConfig{
//		TargetSize: 4096,
//		K:          4,
//		L:          4096,
//	},
//	RollingHashCfg: &RollingHashConfig{
//		RollingHashWindow: 67,
//	},
//}

func DefaultChunkConfig() *ChunkConfig {
	return &ChunkConfig{
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
}

func (cfg *ChunkConfig) Equal(config *ChunkConfig) bool {
	if cfg.ChunkStrategy != config.ChunkStrategy || cfg.MaxChunkSize != config.MaxChunkSize || cfg.MinChunkSize != config.MinChunkSize {
		return false
	}
	if cfg.ChunkStrategy == KeySplitter {
		return cfg.KeySplitterCfg.L == config.KeySplitterCfg.L &&
			cfg.KeySplitterCfg.K == config.KeySplitterCfg.K &&
			cfg.KeySplitterCfg.TargetSize == config.KeySplitterCfg.TargetSize
	} else {
		return cfg.RollingHashCfg.RollingHashWindow == config.RollingHashCfg.RollingHashWindow
	}
}

//func SetGlobalChunkConfig(cfg *ChunkConfig) {
//	if cfg == nil {
//		panic("can not set config nil")
//	}
//	chunkCfg = cfg
//}
//
//func SetDefaultChunkConfig() {
//	chunkCfg = &ChunkConfig{
//		MinChunkSize:  DefaultMinChunkSize,
//		MaxChunkSize:  DefaultMaxChunkSize,
//		ChunkStrategy: KeySplitter,
//		KeySplitterCfg: &KeySplitterConfig{
//			TargetSize: 4096,
//			K:          4,
//			L:          4096,
//		},
//		RollingHashCfg: &RollingHashConfig{
//			RollingHashWindow: 67,
//		},
//	}
//}
