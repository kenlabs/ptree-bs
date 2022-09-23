package linksystem

import (
	"bytes"
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"io"
	log "ptree-bs/pkg/log"
)

var logger = log.NewSubsystemLogger()

func MkLinkSystem(bs blockstore.Blockstore) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.TrustedStorage = true
	lsys.StorageReadOpener = func(lnkCtx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		asCidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("unsupported link types")
		}
		block, err := bs.Get(lnkCtx.Ctx, asCidLink.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(block.RawData()), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			//codec := lnk.(cidlink.Link).Prefix().Codec
			origBuf := buf.Bytes()

			//// Decode the node to check its type.
			//_, err := decodeIPLDNode(codec, buf, tree.IPLDNodePrototype)
			//if err != nil {
			//	return fmt.Errorf("error decoding IPLD node in linksystem, err: %v", err)
			//}

			block, err := blocks.NewBlockWithCid(origBuf, c)
			if err != nil {
				return err
			}
			logger.Debugf("Received unexpected IPLD node, cid: %s", c.String())
			return bs.Put(lctx.Ctx, block)
		}, nil
	}
	return lsys
}

// decodeIPLDNode decodes an ipld.Node from bytes read from an io.Reader.
func decodeIPLDNode(codec uint64, r io.Reader, prototype ipld.NodePrototype) (ipld.Node, error) {
	// NOTE: Considering using the schema prototypes.  This was failing, using
	// a map gives flexibility.  Maybe is worth revisiting this again in the
	// future.
	nb := prototype.NewBuilder()
	decoder, err := multicodec.LookupDecoder(codec)
	if err != nil {
		return nil, err
	}
	err = decoder(nb, r)
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}
