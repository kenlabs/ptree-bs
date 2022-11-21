// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"bytes"
	"context"
	"github.com/ipfs/go-cid"
	"ptree-bs/pkg/prolly/skip"
	"ptree-bs/pkg/prolly/tree/schema"
)

type MutationIter interface {
	NextMutation(ctx context.Context) (key, value []byte)
	Close() error
}

type OrderedListIter struct {
	Iter *skip.ListIter
}

func (it *OrderedListIter) NextMutation(context.Context) ([]byte, []byte) {
	k, v := it.Iter.Current()
	if k == nil {
		return nil, nil
	}
	it.Iter.Advance()
	return k, v
}

func (it *OrderedListIter) Close() error {
	return nil
}

func ApplyMutations(ctx context.Context, ns *NodeStore, root schema.ProllyNode, chunkCfg *schema.ChunkConfig, edits MutationIter, compare CompareFn) (schema.ProllyNode, error) {
	newKey, newValue := edits.NextMutation(ctx)
	if newKey == nil {
		// no update
		return root, nil
	}

	cur, err := NewCursorAtKey(ctx, ns, root, newKey, compare)
	if err != nil {
		return schema.ProllyNode{}, err
	}

	ck, err := newChunker(ctx, cur.Clone(), 0, chunkCfg, cid.Undef, ns)
	if err != nil {
		return schema.ProllyNode{}, err
	}

	for newKey != nil {
		err = cur.seek(ctx, newKey, compare)
		if err != nil {
			return schema.ProllyNode{}, err
		}

		var oldValue []byte
		if cur.Valid() {
			if compare(newKey, cur.CurrentKey()) == 0 {
				oldValue = cur.CurrentValue()
			}
		}

		if equalValues(newValue, oldValue) {
			newKey, newValue = edits.NextMutation(ctx)
			continue
		}

		err = ck.AdvanceTo(ctx, cur)
		if err != nil {
			return schema.ProllyNode{}, err
		}

		if oldValue == nil {
			err = ck.AddPair(ctx, newKey, newValue)
		} else {
			if newValue != nil {
				err = ck.UpdatePair(ctx, newKey, newValue)
			} else {
				err = ck.DeletePair(ctx, newKey, newValue)
			}
		}
		if err != nil {
			return schema.ProllyNode{}, err
		}

		newKey, newValue = edits.NextMutation(ctx)
	}

	return ck.Done(ctx)
}

func equalValues(left, right []byte) bool {
	return bytes.Equal(left, right)
}
