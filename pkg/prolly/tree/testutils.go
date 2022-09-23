package tree

import (
	"math/rand"
	"sort"
)

var testRand = rand.New(rand.NewSource(1))

func RandomTuplePairs(count int) [][2][]byte {
	data := make([][2][]byte, count)
	for i := range data {
		key := make([]byte, (testRand.Int63()%30)+15)
		val := make([]byte, (testRand.Int63()%30)+15)
		testRand.Read(key)
		testRand.Read(val)
		data[i][0] = key
		data[i][1] = val
	}

	dupes := make([]int, 0, count)
	for {
		sort.Slice(data, func(i, j int) bool {
			return DefaultBytesCompare(data[i][0], data[j][0]) < 0
		})
		for i := range data {
			if i == 0 {
				continue
			}
			if DefaultBytesCompare(data[i][0], data[i-1][0]) == 0 {
				dupes = append(dupes, i)
			}
		}
		if len(dupes) == 0 {
			break
		}

		// replace duplicates and validate again
		for _, d := range dupes {
			key := make([]byte, (testRand.Int63()%30)+15)
			testRand.Read(key)
			data[d][0] = key
		}
		dupes = dupes[:0]
	}

	return data
}
