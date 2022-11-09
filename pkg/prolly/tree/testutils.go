package tree

import (
	"encoding/binary"
	"github.com/sethvargo/go-diceware/diceware"
	"math/rand"
	"sort"
	"strings"
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

func RandomIntTuplePairs(count int) [][2][]byte {
	data := make([][2][]byte, count)
	for i := range data {
		key := make([]byte, 10)
		val := make([]byte, 10)
		n1 := testRand.Int63()
		n2 := testRand.Int63()
		binary.PutVarint(key, n1)
		binary.PutVarint(val, n2)

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
			key := make([]byte, 10)
			n1 := testRand.Int63()
			binary.PutVarint(key, n1)
			data[d][0] = key
		}
		dupes = dupes[:0]
	}

	return data
}

func RandomStringTuplePairs(count int) [][2][]byte {
	data := make([][2][]byte, count)
	for i := range data {
		keys, _ := diceware.Generate(2)
		key := joinStrings(keys)
		vals, _ := diceware.Generate(5)
		val := joinStrings(vals)

		data[i][0] = []byte(key)
		data[i][1] = []byte(val)
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
			keys, _ := diceware.Generate(2)
			key := joinStrings(keys)

			data[d][0] = []byte(key)
		}
		dupes = dupes[:0]
	}

	return data
}

func joinStrings(strs []string) string {
	for i, key := range strs {
		bytesKey := []byte(key)
		bytesKey[0] -= 32
		strs[i] = string(bytesKey)
	}
	res := strings.Join(strs, "")
	return res
}
