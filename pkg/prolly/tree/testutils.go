package tree

import (
	"encoding/binary"
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
		key := RandNumString(15)
		val := RandNumString(20)

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
			key := RandAllString(15)

			data[d][0] = []byte(key)
		}
		dupes = dupes[:0]
	}

	return data
}

var CHARS = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
	"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
	"1", "2", "3", "4", "5", "6", "7", "8", "9", "0"}

/*RandAllString  ([a~zA~Z0~9])
 */
func RandAllString(lenNum int) string {
	str := strings.Builder{}
	length := len(CHARS)
	for i := 0; i < lenNum; i++ {
		l := CHARS[rand.Intn(length)]
		str.WriteString(l)
	}
	return str.String()
}

/*RandNumString ([0~9])
 */
func RandNumString(lenNum int) string {
	str := strings.Builder{}
	length := 10
	for i := 0; i < lenNum; i++ {
		str.WriteString(CHARS[52+rand.Intn(length)])
	}
	return str.String()
}

/*RandString  (a~zA~Z])
 */
func RandString(lenNum int) string {
	str := strings.Builder{}
	length := 52
	for i := 0; i < lenNum; i++ {
		str.WriteString(CHARS[rand.Intn(length)])
	}
	return str.String()
}
