package bitmap

import (
	"bytes"
	"fmt"
	"math"
)

type Bitmap struct {
	Words []uint64
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (bitmap *Bitmap) Fill1(length int) *Bitmap {
	word, bit := length/64, uint(length%64)
	wl := len(bitmap.Words)
	if wl > word {
		for i := 0; i < wl; i++ {
			if i < word {
				bitmap.Words[i] = math.MaxUint64
			} else if i == word {
				bitmap.Words[i] = uint64((1 << bit) - 1)
			} else {
				bitmap.Words[i] = 0
			}
		}
	} else {
		for i := 0; i < word; i++ {
			if i < wl {
				bitmap.Words[i] = math.MaxUint64
			} else {
				bitmap.Words = append(bitmap.Words, math.MaxUint64)
			}
		}
		bitmap.Words = append(bitmap.Words, uint64((1<<bit)-1))
	}
	return bitmap
}

func (bitmap *Bitmap) fillLen(length int) {
	for len(bitmap.Words) < length {
		bitmap.Words = append(bitmap.Words, 0)
	}
}

func (bitmap *Bitmap) EqualZero() bool {
	for i := 0; i < len(bitmap.Words); i++ {
		if bitmap.Words[i] != 0 {
			return false
		}
	}
	return true
}

func (bitmap *Bitmap) Has(num int) bool {
	word, bit := num/64, uint(num%64)
	return word < len(bitmap.Words) && (bitmap.Words[word]&(1<<bit)) != 0
}

// setbit(x, y)  x|=(1<<y) // 将X的第Y位置1
func (bitmap *Bitmap) Set(num int) *Bitmap {
	word, bit := num/64, uint(num%64)
	for word >= len(bitmap.Words) {
		bitmap.Words = append(bitmap.Words, 0)
	}
	// 判断num是否已经存在bitmap中
	if bitmap.Words[word]&(1<<bit) == 0 {
		bitmap.Words[word] |= 1 << bit
	}
	return bitmap
}

// clrbit(x,y) x&=～(1<<y) //将X的第Y位清0
func (bitmap *Bitmap) SetZero(num int) *Bitmap {
	word, bit := num/64, uint(num%64)
	bitmap.Words[word] &= ^(1 << bit)
	return bitmap
}

func (bitmap *Bitmap) InterExist(bitmap2 *Bitmap) bool {
	if bitmap2 == nil {
		return false
	}
	for i := 0; i < min(len(bitmap.Words), len(bitmap2.Words)); i++ {
		if bitmap.Words[i]&bitmap2.Words[i] > 0 {
			return true
		}
	}
	return false
}

func (bitmap *Bitmap) Or(bitmap2 *Bitmap) {
	if bitmap2 == nil {
		return
	}
	bitmap.fillLen(len(bitmap2.Words))
	for i := 0; i < len(bitmap2.Words); i++ {
		bitmap.Words[i] = bitmap.Words[i] | bitmap2.Words[i]
	}
}

func (bitmap *Bitmap) And(bitmap2 *Bitmap) *Bitmap {
	var res Bitmap
	res.fillLen(len(bitmap2.Words))
	var minlen int
	if len(bitmap.Words) < len(bitmap2.Words) {
		minlen = len(bitmap.Words)
	} else {
		minlen = len(bitmap2.Words)
	}
	for i := 0; i < minlen; i++ {
		res.Words[i] = bitmap.Words[i] & bitmap2.Words[i]
	}
	return &res
}

func (bitmap *Bitmap) Len() int {
	return len(bitmap.Words) * 64
}

func (bitmap *Bitmap) Conflict(bitmap2 *Bitmap) bool {
	res := false
	var minlen int
	if len(bitmap.Words) < len(bitmap2.Words) {
		minlen = len(bitmap.Words)
	} else {
		minlen = len(bitmap2.Words)
	}
	for i := 0; i < minlen; i++ {
		if bitmap.Words[i]&bitmap2.Words[i] != 0 {
			res = true
			break
		}
	}
	return res
}

func (bitmap *Bitmap) ConflictSet(bitmap2 *Bitmap) []int { // 查找有哪些位发生了冲突
	var minlen int
	ret := make([]int, 0)
	if len(bitmap.Words) < len(bitmap2.Words) {
		minlen = len(bitmap.Words)
	} else {
		minlen = len(bitmap2.Words)
	}
	for i := 0; i < minlen; i++ {
		if bitmap.Words[i]&bitmap2.Words[i] != 0 {
			ret = append(ret, i)
			break
		}
	}
	return ret
}

func (bitmap *Bitmap) Xor(bitmap2 *Bitmap) {
	if bitmap2 == nil {
		return
	}
	bitmap.fillLen(len(bitmap2.Words))
	for i := 0; i < len(bitmap2.Words); i++ {
		bitmap.Words[i] = bitmap.Words[i] ^ bitmap2.Words[i]
	}
}

func (bitmap *Bitmap) Clone() *Bitmap {
	bitmap2 := &Bitmap{
		Words: make([]uint64, len(bitmap.Words)),
	}
	for i, v := range bitmap.Words {
		bitmap2.Words[i] = v
	}
	return bitmap2
}

func (bitmap *Bitmap) Pos1Nums() int {
	var pos int
	for _, v := range bitmap.Words {
		if v == 0 {
			continue
		}
		for j := uint(0); j < 64; j++ {
			if v&(1<<j) != 0 {
				pos = pos + 1
			}
		}
	}
	return pos
}

// 所有为1的Pos
func (bitmap *Bitmap) Pos1() []int {
	var pos []int
	for i, v := range bitmap.Words {
		if v == 0 {
			continue
		}
		for j := uint(0); j < 64; j++ {
			if v&(1<<j) != 0 {
				pos = append(pos, 64*i+int(j))
			}
		}
	}
	return pos
}

func (bitmap *Bitmap) String() string {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, v := range bitmap.Words {
		if v == 0 {
			continue
		}
		for j := uint(0); j < 64; j++ {
			if v&(1<<j) != 0 {
				if buf.Len() > len("{") {
					buf.WriteByte(' ')
				}
				fmt.Fprintf(&buf, "%d", 64*i+int(j))
			}
		}
	}
	buf.WriteByte('}')
	return buf.String()
}
