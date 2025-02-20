package bitmap

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
)

func TestXor(_ *testing.T) {
	bitmap := &Bitmap{}
	bitmap.Set(0).Set(1).Set(2).Set(3)

	bitmap1 := &Bitmap{}
	bitmap1.Set(0)

	bitmap2 := &Bitmap{}
	bitmap2.Set(0).Set(1)

	bitmap.Xor(bitmap1)
	bitmap.Xor(bitmap2)

	fmt.Print(bitmap.String())
}

func TestRand(_ *testing.T) {

	for i := 0; i < 100; i++ {

		//time.Sleep(1 * time.Second)
		sha256 := sha256.New()
		sha256.Write([]byte("1"))
		sha := sha256.Sum(nil)
		seed := binary.BigEndian.Uint64(sha)
		source := rand.NewSource(int64(seed))
		rr := rand.New(source)
		fmt.Println(rr.Int63n(100))
	}

}
func TestAnd(_ *testing.T) {
	bitmap := &Bitmap{}
	bitmap.Set(0).Set(1).Set(2).Set(900)

	bitmap1 := &Bitmap{}
	bitmap1.Set(900)

	fmt.Print(bitmap.InterExist(bitmap1))
}
