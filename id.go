package kad

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"math/bits"
)

const IDLength = 20

type ID []byte

func NewID() ID {
	randBytes := make([]byte, 128)
	if _, err := rand.Reader.Read(randBytes); err != nil {
		panic(err)
	}

	id := sha1.Sum(randBytes)
	return id[:]
}

func IDFromString(s string) ID {
	id := sha1.Sum([]byte(s))
	return id[:]
}

func (id ID) Equal(other ID) bool {
	return bytes.Equal(id, other)
}

func (id ID) DistanceTo(other ID) int {
	if len(id) != IDLength || len(other) != IDLength {
		panic("wrong ID length")
	}

	var matchingPrefixLen int
	for i := 0; i < IDLength; i++ {
		leadingZeros := bits.LeadingZeros8(id[i] ^ other[i])
		matchingPrefixLen += leadingZeros

		// this means we reached the first pair of bits that do not match
		if leadingZeros < 8 {
			break
		}
	}

	d := 159 - matchingPrefixLen
	if d < 0 {
		return 0
	}
	return d
}

func (id ID) String() string {
	return hex.EncodeToString(id)
}
