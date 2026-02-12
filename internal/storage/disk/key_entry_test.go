package disk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyEntry(t *testing.T) {
	keyEntry1 := NewKeyEntryFromInt(8)
	assert.Equal(t, keyEntry1.KeyLen, uint16(8))
	assert.Equal(t, keyEntry1.Key, [64]uint8{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8})

	input := []byte{0, 0, 0, 0, 255, 255, 1, 2}
	keyEntry2 := NewKeyEntryFromBytes(input)
	assert.Equal(t, keyEntry2.KeyLen, uint16(len(input)))
	assert.Equal(t, keyEntry2.Key, [64]uint8 {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,255,255,1,2})
}