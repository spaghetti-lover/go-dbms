package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindLastLE(t *testing.T) {
	nums := [3]int{5, 10, 15}
	assert.Equal(t, -1, FindLastLE(nums[:], 3, 3)) // Less than all
	assert.Equal(t, 0, FindLastLE(nums[:], 3, 5))  // Equal to first
	assert.Equal(t, 0, FindLastLE(nums[:], 3, 7))  // Between first and second
	assert.Equal(t, 1, FindLastLE(nums[:], 3, 10)) // Equal to second
	assert.Equal(t, 2, FindLastLE(nums[:], 3, 15)) // Equal to last
	assert.Equal(t, 2, FindLastLE(nums[:], 3, 20)) // Greater than all
}
