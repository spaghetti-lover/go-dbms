package utils

func FindLastLE(nums []int, length, key int) int {
	idx := -1

	l, r := 0, length-1
	for l <= r {
		m := l + (r-l)/2

		if nums[m] <= key {
			idx = m
			l = m + 1
		} else {
			r = m - 1
		}
	}

	return idx
}
