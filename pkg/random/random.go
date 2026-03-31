package random

import "math/rand/v2"

func RandInt(min, max int) int {
	return rand.Int()%(max-min+1) + min
}
