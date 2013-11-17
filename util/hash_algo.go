package util

// this is copied from redis.

/* Generic hash function (a popular one from Bernstein).
 * I tested a few and this was the best. */
func HashFunction(buf []byte) uint32 {
	var hash uint32 = 5381
	for _, b := range buf {
		hash = ((hash << 5) + hash) + uint32(b)
	}
	return hash
}

/* And a case insensitive version */
/*func CaseHashFunction(buf []byte) uint32 {
	var hash uint32 = 5381

	for _, b := range buf {
		hash = ((hash << 5) + hash) + (bytes.ToLower(b)) // hash * 33 + c
	}
	return hash
}
*/
