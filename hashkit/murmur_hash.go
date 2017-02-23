package hashkit

func hash_murmur(key string) uint32 {
	/*
			 * 'm' and 'r' are mixing constants generated offline.  They're not
		 	 * really 'magic', they just happen to work well.
	*/
	length := uint32(len(key))
	seed := (0xdeadbeef * length)
	/* Initialize the hash to a 'random' value */
	var h uint32 = seed ^ length

	var m uint32 = 0x5bd1e995
	const r uint32 = 24
	subArr := ""
	if len(key) > 4 {
		subArr = key[0:4]
	} else {
		subArr = key[:]
	}

	/* Mix 4 bytes at a time into the hash */
	for len(subArr) == 4 {
		//
		// k := uint32(subArr[0]) << + uint32(subArr[1]) * 0xFFFF + uint32(subArr[2]) * 0xFF + uint32(subArr[3])
		k := uint32(subArr[0]) + uint32(subArr[1])<<8 + uint32(subArr[2])<<16 + uint32(subArr[3])<<24

		k *= m
		k ^= k >> r
		k *= m

		h *= m
		h ^= k

		key = key[4:]

		if len(key) > 4 {
			subArr = key[:4]
		} else {
			subArr = key[:]
		}

	}
	/* Handle the last few bytes of the input array */
	switch len(subArr) {
	case 3:
		h ^= uint32(subArr[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(subArr[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(subArr[0])
		h *= m
		fallthrough
	default:
		break
	}

	/*
	 * Do a few final mixes of the hash to ensure the last few bytes are
	 * well-incorporated.
	 */

	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h

}
