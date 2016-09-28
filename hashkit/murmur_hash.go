package hashkit

import "fmt"

func hash_murmur(key string) uint64 {
	/*
	 * 'm' and 'r' are mixing constants generated offline.  They're not
 	 * really 'magic', they just happen to work well.
 	 */
	length := uint64(len(key))
	seed := (0xdeadbeef * length)
	/* Initialize the hash to a 'random' value */
	var h uint64 = seed ^ length;

	var m uint64 = 0x5bd1e995
	const r uint = 24;
	subArr := key[0:4]
	/* Mix 4 bytes at a time into the hash */
	for len(subArr) == 4 {
		fmt.Printf("subArr:%q\n", subArr)
		k := uint64(subArr[0]) * 0xFFFFFF + uint64(subArr[1]) * 0xFFFF + uint64(subArr[2]) * 0xFF + uint64(subArr[3])

		k *= m;
		k ^= k >> r;
		k *= m;

		h *= m;
		h ^= k;

		key =  key[4:]

		if len(key) > 4 {
			subArr = key[:4]
		} else {
			subArr = key[:]
		}

	}
	/* Handle the last few bytes of the input array */
	fmt.Printf("subArr:%q\n", subArr)

	switch(len(subArr)) {
		case 3:
		h ^= uint64(subArr[2]) << 16;

		case 2:
		h ^= uint64(subArr[1]) << 8;

		case 1:
		h ^= uint64(subArr[0])
		h *= m;

		default:
		break;
	};

	/*
	 * Do a few final mixes of the hash to ensure the last few bytes are
	 * well-incorporated.
	 */

	h ^= h >> 13;
	h *= m;
	h ^= h >> 15;

//size_dyn_token(token, 1);
//set_int_dyn_token(token, h);
	return h

}
