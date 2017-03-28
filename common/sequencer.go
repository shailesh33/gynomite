package common

var c = make(chan uint64, 5)

func startIdGenerator() {
	var counter uint64 = 0
	for {
		c <- counter
		counter++
	}
}

func GetNextId() uint64 {
	return <-c
}

func init() {
	go startIdGenerator()
}

