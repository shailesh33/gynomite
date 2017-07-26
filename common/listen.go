package common

import (
	"log"
	"net"
	"os"
)

func ListenAndServe(listen string, chc ConnCreator, placement INodePlacement, msgForwarder IMsgForwarder) {
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		log.Println("Error listening on ", listen, err.Error())
		os.Exit(1)
	}
	defer listener.Close()
	log.Println("Listening on ", listen)
	for {
		// Listen for an incoming connection.
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go func(conn net.Conn, placement INodePlacement, msgForwarder IMsgForwarder) {
			c, err := chc(conn, placement, msgForwarder)
			if err != nil {
				log.Println("Failed to handle client ", err)
			}
			go c.Run()
		}(conn, placement, msgForwarder)
	}
}
