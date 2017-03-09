package server

import (
	"log"
	"net"
	"os"

	"bitbucket.org/shailesh33/dynomite/common"
)

func ListenAndServe(listen string, msgForwarder common.MsgForwarder) {
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
		go handleClient(conn, msgForwarder)
	}
}

func handleClient(clientConn net.Conn, msgForwarder common.MsgForwarder) {
	defer clientConn.Close()
	c, err := newClientConnHandler(clientConn, msgForwarder)
	if err != nil {
		log.Println("Failed to handle client ", err)
	}
	c.Run()
}
