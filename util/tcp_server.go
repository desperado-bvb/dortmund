package util

import (
    "fmt"
    "net"
    "runtime"
    "strings"
)

type TCPHandler interface {
    Handle(net.Conn)
}

func TCPServer(listener net.Listener, handler TCPHandler, l logger) {
    l.Output(2, fmt.Sprintf("TCP: listening on %s", listener.Addr()))

    for {
	clientConn, err := listener.Accept()
	if err != nil {
            if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
		l.Output(2, fmt.Sprintf("NOTICE: temporary Accept() failure - %s", err))
		runtime.Gosched()
		continue
	    }

	    if !strings.Contains(err.Error(), "use of closed network connection") {
		l.Output(2, fmt.Sprintf("ERROR: listener.Accept() - %s", err))
	    }

	    break
	}

	go handler.Handle(clientConn)
    }

    l.Output(2, fmt.Sprintf("TCP: closing %s", listener.Addr()))
}

