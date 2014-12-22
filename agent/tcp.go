package agent

import (
    "net"
)

type tcpServer struct {
    ctx *context
}

func (t *tcpServer) Handle(clientConn net.Conn) {
    t.ctx.svr.logf("TCP: new client(%s)", clientConn.RemoteAddr())
    
}
