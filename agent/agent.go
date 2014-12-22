pckage agent

import (
    "sync"
    "os"
    "net"

    "github.com/desperado-bvb/dortmund/agent"
)

type SERVER struct {
    clients       map[string]*Client

    sync.RWMutex
    opts          *options

    healthMtx     sync.RWMutex
    healthy       int32
    err           error

    tcpAddr       *net.TCPAddr
    httpAddr      *net.TCPAddr
    tcpListener   net.Listener
    httpListener  net.Listener

    exitChan      chan int
    waitGroup     wait.WaitGroupWrapper
}

func NewServer(opts *options) {
    s := &SERVER {
        opts     :   opts,
        healthy  :   1,
        exitChan : make(chan int),
    }

    if opts.ID < 0 || opts.ID > 4096 {
        s.logf("FATAL:--worker-id must be [0, 4096]")
        os.Exit(1)
    }

    tcpAddr, err := net.ResolveTCPAddr("tcp", opts.TCPAddress)
    if err != nil {
        s.logf("FATAL: fail to resolve TCP address(%s)-%s", opts.TCPAddress, err)
        os.Exit(1)
    }
    s.tcpAddr = tcpAddr

    httpAddr, err := net.ResolveTCPAddr("tcp", opts.HTTPAddress)
    if err != nil {
        s.logf("FATAL: fail to resolve TCP address(%s)-%s", opts.HTTPAddress, err)
        os.Exit(1)
    }
    s.httpAddr = httpAddr

    s.logf("ID:%d", s.opts.ID)

    return s
}

func (s *SERVER) logf(f string, args ...interface{}) {
    if s.opts.Logger == nil {
        return
    }

    s.opts.Logger.Output(2, fmt.Sprintf(f, args...))
}

func (s *SERVER) Main() {
    ctx := &context{s}

    tcpListener, err := net.Listen("tcp", s.tcpAddr.String())
    if err != nil {
        s.logf("FATAL: listen(%s) failed - %s", s.tcpAddr, err)
        os.Exit(1)
    }

    s.tcpListener = tcpListener
    tcpServer := &tcpServer{ctx: ctx}

    s.waitGroup.Wrap(func() {
        util.TCPServer(s.tcpListener, tcpServer, s.opts.Logger)
    })

    httpListener, err = net.Listen("tcp", s.httpAddr.String())
    if err != nil {
	s.logf("FATAL: listen (%s) failed - %s", s.httpAddr, err)
	os.Exit(1)
    }

    s.httpListener = httpListener
    httpServer := &httpServer{
        ctx:         ctx,
        tlsEnabled:  false,
	tlsRequired: false,
    }

    s.waitGroup.Wrap(func() {
        util.HTTPServer(s.httpListener, httpServer, s.opts.Logger, "HTTP")
    })

}



