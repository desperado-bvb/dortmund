package agent

import (
    "sync"
    "os"
    "fmt"
    "net"
    "errors"
    "sync/atomic"

    "github.com/desperado-bvb/dortmund/util/wait"
    "github.com/desperado-bvb/dortmund/util"
)

type SERVER struct {

    sync.RWMutex
    opts          *options

    healthMtx     sync.RWMutex
    healthy       int32
    err           error

    tcpAddr       *net.TCPAddr
    httpAddr     *net.TCPAddr
    mqttAddr    *net.TCPAddr 
    tcpListener   net.Listener
    httpListener net.Listener
    pubSvr          *PubSvr 
    subSvrs         map[string] *SubSvr 

    exitChan      chan int
    waitGroup     wait.WaitGroupWrapper
}

func NewServer(opts *options) *SERVER {
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

    mqttAddr, err := net.ResolveTCPAddr("tcp", opts.MQTTAddress)
    if err != nil {
        s.logf("FATAL: fail to resolve TCP address(%s)-%s", opts.MQTTAddress, err)
        os.Exit(1)
    }
    s.mqttAddr = mqttAddr

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

    pubSvr, err := newPubSvr(ctx)
    if err != nil {
         s.logf("FATAL: PubSvr(%s)  create pubSvr fail- %s", s.mqttAddr, err)
        os.Exit(1)
    }
    s.pubSvr = pubSvr

    err := s.pubSvr.start()
    if err != nil {
        s.logf("FATAL: PubSvr(%s) connection mqtt failed - %s", s.mqttAddr, err)
        os.Exit(1)
    }

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

    httpListener, err := net.Listen("tcp", s.httpAddr.String())
    if err != nil {
	s.logf("FATAL: listen (%s) failed - %s", s.httpAddr, err)
	os.Exit(1)
    }

    s.httpListener = httpListener
    httpServer := &httpServer{
        ctx:         ctx,
    }

    s.waitGroup.Wrap(func() {
        util.HTTPServer(s.httpListener, httpServer, s.opts.Logger, "HTTP")
    })
}

func (s *SERVER) Exit() {
    if s.tcpListener != nil {
        s.tcpListener.Close()
    }

    if s.httpListener != nil {
        s.httpListener.Close()
    }

    s.Lock()
    for ss := range s.subSvr {
        ss.close()
    }
    s.Unlock()

    s.pubSvr.close()
    s.logf("Exiting from server")
}

func (s *SERVER) SetHealth(err error) {
    s.healthMtx.Lock()
    defer s.healthMtx.Unlock()

    s.err = err
    if err != nil {
        atomic.StoreInt32(&s.healthy, 0)
    } else {
        atomic.StoreInt32(&s.healthy, 1)
    }
}

func (s *SERVER) IsHealthy() bool {
    return atomic.LoadInt32(&s.healthy) == 1
}

func (s *SERVER) GetError() error {
    s.healthMtx.RLock()
    defer s.healthMtx.RUnlock()
    return s.err
}

func (s *SERVER) GetHealth() string {
    if !s.IsHealthy() {
        return fmt.Sprintf("NOK - %s", s.GetError())
    }
    return "OK"
}

func (s *SERVER) createSub(topic string, callbackUrl string) error {
    ctx := &context{s}

    deleteCallback := func(sub *SubSvr) {
        s.DeleteExistingSub(sub.fd.ClientId)
    }
     sub, err:= newSubSvr(callbackUrl, topic, ctx,  deleteCallback)
     if err != nil {
        return err
     }

     s.Lock()
     item, ok := s.subSvrs[sub.fd.ClientId]
     if !ok {
        s.Unlock()
        sub.close()
        return errors.New("sub conflict")
     } 
     s.subSvrs[sub.fd.ClientId] = sub
     s.Unlock()
     return nil
}

func (s *SERVER) DeleteExistingSub(name string) error {
    s.RLock()
    sub, ok := s.subSvrs[name]
    if !ok {
        n.RUnlock()
        return errors.New("sub does not exist")
    }
    s.RUnlock()

    sub.Close()

    s.Lock()
    delete(s.subSvrs, name)
    s.Unlock()

    return nil
}