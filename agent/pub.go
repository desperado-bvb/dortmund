package agent

import (
    "net"
    "time"
    "errors"
    "sync/atomic"

    proto "github.com/huin/mqtt"
    "github.com/desperado-bvb/dortmund/util/mqtt"
    "github.com/desperado-bvb/dortmund/util/wait"
)

type receipt chan struct{}

func (r receipt) wait() error {
        ticker := time.NewTicker(2 * time.Second)

        select {
	case <- r:
            return nil
        case <-ticker.C:
            ticker.Stop()
            return errors.New("timeout")
        }
}

type job struct {
    body []byte
    topic string
    r        receipt
}

type PubSvr struct {
    exitFlag       int32
    addr           string
    jobs           chan *job
    fd             *mqtt.ClientConn

    ctx            *context
    waitGroup wait.WaitGroupWrapper
}

func newPubSvr(ctx *context) (*PubSvr , error) {
    p := &PubSvr {
    	jobs  :  make(chan *job, ctx.svr.opts.MaxPubQueueSize),
    	ctx    : ctx,
    } 

    conn, err := net.Dial("tcp", ctx.svr.mqttAddr.String())
    if err != nil {
        return p, err
    }

    handle := mqtt.NewClientConn(conn)
    handle.Dump = false

    p.fd = handle
    return p, nil
}

func (p *PubSvr) start() error {
    if err := p.fd.Connect(p.ctx.svr.opts.PubUsername, p.ctx.svr.opts.PubPassword); err != nil {
        p.ctx.svr.logf("PubSvr: connect to mqtt err - %s",  err)
        return err
    }
     
     p.ctx.svr.logf("PubSvr: Connected with client id(%s) ", p.fd.ClientId)
     p.waitGroup.Wrap(func() { p.pubLoop() })
     return nil
}

func (p *PubSvr) close() {
    atomic.StoreInt32(&p.exitFlag, 1)
    close(p.jobs)
    p.waitGroup.Wait()
    p.fd.Disconnect()
}

func (p *PubSvr) submit(topic string, body []byte)  error {
    if atomic.LoadInt32(&p.exitFlag) == 1 {
        return errors.New("exiting")
    }

    j := &job {
        topic : topic,
        body : body,
    }

    select {
     case p.jobs <- j:

     default:
         p.ctx.svr.logf("PubSvr: fail to publish  %s",  topic)

    }

    return nil
}

func (p *PubSvr) submitAsync(topic string, body []byte) (receipt, error) {
     if atomic.LoadInt32(&p.exitFlag) == 1 {
        return nil, errors.New("exiting")
    }

    j := &job {
        topic : topic,
        body : body,
        r        : make(receipt),
    }

    p.jobs <- j
    return j.r, nil
}

func (p *PubSvr) pubLoop()  {
    for j := range p.jobs {

        err := p.fd.Publish(&proto.Publish{
            Header:    proto.Header{Retain: false},
            TopicName: j.topic,
            Payload:   proto.BytesPayload(j.body),
        })
        if err != nil {
            p.close()
            return 
        }

        if j.r != nil {
                close(j.r)
        }
    }

    p.ctx.svr.logf("PubSvr: exiting from loop")
}
