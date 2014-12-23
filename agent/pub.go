package agent

import (
    "net"
    "time"
    "errors"

    proto "github.com/huin/mqtt"
    "github.com/jeffallen/mqtt"
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
    addr           string
    jobs           chan *job
    fd             *mqtt.ClientConn

    ctx            *context
    waitGroup wait.WaitGroupWrapper
}

func newPubSvr(addr string, ctx *context) *PubSvr {
    p := &PubSvr {
    	addr : addr ,
    	jobs  :  make(chan *job, ctx.svr.opts.MaxPubQueueSize),
    	ctx    : ctx,
    } 

    conn, err := net.Dial("tcp", addr)
    if err != nil {
        p.ctx.svr.logf("PunSvr: dial error - %s ", err)
    }

    handle := mqtt.NewClientConn(conn)
    handle.Dump = false

    p.fd = handle
    return p
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
    close(p.jobs)
    p.fd.Disconnect()
    p.waitGroup.Wait()
}

func (p *PubSvr) submit(topic string, body []byte)  {
    j := &job {
        topic : topic,
        body : body,
    }

    select {
     case p.jobs <- j:

     default:
         p.ctx.svr.logf("PubSvr: fail to publish  %s",  topic)

    }
}

func (p *PubSvr) submitAsync(topic string, body []byte) receipt {
    j := &job {
        topic : topic,
        body : body,
        r        : make(receipt),
    }

    p.jobs <- j
    return j.r
}

func (p *PubSvr) pubLoop()  {
    for j := range p.jobs {

        if j.r != nil {
        	close(j.r)
        }

        p.fd.Publish(&proto.Publish{
            Header:    proto.Header{Retain: false},
            TopicName: j.topic,
            Payload:   proto.BytesPayload(j.body),
        })
    }

    p.ctx.svr.logf("PubSvr: exiting from loop")
}
