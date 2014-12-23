package agent

import (
    "sync"
    "errors"
    "fmt"

    proto "github.com/huin/mqtt"
    "github.com/jeffallen/mqtt"
    "github.com/desperado-bvb/dortmund/util/wait"
)

type SubSvr struct {
    callbackUrl       string
    topic                  string
    fd                       *mqtt.ClientConn
    ctx                     *context
    exitChan            chan int

    deleteCallback func(*SubSvr)
    deleter              sync.Once
}

func (s *SubSvr) newSubSvr(callbackUrl string, topic string, ctx *context, deleteCallback func(*Subsvr) ) (*SubSvr, err) {
    s := &SubSvr {
    	callbackUrl        : callbackUrl,
    	topic                  : topic,
    	deleteCallback  : deleteCallback,
    	ctx                      : ctx,
    	exitChan            : make(chan int),
    }

    conn, err := net.Dial("tcp",  ctx.svr.mqttAddr.String())
    if err != nil {
        s.ctx.svr.logf("SubSvr: create connect to mqtt err - %s",  err)
        return s, err
    }

    handle := mqtt.NewClientConn(conn)
    handle.Dump = false

    s.fd = handle

    if err := s.fd.Connect(s.ctx.svr.opts.PubUsername, s.ctx.svr.opts.PubPassword); err != nil {
        s.ctx.svr.logf("SubSvr(%s): connect to mqtt err - %s",  s.fd.ClientId, err)
        return s, err
    }
     
    tp := proto.TopicQos {
    	Topic : topic,
    	Qos    : proto.QosAtMostOnce
    }

    s.fd.Subscribe([]proto.TopicQos{tp})

     s.ctx.svr.logf("SubSvr: Connected with client id(%s) ", s.fd.ClientId)
     s.waitGroup.Wrap(func() { s.subLoop() })
     return s, nil
}

func (s *SubSvr) Close() {
    close(s.exitChan)
    s.waitGroup.Wait()
    s.fd.Disconnect()
}

func (s *SubSvr) subLoop() {
    for {
        select {
        case  m := <- s.fd.Incoming :
        	fmt.Println(m)
        	if m == nil {
        		go s.deleter.Do(func() { s.deleteCallback(s) })
        	}

        case <- exitChan:
        	goto exit
        }
    }

exit:
    s.ctx.svr.logf("SunSvr(%s): exit from loop", s.fd.ClientId)
}