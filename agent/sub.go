package agent

import (
    "sync"
    "fmt"
    "net"

    proto "github.com/huin/mqtt"
    "github.com/jeffallen/mqtt"
    "github.com/desperado-bvb/dortmund/util/wait"
)

type SubSvr struct {
    name                string
    callbackUrl       string
    topic                  string
    fd                       *mqtt.ClientConn
    ctx                     *context
    exitChan            chan int

    deleteCallback func(*SubSvr)
    tc                         bool
    deleter                sync.Once
    waitGroup          wait.WaitGroupWrapper
}

func newSubSvr(callbackUrl string, topic string, tc bool, ctx *context, deleteCallback func(*SubSvr) ) (*SubSvr, error) {
    s := &SubSvr {
    	callbackUrl        : callbackUrl,
    	topic                  : topic,
              tc                        : tc,
    	deleteCallback : deleteCallback,
    	ctx                     : ctx,
    	exitChan           : make(chan int),
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

    if tc {
        s.name = topic
        topicName := topic + "/#"
    } else {
        s.name = s.fd.ClientId
        topicName := topic 
    }
     
    tp := proto.TopicQos {
    	Topic : topicName
    	Qos    : proto.QosAtMostOnce,
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
                        return
        	}

              if tc {
                    t.ctx.svr.pubSvr.submit("test2", []byte("hahahha"))
              }

        case <- s.exitChan:
        	goto exit
        }
    }

exit:
    s.ctx.svr.logf("SunSvr(%s): exit from loop", s.fd.ClientId)
}
