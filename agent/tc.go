package agent

import (
    "sync"
    "fmt"
    "net"

    proto "github.com/huin/mqtt"
    "github.com/jeffallen/mqtt"
    "github.com/desperado-bvb/dortmund/util/wait"
)

type TransterCoding struct {
    url            string
    name           string
    fd             *mqtt.ClientConn
    ctx            *context
    exitChan       chan int

    deleteCallback func(*TransterCoding)
    deleter        sync.Once
    waitGroup      wait.WaitGroupWrapper
}

func newTransterCoding(name string, url string, ctx *context, deleteCallback func(*TransterCoding) ) (*TransterCoding, error) {
    t := &TransterCoding {
        name:           name,  
    	url :           url,
    	deleteCallback: deleteCallback,
    	ctx :           ctx,
    	exitChan:       make(chan int),
    }

    conn, err := net.Dial("tcp",  ctx.svr.mqttAddr.String())
    if err != nil {
        t.ctx.svr.logf("TransterCoding: create connect to mqtt err - %s",  err)
        return nil, err
    }

    handle := mqtt.NewClientConn(conn)
    handle.Dump = false

    t.fd = handle

    if err := t.fd.Connect(t.ctx.svr.opts.PubUsername, t.ctx.svr.opts.PubPassword); err != nil {
        t.ctx.svr.logf("TransterCoding(%s): connect to mqtt err - %s",  t.name, err)
        return nil, err
    }
     
    tp := proto.TopicQos {
    	Topic : name + "/#",
    	Qos   : proto.QosAtMostOnce,
    }

    t.fd.Subscribe([]proto.TopicQos{tp})

    t.ctx.svr.logf("TransterCoding: Connected with client id(%s) ", t.name)
    t.waitGroup.Wrap(func() { t.tcLoop() })
    return t, nil
}

func (t *TransterCoding) Close() {
    close(t.exitChan)
    t.waitGroup.Wait()
    t.fd.Disconnect()
}

func (t *TransterCoding) tcLoop() {
    for {
        select {
        case  m := <- t.fd.Incoming :
        	fmt.Println("tc", m)
        	if m == nil {
        		go t.deleter.Do(func() { t.deleteCallback(t) })
                        return
        	}

               t.ctx.svr.pubSvr.submit("test2", []byte("hahahha"))

        case <- t.exitChan:
        	goto exit
        }
    }

exit:
    t.ctx.svr.logf("TransterCoding(%s): exit from loop", t.name)
}
