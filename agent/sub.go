package agent

import (
    "sync"
    "net"
    "net/url"
    "net/http"
    "io/ioutil"
    "encoding/json"

    proto "github.com/huin/mqtt"
    "github.com/desperado-bvb/dortmund/util/mqtt"
    "github.com/desperado-bvb/dortmund/util/wait"
)

type SubSvr struct {
    name           string
    callbackUrl    string
    topic          string
    fd             *mqtt.ClientConn
    ctx            *context
    exitChan       chan int

    deleteCallback func(*SubSvr)
    tc             bool
    deleter        sync.Once
    waitGroup      wait.WaitGroupWrapper
}

func newSubSvr(callbackUrl string, topic string, tc bool, ctx *context, deleteCallback func(*SubSvr) ) *SubSvr {
    s := &SubSvr {
    	callbackUrl:     callbackUrl,
    	topic :          topic,
        tc :             tc,
    	deleteCallback : deleteCallback,
    	ctx :            ctx,
    	exitChan :       make(chan int),
    }

    return s
}

func (s *SubSvr) start() error {
    var topicName string

    conn, err := net.Dial("tcp",  s.ctx.svr.mqttAddr.String())
    if err != nil {
        s.ctx.svr.logf("SubSvr: create connect to mqtt err - %s",  err)
        return  err
    }

    handle := mqtt.NewClientConn(conn)
    handle.Dump = false

    s.fd = handle

    if err := s.fd.Connect(s.ctx.svr.opts.MqttUserName, s.ctx.svr.opts.MqttPassWord); err != nil {
        s.ctx.svr.logf("SubSvr(%s): connect to mqtt err - %s",  s.fd.ClientId, err)
        return err
    }

    if s.tc {
        s.name = s.topic
        topicName = s.topic + "/#"
    } else {
        s.name = s.fd.ClientId
        topicName = s.topic 
    }
     
    tp := proto.TopicQos {
    	Topic : topicName,
    	Qos    : proto.QosAtMostOnce,
    }

    _, err = s.fd.Subscribe([]proto.TopicQos{tp})
    if err != nil {
        return err
    }

     s.ctx.svr.logf("SubSvr: Connected with client id(%s) ", s.name)
     s.waitGroup.Wrap(func() { s.subLoop() })
     return nil
}

func (s *SubSvr) Close() {
    close(s.exitChan)
    s.waitGroup.Wait()
}

func (s *SubSvr) subLoop() {
    for {
        select {
        case  m := <- s.fd.Incoming :
            if m == nil {
                go s.deleter.Do(func() { s.deleteCallback(s) })
                return
            }

            resp, err := http.PostForm(s.callbackUrl,
                url.Values{"topic": {m.TopicName}, "body": {string(m.Payload.(proto.BytesPayload))}})
            if err != nil {
               s.ctx.svr.logf("SubSvr(%s): call callbackUrl err - %s ", s.name, err)
               continue
            }

            res, err := ioutil.ReadAll(resp.Body)

            if s.tc {
                content := make(map[string] string)

                err = json.Unmarshal(res, &content)
                if err != nil {
                    s.ctx.svr.logf("SubSvr(%s): json topic(%s) err - %s ", s.name,m.TopicName, err)
                } else {
                    s.fd.Submit(content["topic"], []byte(content["body"]))
                }
            } 
               
            resp.Body.Close()

        case <- s.exitChan:
        	goto exit
        }
    }

exit:
    s.fd.Disconnect()
    s.ctx.svr.logf("SunSvr(%s): exit from loop", s.fd.ClientId)
}
