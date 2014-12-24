package agent

import (
    "net"
    "errors"

    proto "github.com/huin/mqtt"
    "github.com/desperado-bvb/dortmund/util/mqtt"
)

type PubSvr struct {
    addr           string
    fd             *mqtt.ClientConn

    ctx            *context
}

func newPubSvr(ctx *context) *PubSvr {
    p := &PubSvr {
    	ctx    : ctx,
    } 

    return p
}

func (p *PubSvr) start() error {
    conn, err := net.Dial("tcp", p.ctx.svr.mqttAddr.String())
    if err != nil {
        return  err
    }

    handle := mqtt.NewClientConn(conn)
    handle.Dump = false
    p.fd = handle

    if err := p.fd.Connect(p.ctx.svr.opts.MqttUserName, p.ctx.svr.opts.MqttPassWord); err != nil {
        p.ctx.svr.logf("PubSvr: connect to mqtt err - %s",  err)
        return err
    }
     
    p.ctx.svr.logf("PubSvr: Connected with client id(%s) ", p.fd.ClientId)
    return nil
}

func (p *PubSvr) close() {
    p.fd.Disconnect()
}

func (p *PubSvr) submit(topic string, body []byte)  error {

    if p.fd.IsExit() {
        return errors.New("exiting")
    }
    
    j := mqtt.Job {
        M : &proto.Publish{
            Header:    proto.Header{Retain: false},
            TopicName: topic,
            Payload:   proto.BytesPayload(body),
        },
    }

    p.fd.Out <- j   

    return nil
}

func (p *PubSvr) submitAsync(topic string, body []byte) error {

    if p.fd.IsExit() {
        return errors.New("exiting")
    }

    j := mqtt.Job {
        M : &proto.Publish{
            Header:    proto.Header{Retain: false},
            TopicName: topic,
            Payload:   proto.BytesPayload(body),
        },
        R : make(mqtt.Receipt),
    }

    p.fd.Out <- j
    return j.R.Wait()
    
}

