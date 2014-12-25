package agent

import (
    "net"

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

func (p *PubSvr) submitAsync(topic string, body []byte) error {
    return p.fd.SubmitAsync(topic, body)
}

func (p *PubSvr) submit(topic string, body []byte)  error {
    return p.fd.Submit(topic, body)
}
