package mqtt

import (
     "sync/atomic"
     "net"
     crand "crypto/rand"
     "math/rand"
     "io"
     "strings"
     "log"
     "errors"
     "fmt"
     "time"

      proto "github.com/huin/mqtt"
)

var cliRand *rand.Rand

func init() {
	var seed int64
	var sb [4]byte
	crand.Read(sb[:])
	seed = int64(time.Now().Nanosecond())<<32 |
		int64(sb[0])<<24 | int64(sb[1])<<16 |
		int64(sb[2])<<8 | int64(sb[3])
	cliRand = rand.New(rand.NewSource(seed))
}

type retainFlag bool
type dupFlag bool

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
	m proto.Message
	r receipt
}

const (
	retainFalse retainFlag = false
	retainTrue             = true
	dupFalse    dupFlag    = false
	dupTrue                = true
)

const clientQueueLength = 100

type ClientConn struct {
	ClientId    string          
	Dump        bool             
	Incoming    chan *proto.Publish
               
	out             chan job
	conn          net.Conn
	done          chan struct{}
	connack    chan *proto.ConnAck
	suback      chan *proto.SubAck
	exitFlage   int32
}

func header(d dupFlag, q proto.QosLevel, r retainFlag) proto.Header {
	return proto.Header{
		DupFlag: bool(d), QosLevel: q, Retain: bool(r),
	}
}

func NewClientConn(c net.Conn) *ClientConn {
	cc := &ClientConn{
		conn:     c,
		out:      make(chan job, clientQueueLength),
		Incoming: make(chan *proto.Publish, clientQueueLength),
		done:     make(chan struct{}),
		connack:  make(chan *proto.ConnAck),
		suback:   make(chan *proto.SubAck),
	}
	go cc.reader()
	go cc.writer()
	return cc
}

func (c *ClientConn) reader() {
	defer func() {
		atomic.StoreInt32(&c.exitFlage, 1)
		close(c.out)
		close(c.Incoming)
		c.conn.Close()
	}()

	for {
		// TODO: timeout (first message and/or keepalives)
		m, err := proto.DecodeOneMessage(c.conn, nil)
		if err != nil {
			if err == io.EOF {
				return
			}
			if strings.HasSuffix(err.Error(), "use of closed network connection") {
				return
			}
			log.Print("cli reader: ", err)
			return
		}

		if c.Dump {
			log.Printf("dump  in: %T", m)
		}

		switch m := m.(type) {
		case *proto.Publish:
			c.Incoming <- m
		case *proto.PubAck:
			continue
		case *PingResp:
			continue
		case *proto.ConnAck:
			c.connack <- m
		case *proto.SubAck:
			c.suback <- m
		case *proto.Disconnect:
			return
		default:
			log.Printf("cli reader: got msg type %T", m)
		}
	}
}

func (c *ClientConn) writer() {
	defer func() {
		close(c.done)
	}()

	for job := range c.out {
		if c.Dump {
			log.Printf("dump out: %T", job.m)
		}

		// TODO: write timeout
		err := job.m.Encode(c.conn)
		if job.r != nil {
			close(job.r)
		}

		if err != nil {
			log.Print("cli writer: ", err)
			return
		}

		if _, ok := job.m.(*proto.Disconnect); ok {
			return
		}
	}
}

func (c *ClientConn) keppalive() {
	ticker := time.NewTicker(600 * time.Second)
	for {
		select {
		case <- ticker.C:
			m   := &proto.PingReq{}
			err  := m.Encode(c.conn)
			if err != nil {

			}
		case <- done:
			ticker.Stop()
			return
		}
	}

}

func (c *ClientConn) Connect(user, pass string) error {
	// TODO: Keepalive timer
	if c.ClientId == "" {
		c.ClientId = fmt.Sprint(cliRand.Int63())
	}
	req := &proto.Connect{
		ProtocolName:    "MQIsdp",
		ProtocolVersion: 3,
		ClientId:        c.ClientId,
		CleanSession:    true,
		KeepAliveTimer: 600,
	}
	if user != "" {
		req.UsernameFlag = true
		req.PasswordFlag = true
		req.Username = user
		req.Password = pass
	}

	c.sync(req)
	ack := <-c.connack
	return ConnectionErrors[ack.ReturnCode]
}

var ConnectionErrors = [6]error{
	nil, 
	errors.New("Connection Refused: unacceptable protocol version"),
	errors.New("Connection Refused: identifier rejected"),
	errors.New("Connection Refused: server unavailable"),
	errors.New("Connection Refused: bad user name or password"),
	errors.New("Connection Refused: not authorized"),
}

func (c *ClientConn) Disconnect() {
        if atomic.LoadInt32(&c.exitFlage) == 1 {
                return
        }
	c.sync(&proto.Disconnect{})
	<-c.done
}

func (c *ClientConn) Subscribe(tqs []proto.TopicQos) (*proto.SubAck, error) {
	if atomic.LoadInt32(&c.exitFlage) == 1 {
		return nil, errors.New("exiting")
	}

	c.sync(&proto.Subscribe{
		Header:    header(dupFalse, proto.QosAtLeastOnce, retainFalse),
		MessageId: 0,
		Topics:    tqs,
	})
	ack := <-c.suback
	return ack, nil
}

func (c *ClientConn) IsExit() bool {
    return atomic.LoadInt32(&c.exitFlage) == 1
}


func (c *ClientConn) sync(m proto.Message) {
	j := job{m: m, r: make(receipt)}
	c.out <- j
	<-j.r
	return
}

func (c *ClientConn) Submit(topic string, body []byte)  error {

    if atomic.LoadInt32(&c.exitFlage) == 1 {
        return errors.New("mqtt client exiting")
    }
    
    j := job {
        m : &proto.Publish{
            Header:    proto.Header{Retain: false},
            TopicName: topic,
            Payload:   proto.BytesPayload(body),
        },
    }

   c.out <- j   

    return nil
}

func (c *ClientConn) SubmitAsync(topic string, body []byte) error {

    if atomic.LoadInt32(&c.exitFlage) == 1 {
        return errors.New("mqtt client exiting")
    }

    j := job {
        m : &proto.Publish{
            Header:    proto.Header{Retain: false},
            TopicName: topic,
            Payload:   proto.BytesPayload(body),
        },
        r : make(receipt),
    }

    c.out <- j
    return j.r.wait()
    
}
