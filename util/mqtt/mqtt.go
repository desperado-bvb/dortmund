package mqtt

import (
     "sync/atomic"

     "github.com/desperado-bvb/dortmund/util"
      proto "github.com/huin/mqtt"
)

type retainFlag bool
type dupFlag bool

type receipt chan struct{}

// Wait for the receipt to indicate that the job is done.
func (r receipt) wait() {
	// TODO: timeout
	<-r
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
	ClientId string          
	Dump     bool             
	Incoming chan *proto.Publish
               
	out      chan job
	conn     net.Conn
	done     chan struct{}
	connack  chan *proto.ConnAck
	suback   chan *proto.SubAck
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
			// ignore these
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
	// Close connection on exit in order to cause reader to exit.
	defer func() {
		// Signal to Disconnect() that the message is on its way, or
		// that the connection is closing one way or the other...
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

// Send the CONNECT message to the server. If the ClientId is not already
// set, use a default (a 63-bit decimal random number). The "clean session"
// bit is always set.
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

// ConnectionErrors is an array of errors corresponding to the
// Connect return codes specified in the specification.
var ConnectionErrors = [6]error{
	nil, // Connection Accepted (not an error)
	errors.New("Connection Refused: unacceptable protocol version"),
	errors.New("Connection Refused: identifier rejected"),
	errors.New("Connection Refused: server unavailable"),
	errors.New("Connection Refused: bad user name or password"),
	errors.New("Connection Refused: not authorized"),
}

// Sent a DISCONNECT message to the server. This function blocks until the
// disconnect message is actually sent, and the connection is closed.
func (c *ClientConn) Disconnect() {
	c.sync(&proto.Disconnect{})
	<-c.done
}

// Subscribe subscribes this connection to a list of topics. Messages
// will be delivered on the Incoming channel.
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

// Publish publishes the given message to the MQTT server.
// The QosLevel of the message must be QosAtLeastOnce for now.
func (c *ClientConn) Publish(m *proto.Publish) err {
	if atomic.LoadInt32(&c.exitFlage) == 1 {
		return errors.New("exiting")
	}
	if m.QosLevel != proto.QosAtMostOnce {
		panic("unsupported QoS level")
	}
	c.out <- job{m: m}
}

// sync sends a message and blocks until it was actually sent.
func (c *ClientConn) sync(m proto.Message) {
	if atomic.LoadInt32(&c.exitFlage) == 1 {
		return  errors.New("exiting")
	}
	j := job{m: m, r: make(receipt)}
	c.out <- j
	<-j.r
	return
}