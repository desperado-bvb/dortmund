package agent

import (
    "errors"
    "fmt"
    "os"
    "io"
    "io/ioutil"
    "net"
    "net/http"
    httpprof "net/http/pprof"
    "net/url"

    "github.com/desperado-bvb/dortmund/util"
    
    proto "github.com/huin/mqtt"
    "github.com/jeffallen/mqtt"
)

type httpServer struct {
    ctx  *context
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {

    err := s.v1Router(w, req)
    if err == nil {
	return
    }

    err = s.debugRouter(w, req)
    if err != nil {
	s.ctx.svr.logf("ERROR: %s", err)
//	util.ApiResponse(w, 404, "NOT_FOUND", nil)
    }
}

func (s *httpServer) debugRouter(w http.ResponseWriter, req *http.Request) error {
    switch req.URL.Path {
    case "/debug/pprof":
	httpprof.Index(w, req)
    case "/debug/pprof/cmdline":
	httpprof.Cmdline(w, req)
    case "/debug/pprof/symbol":
	httpprof.Symbol(w, req)
    case "/debug/pprof/heap":
	httpprof.Handler("heap").ServeHTTP(w, req)
    case "/debug/pprof/goroutine":
	httpprof.Handler("goroutine").ServeHTTP(w, req)
    case "/debug/pprof/profile":
	httpprof.Profile(w, req)
    case "/debug/pprof/block":
	httpprof.Handler("block").ServeHTTP(w, req)
    case "/debug/pprof/threadcreate":
	httpprof.Handler("threadcreate").ServeHTTP(w, req)
    default:
	return errors.New(fmt.Sprintf("404 %s", req.URL.Path))
    }

    return nil
}

func (s *httpServer) v1Router(w http.ResponseWriter, req *http.Request) error {
    switch req.URL.Path {
    case "/publish":
	util.NegotiateAPIResponseWrapper(w, req, util.POSTRequired(req,
	    func() (interface{}, error) { return s.doPUB(req) }))


    default:
        return errors.New(fmt.Sprintf("404 %s", req.URL.Path))
    }
    return nil
}

func (s *httpServer) doPUB(req *http.Request) (interface{}, error) {

    if req.ContentLength > s.ctx.svr.opts.MaxMsgSize {
	return nil, util.HTTPError{413, "MSG_TOO_BIG"}
    }

    readMax := s.ctx.svr.opts.MaxMsgSize + 1
    body, err := ioutil.ReadAll(io.LimitReader(req.Body, readMax))
    if err != nil {
	return nil, util.HTTPError{500, "INTERNAL_ERROR"}
    }

    if int64(len(body)) == readMax {
	s.ctx.svr.logf("ERROR: /put hit max message size")
	return nil, util.HTTPError{413, "MSG_TOO_BIG"}
    }

    if len(body) == 0 {
	return nil, util.HTTPError{400, "MSG_EMPTY"}
    }

    topic, err := s.getTopicFromQuery(req)
    if err != nil {
	return nil, err
    }

    conn, err := net.Dial("tcp", "localhost:1883")
    if err != nil {
        fmt.Fprintf(os.Stderr, "dial: ", err)
    }

    cc := mqtt.NewClientConn(conn)
    cc.Dump = false

    if err := cc.Connect("", ""); err != nil {
        fmt.Fprintf(os.Stderr, "connect: %v\n", err)
        os.Exit(1)
    }

    fmt.Println("Connected with client id ", cc.ClientId)

    cc.Publish(&proto.Publish{
        Header:    proto.Header{Retain: false},
        TopicName: topic,
        Payload:   proto.BytesPayload(body),
    })

    cc.Disconnect()

    /*msg := NewMessage(<-s.ctx.nsqd.idChan, body)
    err = topic.PutMessage(msg)
    if err != nil {
	return nil, util.HTTPError{503, "EXITING"}
    }*/

    return "OK", nil
}

func (s *httpServer) getTopicFromQuery(req *http.Request) (string,  error) {
    reqParams, err := url.ParseQuery(req.URL.RawQuery)
    if err != nil {
	s.ctx.svr.logf("ERROR: failed to parse request params - %s", err)
	return "",  util.HTTPError{400, "INVALID_REQUEST"}
    }

    topicNames, ok := reqParams["topic"]
    if !ok {
	return "", util.HTTPError{400, "MISSING_ARG_TOPIC"}
    }
    topicName := topicNames[0]

    return topicName, nil
}
