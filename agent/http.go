package agent

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	httpprof "net/http/pprof"
	"net/url"

	"github.com/desperado-bvb/dortmund/util"
)

type httpServer struct {
	ctx *context
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	err := s.v1Router(w, req)
	if err == nil {
		return
	}

	err = s.debugRouter(w, req)
	if err != nil {
		s.ctx.svr.logf("ERROR: %s", err)
		util.ApiResponse(w, 404, "NOT_FOUND", nil)
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
	case "/pub":
		util.NegotiateAPIResponseWrapper(w, req, util.POSTRequired(req,
			func() (interface{}, error) { return s.doHandle(req, "pub") }))
	case "/sub":
		util.NegotiateAPIResponseWrapper(w, req, util.POSTRequired(req,
			func() (interface{}, error) { return s.doHandle(req, "sub") }))
	case "/unsub":
		util.NegotiateAPIResponseWrapper(w, req, util.POSTRequired(req,
			func() (interface{}, error) { return s.doUNSUB(req) }))

    	case "/addTranstercoding":
                util.NegotiateAPIResponseWrapper(w, req, util.POSTRequired(req,
                        func() (interface{}, error) { return s.doHandle(req, "add") }))

       	case "/removeTranstercoding":
                util.NegotiateAPIResponseWrapper(w, req, util.POSTRequired(req,
                        func() (interface{}, error) { return s.doUNSUB(req) }))

	default:
		return errors.New(fmt.Sprintf("404 %s", req.URL.Path))
	}
	return nil
}

func (s *httpServer) doHandle(req *http.Request, operation string) (interface{}, error) {
	var res string

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

	fmt.Println(topic, string(body))

	switch operation {
	case "pub":
		r := s.ctx.svr.pubSvr.submitAsync(topic, body)
		err = r.wait()
		if err != nil {
			return nil, util.HTTPError{503, "EXITING"}
		}
		res = "OK"

	case "sub":
		id, err := s.ctx.svr.createSub(topic, string(body))
		if err != nil {
			s.ctx.svr.logf("ERROR: create sub - %s", err)
			return nil, util.HTTPError{503, "EXITING"}
		}
		res = id

	case "add":
		err = s.ctx.svr.createSub(topic, string(body))
        		if err != nil {
               		s.ctx.svr.logf("ERROR: create transterCoding - %s", err)
               		return nil, util.HTTPError{503, "EXITING"}
        		}
        		res = "OK"
	}

	return "OK", nil
}


func (s *httpServer) doUNSUB(req *http.Request) (interface{}, error) {

	name, err := s.getParamFromQuery(req)
	if err != nil {
		return nil, err
	}

	err = s.ctx.svr.DeleteExistingSub(string(name))
	if err != nil {
		s.ctx.svr.logf("ERROR: delete sub - %s", err)
		return nil, util.HTTPError{503, "EXITING"}
	}

	return "OK", nil
}

func (s *httpServer) getTopicFromQuery(req *http.Request) (string, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		s.ctx.svr.logf("ERROR: failed to parse request params - %s", err)
		return "", util.HTTPError{400, "INVALID_REQUEST"}
	}

	topicNames, ok := reqParams["name"]
	if !ok {
		return "", util.HTTPError{400, "MISSING_ARG_TOPIC"}
	}
	topicName := topicNames[0]

	return topicName, nil
}