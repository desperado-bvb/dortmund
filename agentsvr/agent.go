package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

        "github.com/mreiferson/go-options"
	"github.com/desperado-bvb/dortmund/agent"
)

var (
	flagSet = flag.NewFlagSet("agent", flag.ExitOnError)

	httpAddress        = flagSet.String("http-address", "0.0.0.0:4151", "<addr>:<port> to listen on for HTTP clients")
	tcpAddress         = flagSet.String("tcp-address", "0.0.0.0:4150", "<addr>:<port> to listen on for TCP clients")
	mqttAddress      = flagSet.String("mqtt-address", "0.0.0.0:1883", "<addr>:<port> to create MQTT clients")
	maxMsgSize       = flagSet.Int64("max-msg-size", 1024768, "number of bytes per http msg body")
	mqttUserName = flagSet.String("mqtt-username", "", "username for mqtt server")
	mqttPassWord  = flagSet.String("mqtt-password", "", "password for mqtt server")
	metaUrl              = flagSet.String("meta-url", "http://api.easylink.io/v1/agent/transtercodinginfo", "product meta data url")
	
)

func main() {
	flagSet.Parse(os.Args[1:])
	var cfg map[string]interface{}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	opts := agent.NewOptions()
	options.Resolve(opts, flagSet, cfg)
	svr := agent.NewServer(opts)

	svr.Main()
	<-signalChan
	svr.Exit()
}
