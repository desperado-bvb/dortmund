package agent

import (
    "io"
    "crypto/md5"
    "hash/crc32"
    "log"
    "os"
)

type options struct {
    ID               int64    `flag:"worker-id" cfg:"id"`
    TCPAddress       string   `flag:"tcp-address"`
    HTTPAddress     string   `flag:"http-address"`
    MQTTAddress     string   `flag:"mqtt-address"`
    MaxMsgSize       int64    `flag:"max-msg-size" deprecated:"max-message-size" cfg:"max_msg_size"`
    MaxPubQueueSize int64   `flag:"max-pub-queue-size"`
    PubUsername   string    `flag:"pub-username"`    
    PubPassword    string    `flag:"pub-password"`
    MetaUrl              string    `flag:"meta-url"`
    Logger           logger
}

func NewOptions() *options {

    hostname, err := os.Hostname()
    if err != nil {
       log.Fatal(err)
    }

    option := &options {
        TCPAddress:             "0.0.0.0:4150",
        HTTPAddress:           "0.0.0.0:4151",
        MQTTAddress:          "0.0.0.0:1883",
        MetaUrl:                    "http://api.easylink.io/v1/agent/transtercodinginfo", 
        MaxMsgSize:             1024768,
        MaxPubQueueSize:  100,
        PubPassword:            "",
        PubUsername:           "",
        Logger:           log.New(os.Stderr, "[MQTT] ", log.Ldate|log.Ltime|log.Lmicroseconds),
    }

    h := md5.New()
    io.WriteString(h, hostname)
    option.ID = int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

    return option
}
