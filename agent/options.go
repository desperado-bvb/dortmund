package agent

import (
    "io"
    "crypto/md5"
    "hash/crc32"
    "log"
    "os"
)

type options struct {
    ID               int64    
    TCPAddress       string   `flag:"tcp-address" cfg:"tcp-address"`
    HTTPAddress      string   `flag:"http-address" cfg:"http-address"`
    MQTTAddress      string   `flag:"mqtt-address" cfg:"mqtt-address"`
    MaxMsgSize       int64    `flag:"max-msg-size"  cfg:"max-msg-size"`
    MqttUserName     string   `flag:"mqtt-username" cfg:"mqtt-username"`    
    MqttPassWord     string   `flag:"mqtt-password" cfg:"mqtt-password"`
    MetaUrl          string   `flag:"meta-url" cfg:"meta-url"`
    Logger           logger
}

func NewOptions() *options {

    hostname, err := os.Hostname()
    if err != nil {
       log.Fatal(err)
    }

    option := &options {
        TCPAddress:       "0.0.0.0:4150",
        HTTPAddress:      "0.0.0.0:4151",
        MQTTAddress:      "0.0.0.0:1883",
        MaxMsgSize:       1024768,
        MetaUrl:          "http://api.easylink.io/v1/agent/transtercodinginfo", 
        MqttPassWord:     "",
        MqttUserName:     "",
        Logger:           log.New(os.Stderr, "[MQTT] ", log.Ldate|log.Ltime|log.Lmicroseconds),
    }

    h := md5.New()
    io.WriteString(h, hostname)
    option.ID = int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

    return option
}
