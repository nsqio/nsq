package flume_log

import (
	"errors"
	"strings"
)

var defaultAgentAddr string

type FlumeLogger struct {
	client *FlumeClient
}

func init() {
	defaultAgentAddr = "127.0.0.1:5140"
}

func NewFlumeLogger() *FlumeLogger {
	c := NewFlumeClient(defaultAgentAddr)
	return &FlumeLogger{c}
}

func NewFlumeLoggerWithAddr(agentAddr string) *FlumeLogger {
	c := NewFlumeClient(agentAddr)
	return &FlumeLogger{c}
}

func (l *FlumeLogger) Info(logInfo string, detail *DetailInfo) error {
	return l.log("info", logInfo, detail)
}

func (l *FlumeLogger) Warn(logInfo string, detail *DetailInfo) error {
	return l.log("warn", logInfo, detail)
}

func (l *FlumeLogger) Error(logInfo string, detail *DetailInfo) error {
	return l.log("error", logInfo, detail)
}

func (l *FlumeLogger) log(typeInfo string, logInfo string, detail *DetailInfo) error {
	log_info := NewLogInfo()
	log_info.module = detail.module
	topic := "log." + gAppName + "." + detail.module
	log_info.topic = strings.ToLower(topic)
	log_info.level = typeInfo
	log_info.typ = typeInfo
	log_info.tag = logInfo
	log_info.detail = detail
	if l.client != nil {
		return l.client.SendLog(log_info.Serialize())
	}
	return errors.New("no client")
}

func (l *FlumeLogger) Stop() {
	if l.client != nil {
		l.client.Stop()
	}
}
