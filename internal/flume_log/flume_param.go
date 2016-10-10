package flume_log

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
	//ffjson "github.com/pquerna/ffjson/ffjson"
	"bytes"
	"encoding/json"
	"reflect"
)

var gAppName string
var gFlumeID int
var gHost string
var gIp string
var gPid int

//var gTopic string
var gPlatform string

func init() {
	gFlumeID = 158
	gHost = getHostName()
	gIp = getLocalIP(gHost)
	gPid = getPID()
	gPlatform = "go"
	gAppName = "nsq"
}

func getPID() int {
	return os.Getpid()
}

func getHostName() string {
	if hostName, err := os.Hostname(); err != nil {
		fmt.Println("flume_param.go:getHostName failed.")
		return ""
	} else {
		return hostName
	}
}

func getLocalIP(host string) string {
	if ip_addr, err := net.ResolveIPAddr("ip", host); err != nil {
		fmt.Println("flume_param.go:getLocalIP failed.")
		return ""
	} else {
		return ip_addr.String()
	}
}

type LogInfo struct {
	id       int
	date     string
	host     string
	ip       string
	level    string
	pid      int
	topic    string
	typ      string
	tag      string
	platform string
	app      string
	module   string
	detail   *DetailInfo
}

type LogInfoJson struct {
	Type     string      `json:"type"`
	Tag      string      `json:"tag"`
	PlatForm string      `json:"platform"`
	Level    string      `json:"level"`
	App      string      `json:"app"`
	Module   string      `json:"module"`
	Detail   interface{} `json:"detail"`
}

func NewLogInfo() *LogInfo {
	log_info := &LogInfo{
		app:      gAppName,
		id:       gFlumeID,
		date:     time.Now().Format("2006-01-02 15:04:05"),
		host:     gHost,
		ip:       gIp,
		pid:      gPid,
		platform: gPlatform,
	}
	return log_info
}

func (l *LogInfo) String() string {
	t := reflect.TypeOf(l.detail)
	//v := reflect.ValueOf(l.detail)

	logString := "id: " + strconv.FormatInt(int64(l.id), 10) + ", date: " + l.date + ", host: " + l.host + ", ip: " + l.ip +
		", level: " + l.level + ", pid: " + strconv.FormatInt(int64(l.pid), 10) + ", topic:" + l.topic + ", platform: " + l.platform +
		", app: " + l.app + ", module: " + l.module + ", tag: " + l.tag + ", type: " + l.typ + ", detail type: " + t.String()
	//fmt.Println(t.String(), v.String())
	//fmt.Println(l)
	return logString
}

func (l *LogInfo) Serialize() []byte {
	logInfoJson := &LogInfoJson{
		Type:     l.typ,
		Tag:      l.tag,
		PlatForm: l.platform,
		Level:    l.level,
		App:      l.app,
		Module:   l.module,
	}
	if l.detail != nil {
		logInfoJson.Detail = l.detail.detail
	} else {
		logInfoJson.Detail = nil
	}

	data, _ := json.Marshal(logInfoJson)
	var tmpBuf bytes.Buffer
	tmpBuf.WriteString("<")
	tmpBuf.WriteString(strconv.FormatInt(int64(l.id), 10))
	tmpBuf.WriteString(">")
	tmpBuf.WriteString(l.date)
	tmpBuf.WriteString(" ")
	tmpBuf.WriteString(l.host)
	tmpBuf.WriteString("/")
	tmpBuf.WriteString(l.ip)
	tmpBuf.WriteString(" ")
	tmpBuf.WriteString(l.level)
	tmpBuf.WriteString("[")
	tmpBuf.WriteString(strconv.FormatInt(int64(l.pid), 10))
	tmpBuf.WriteString("]: topic=")
	tmpBuf.WriteString(l.topic)
	tmpBuf.WriteString(" ")
	tmpBuf.Write(data)
	tmpBuf.WriteString("\n")
	return tmpBuf.Bytes()
}
