package flume_log

import (
	"fmt"
	//"time"
	"testing"
)

type JsonTest struct {
	A string `json:"a"`
}

func (j *JsonTest) test(a int32) {}

func Test_Init_FLUME_SDK(t *testing.T) {
	l := InitSDK()
	//l := InitSDKByAddr("172.16.3.139:5140")
	defer StopSDK()
	//json_test := JsonTest{
	//	A: "1",
	//}

	var extra ExtraInfo
	extra = make(map[string]interface{})
	extra["a"] = 1
	extra["b"] = "c"
	detail := NewDetailInfo("test")
	detail.AddExtraInfo(extra)
	detail.AddLogItem("cccc", 2313)
	l.Info("1", "a bbb\nddd", detail)
	//Warn("2", "b", "d")
	//Error("3", "c", 3)
	//<- time.After(2 * time.Second)
	fmt.Println("aaa")
	fmt.Println("bbb")
}

func Test_Init_FLUME_SDK2(t *testing.T) {
	l := InitSDK()
	//l := InitSDKByAddr("172.16.3.139:5140")
	defer StopSDK()

	detail := NewDetailInfo("test")
	detail.AddLogItem("a", "cccc")
	detail.AddLogItem("b", 123)
	l.Info("1", "a bbb\nddd", detail)
	//Warn("2", "b", "d")
	//Error("3", "c", 3)
	//<- time.After(2 * time.Second)
}
