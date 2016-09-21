##代码：
	import . "gitlab.qima-inc.com/shiwei/go_flume_sdk"
	func test() {
     	l := InitSDK(0)
     	//l := InitSDKByAddr("172.16.3.139:5140", 0)
    	defer Flush()
    	detail := NewDetailInfo()
    	detail.AddLogItem("a", "cccc")
    	detail.AddLogItem("b", 123)
    	l.Info("Info", "a bbb\nddd", detail)
	}

##demo
	<158>2016-08-26 12:25:35 shiweideMacBook-Pro-2.local/172.17.10.108 info[57399]: topic=log.tetherbin.main {"type":"Info","tag":"tether start..., version: Ver No Version Provided, build : 2016-08-26 04:25:35.514558343 +0000 UTC, gc go1.6 darwin amd64","platform":"go","level":"info","app":"TetherBin","module":"main","detail":{"extra":[{"aaa":123,"bbb":"ccc"}]}}

##topic生成规则：
	topic = log.tetherbin.main
	log: 固定名称
	tetherbin: app名字,自动获取,小写
	main: logIndex,自动获取,非main package为git中package所处路径的最后一个值, main package则为main，小写
***
	
	比如main package中logindex为main
	gitlab.qima-inc.com/shiwei/TetherSDK中则为tethersdk
