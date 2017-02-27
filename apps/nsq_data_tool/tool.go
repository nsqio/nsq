package main

import (
	"flag"
	"fmt"
	"log"
	"path"

	"github.com/absolute8511/nsq/consistence"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/version"
	"github.com/absolute8511/nsq/nsqd"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic              = flag.String("topic", "", "NSQ topic")
	partition          = flag.Int("partition", -1, "NSQ topic partition")
	dataPath           = flag.String("data_path", "", "the data path of nsqd")
	view               = flag.String("view", "commitlog", "commitlog | topicdata ")
	searchMode         = flag.String("search_mode", "count", "the view start of mode. (count|id|timestamp|virtual_offset)")
	viewStart          = flag.Int64("view_start", 0, "the start count of message.")
	viewStartID        = flag.Int64("view_start_id", 0, "the start id of message.")
	viewStartTimestamp = flag.Int64("view_start_timestamp", 0, "the start timestamp of message.")
	viewOffset         = flag.Int64("view_offset", 0, "the virtual offset of the queue")
	viewCnt            = flag.Int("view_cnt", 1, "the total count need to be viewed. should less than 1,000,000")
)

func getBackendName(topicName string, part int) string {
	backendName := nsqd.GetTopicFullName(topicName, part)
	return backendName
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_data_tool v%s\n", version.Binary)
		return
	}

	nsqd.SetLogger(levellogger.NewSimpleLog())
	consistence.SetCoordLogger(levellogger.NewSimpleLog(), levellogger.LOG_INFO)

	if *topic == "" {
		log.Fatal("--topic is required\n")
	}
	if *partition == -1 {
		log.Fatal("--partition is required")
	}
	if *dataPath == "" {
		log.Fatal("--data_path is required")
	}

	if *viewCnt > 1000000 {
		log.Fatal("--view_cnt is too large")
	}

	topicDataPath := path.Join(*dataPath, *topic)
	topicCommitLogPath := consistence.GetTopicPartitionBasePath(*dataPath, *topic, *partition)
	tpLogMgr, err := consistence.InitTopicCommitLogMgr(*topic, *partition, topicCommitLogPath, 0)
	if err != nil {
		log.Fatalf("loading commit log %v failed: %v\n", topicCommitLogPath, err)
	}
	logIndex, lastOffset, lastLogData, err := tpLogMgr.GetLastCommitLogOffsetV2()
	if err != nil {
		log.Fatalf("loading last commit log failed: %v\n", err)
	}
	log.Printf("topic last commit log at %v:%v is : %v\n", logIndex, lastOffset, lastLogData)

	// note: since there may be exist group commit. It is not simple to do the direct position of log.
	// we need to search in the ordered log data.
	searchOffset := int64(0)
	searchLogIndexStart := int64(0)
	if *searchMode == "count" {
		searchLogIndexStart, searchOffset, _, err = tpLogMgr.SearchLogDataByMsgCnt(*viewStart)
		if err != nil {
			log.Fatalln(err)
		}
	} else if *searchMode == "id" {
		searchLogIndexStart, searchOffset, _, err = tpLogMgr.SearchLogDataByMsgID(*viewStartID)
		if err != nil {
			log.Fatalln(err)
		}
	} else if *searchMode == "virtual_offset" {
		searchLogIndexStart, searchOffset, _, err = tpLogMgr.SearchLogDataByMsgOffset(*viewStartID)
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		log.Fatalln("not supported search mode")
	}

	logData, err := tpLogMgr.GetCommitLogFromOffsetV2(searchLogIndexStart, searchOffset)
	if err != nil {
		log.Fatalf("topic read at: %v:%v failed: %v\n", searchLogIndexStart, searchOffset, err)
	}
	log.Printf("topic read at: %v:%v, %v\n", searchLogIndexStart, searchOffset, logData)

	if *view == "commitlog" {
		logs, err := tpLogMgr.GetCommitLogsV2(searchLogIndexStart, searchOffset, int(*viewCnt))
		if err != nil {
			if err != consistence.ErrCommitLogEOF {
				log.Fatalf("get logs failed: %v", err)
				return
			}
		}
		for _, l := range logs {
			fmt.Println(l)
		}
	} else if *view == "topicdata" {
		queueOffset := logData.MsgOffset
		if *searchMode == "virtual_offset" {
			if queueOffset != *viewOffset {
				queueOffset = *viewOffset
				log.Printf("search virtual offset not the same : %v, %v\n", logData.MsgOffset, *viewOffset)
			}
		}
		backendName := getBackendName(*topic, *partition)
		backendWriter, err := nsqd.NewDiskQueueWriter(backendName, topicDataPath, 1024*1024*1024, 1, 1024*1024*100, 1)
		if err != nil {
			log.Fatal("init disk writer failed: %v", err)
			return
		}
		backendReader := nsqd.NewDiskQueueSnapshot(backendName, topicDataPath, backendWriter.GetQueueReadEnd())
		backendReader.SeekTo(nsqd.BackendOffset(queueOffset))
		cnt := *viewCnt
		for cnt > 0 {
			cnt--
			ret := backendReader.ReadOne()
			if ret.Err != nil {
				log.Fatalf("read data error: %v", ret)
				return
			}
			fmt.Printf("%v:%v:%v\n", ret.Offset, ret.MovedSize, ret.Data)
		}
	}
}
