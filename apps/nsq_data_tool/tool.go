package main

import (
	"flag"
	"fmt"
	"log"
	"path"

	"github.com/absolute8511/nsq/consistence"
	"github.com/absolute8511/nsq/internal/version"
	"github.com/absolute8511/nsq/nsqd"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic     = flag.String("topic", "", "NSQ topic")
	partition = flag.Int("partition", -1, "NSQ topic partition")
	dataPath  = flag.String("data_path", "", "the data path of nsqd")
	view      = flag.String("view", "commitlog", "commitlog | topicdata ")
	viewStart = flag.Int64("view_start", 0, "the start id of message.")
	viewCnt   = flag.Int("view_cnt", 1, "the total count need to be viewed. should less than 1,000,000")
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

	if *topic == "" {
		log.Fatal("--topic is required")
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
		return
	}
	lastOffset, err := tpLogMgr.GetLastLogOffset()
	if err != nil {
		log.Fatalf("loading last commit log failed: %v\n", err)
		return
	}
	lastLogData, err := tpLogMgr.GetCommitLogFromOffset(lastOffset)
	if err != nil {
		log.Fatalf("loading last commit log failed: %v\n", err)
		return
	}
	log.Printf("topic last commit log is : %v\n", lastLogData)
	searchOffset := lastOffset
	searchCntEnd := lastOffset / int64(consistence.GetLogDataSize())
	searchCntStart := int64(0)
	for {
		if searchCntStart >= searchCntEnd-1 {
			break
		}
		searchCntPos := searchCntStart + (searchCntEnd-searchCntStart)/2
		searchOffset = searchCntPos * int64(consistence.GetLogDataSize())
		cur, _ := tpLogMgr.GetCommitLogFromOffset(searchOffset)
		if cur.MsgCnt > *viewStart {
			searchCntEnd = searchCntPos
		} else if cur.MsgCnt < *viewStart {
			searchCntStart = searchCntPos
		} else {
			break
		}
	}

	logData, _ := tpLogMgr.GetCommitLogFromOffset(searchOffset)
	log.Printf("topic read at: %v, %v\n", searchOffset, logData)
	if *view == "commitlog" {
		logs, err := tpLogMgr.GetCommitLogs(searchOffset, int(*viewCnt))
		if err != nil {
			log.Fatalf("get logs failed: %v", err)
			return
		}
		for index, l := range logs {
			fmt.Print(l)
			fmt.Print(" || ")
			if index%10 == 0 {
				fmt.Print("\n")
			}
		}
	} else if *view == "topicdata" {
		backendName := getBackendName(*topic, *partition)
		backendWriter := nsqd.NewDiskQueueWriter(backendName, topicDataPath, 1024*1024*1024, 1, 1024*1024*100, 1)
		backendReader := nsqd.NewDiskQueueSnapshot(backendName, topicDataPath, backendWriter.GetQueueReadEnd())
		backendReader.SeekTo(nsqd.BackendOffset(searchOffset))
		cnt := *viewCnt
		for cnt > 0 {
			cnt--
			ret := backendReader.ReadOne()
			if ret.Err != nil {
				log.Fatalf("read data error: %v", ret)
				return
			}
			fmt.Printf("%v:%v:%v\n", ret.Offset, ret.MovedSize, string(ret.Data))
		}
	}
}
