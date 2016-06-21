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

	topic       = flag.String("topic", "", "NSQ topic")
	partition   = flag.Int("partition", -1, "NSQ topic partition")
	dataPath    = flag.String("data_path", "", "the data path of nsqd")
	view        = flag.String("view", "commitlog", "commitlog | topicdata ")
	searchMode  = flag.String("search_mode", "count", "the view start of mode. (count|id|virtual_offset)")
	viewStart   = flag.Int64("view_start", 0, "the start count of message.")
	viewStartID = flag.Int64("view_start_id", 0, "the start id of message.")
	viewOffset  = flag.Int64("view_offset", 0, "the virtual offset of the queue")
	viewCnt     = flag.Int("view_cnt", 1, "the total count need to be viewed. should less than 1,000,000")
)

func getBackendName(topicName string, part int) string {
	backendName := nsqd.GetTopicFullName(topicName, part)
	return backendName
}

func getSearchPositionFromCount(tpLogMgr *consistence.TopicCommitLogMgr, logIndex int64,
	lastOffset int64, lastLogData consistence.CommitLogData) (int64, int64) {
	searchOffset := int64(0)
	searchLogIndexStart := int64(0)
	searchLogIndexEnd := *viewStart/int64(consistence.LOGROTATE_NUM) + 1
	if searchLogIndexEnd > logIndex {
		searchLogIndexEnd = logIndex
	}
	// find the start index of log
	for searchLogIndexStart < searchLogIndexEnd {
		_, l, err := tpLogMgr.GetLastCommitLogDataOnSegment(searchLogIndexStart)
		if err != nil {
			log.Fatalf("read log data failed: %v", err)
		}
		if l.MsgCnt+int64(l.MsgNum-1) >= *viewStart {
			break
		}
		searchLogIndexStart++
	}
	searchCntStart := int64(0)
	roffset, _, err := tpLogMgr.GetLastCommitLogDataOnSegment(searchLogIndexStart)
	if err != nil {
		log.Fatalf("get last log data on segment:%v, failed:%v\n", searchLogIndexStart, err)
	}
	searchCntEnd := roffset / int64(consistence.GetLogDataSize())
	for {
		if searchCntStart >= searchCntEnd {
			break
		}
		searchCntPos := searchCntStart + (searchCntEnd-searchCntStart)/2
		searchOffset = searchCntPos * int64(consistence.GetLogDataSize())
		cur, _ := tpLogMgr.GetCommitLogFromOffsetV2(searchLogIndexStart, searchOffset)
		if cur.MsgCnt > *viewStart {
			searchCntEnd = searchCntPos - 1
		} else if cur.MsgCnt+int64(cur.MsgNum-1) < *viewStart {
			searchCntStart = searchCntPos + 1
		} else {
			break
		}
	}

	return searchLogIndexStart, searchOffset
}

func getSearchPositionFromID(tpLogMgr *consistence.TopicCommitLogMgr, logIndex int64,
	lastOffset int64, lastLogData consistence.CommitLogData) (int64, int64) {
	searchOffset := int64(0)
	searchLogIndexStart := int64(0)
	searchLogIndexEnd := *viewStartID/int64(consistence.LOGROTATE_NUM) + 1
	if searchLogIndexEnd > logIndex {
		searchLogIndexEnd = logIndex
	}
	// find the start index of log
	for searchLogIndexStart < searchLogIndexEnd {
		_, l, err := tpLogMgr.GetLastCommitLogDataOnSegment(searchLogIndexStart)
		if err != nil {
			log.Fatalf("read log data failed: %v", err)
		}
		if l.LastMsgLogID >= *viewStartID {
			break
		}
		searchLogIndexStart++
	}
	searchCntStart := int64(0)
	roffset, _, err := tpLogMgr.GetLastCommitLogDataOnSegment(searchLogIndexStart)
	if err != nil {
		log.Fatalf("get last log data on segment:%v, failed:%v\n", searchLogIndexStart, err)
	}
	searchCntEnd := roffset / int64(consistence.GetLogDataSize())
	for {
		if searchCntStart >= searchCntEnd {
			break
		}
		searchCntPos := searchCntStart + (searchCntEnd-searchCntStart)/2
		searchOffset = searchCntPos * int64(consistence.GetLogDataSize())
		cur, _ := tpLogMgr.GetCommitLogFromOffsetV2(searchLogIndexStart, searchOffset)
		if cur.LogID > *viewStartID {
			searchCntEnd = searchCntPos - 1
		} else if cur.LastMsgLogID < *viewStartID {
			searchCntStart = searchCntPos + 1
		} else {
			break
		}
	}

	return searchLogIndexStart, searchOffset
}

func getSearchPositionFromVirtualPos(tpLogMgr *consistence.TopicCommitLogMgr, logIndex int64,
	lastOffset int64, lastLogData consistence.CommitLogData) (int64, int64) {
	return 0, 0
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_data_tool v%s\n", version.Binary)
		return
	}

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
		searchLogIndexStart, searchOffset = getSearchPositionFromCount(tpLogMgr, logIndex, lastOffset, *lastLogData)
	} else if *searchMode == "id" {
		searchLogIndexStart, searchOffset = getSearchPositionFromID(tpLogMgr, logIndex, lastOffset, *lastLogData)
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
		backendWriter := nsqd.NewDiskQueueWriter(backendName, topicDataPath, 1024*1024*1024, 1, 1024*1024*100, 1)
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
