package main

import (
	"encoding/binary"
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
	view               = flag.String("view", "commitlog", "commitlog | topicdata | delayedqueue")
	searchMode         = flag.String("search_mode", "count", "the view start of mode. (count|id|timestamp|virtual_offset)")
	viewStart          = flag.Int64("view_start", 0, "the start count of message.")
	viewStartID        = flag.Int64("view_start_id", 0, "the start id of message.")
	viewStartTimestamp = flag.Int64("view_start_timestamp", 0, "the start timestamp of message.")
	viewOffset         = flag.Int64("view_offset", 0, "the virtual offset of the queue")
	viewCnt            = flag.Int("view_cnt", 1, "the total count need to be viewed. should less than 1,000,000")
	viewCh             = flag.String("view_channel", "", "channel detail need to view")
	logLevel           = flag.Int("level", 3, "log level")
	//TODO: add ext ver for decode message
	isExt = flag.Bool("ext", false, "is there extension for message ")
)

func getBackendName(topicName string, part int) string {
	backendName := nsqd.GetTopicFullName(topicName, part)
	return backendName
}

func decodeMessage(b []byte) (*nsqd.Message, error) {
	var msg nsqd.Message

	if len(b) < 16+8+2 {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	msg.ID = nsqd.MessageID(binary.BigEndian.Uint64(b[10:18]))
	msg.TraceID = binary.BigEndian.Uint64(b[18:26])

	msg.Body = b[26:]
	return &msg, nil
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_data_tool v%s\n", version.Binary)
		return
	}

	nsqd.SetLogger(levellogger.NewSimpleLog())
	nsqd.NsqLogger().SetLevel(int32(*logLevel))
	consistence.SetCoordLogger(levellogger.NewSimpleLog(), int32(*logLevel))

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

	backendName := getBackendName(*topic, *partition)
	backendWriter, err := nsqd.NewDiskQueueWriterForRead(backendName, topicDataPath, 1024*1024*1024, 1, 1024*1024*100, 1)
	if err != nil {
		if *view != "commitlog" {
			log.Fatalf("init disk writer failed: %v", err)
			return
		}
	}
	if *view == "delayedqueue" {
		opts := &nsqd.Options{
			MaxBytesPerFile: 1024 * 1024 * 100,
		}
		nsqd.NsqLogger().Infof("checking delayed")
		delayQ, err := nsqd.NewDelayQueueForRead(*topic, *partition, topicDataPath, opts, nil, false)
		if err != nil {
			log.Fatalf("init delayed queue failed: %v", err)
		}
		recentKeys, cntList, chCntList := delayQ.GetOldestConsumedState([]string{*viewCh}, true)
		for _, k := range recentKeys {
			nsqd.NsqLogger().Infof("delayed recent: %v", k)
		}
		nsqd.NsqLogger().Infof("cnt list: %v", cntList)
		for k, v := range chCntList {
			nsqd.NsqLogger().Infof("channel %v cnt : %v", k, v)
		}
		rets := make([]nsqd.Message, *viewCnt)
		cnt, err := delayQ.PeekAll(rets)
		for _, m := range rets[:cnt] {
			nsqd.NsqLogger().Infof("peeked msg : %v", m)
		}
		return
	}
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
		backendReader := nsqd.NewDiskQueueSnapshot(backendName, topicDataPath, backendWriter.GetQueueReadEnd())
		backendReader.SetQueueStart(backendWriter.GetQueueReadStart())
		backendReader.SeekTo(nsqd.BackendOffset(queueOffset))
		cnt := *viewCnt
		for cnt > 0 {
			cnt--
			ret := backendReader.ReadOne()
			if ret.Err != nil {
				log.Fatalf("read data error: %v", ret)
				return
			}

			fmt.Printf("%v:%v:%v, string: %v\n", ret.Offset, ret.MovedSize, ret.Data, string(ret.Data))
			msg, err := nsqd.DecodeMessage(ret.Data, *isExt)
			if err != nil {
				log.Fatalf("decode data error: %v", err)
				continue
			}
			fmt.Printf("%v:%v:%v:%v, body: %v\n", msg.ID, msg.TraceID, msg.Timestamp, msg.Attempts, string(msg.Body))
		}
	}
}
