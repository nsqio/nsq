package main

import (
	"../nsq"
	"../util"
	"log"
)

func main() {
	consumer := nsq.Consumer{}
	err := consumer.Connect("127.0.0.1", 5152)
	if err != nil {
		log.Fatal(err)
	}
	consumer.Version(nsq.ProtocolV2Magic)
	consumer.WriteCommand(consumer.Subscribe("test", "ch"))
	consumer.WriteCommand(consumer.Ready(10))

	for {
		resp, err := consumer.ReadResponse()
		if err != nil {
			log.Fatal(err)
		}
		switch resp.FrameType {
		case nsq.FrameTypeMessage:
			msg := resp.Data.(*nsq.Message)
			log.Printf("%s - %s", util.UuidToStr(msg.Uuid()), msg.Body())
			consumer.WriteCommand(consumer.Finish(util.UuidToStr(msg.Uuid())))
		}
	}
}
