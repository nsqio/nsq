package main

import (
	"../nsq"
	"../util"
	"log"
	"net"
)

func main() {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:5152")
	if err != nil {
		log.Fatal(err.Error())
	}

	consumer := nsq.NewConsumer(tcpAddr)
	err = consumer.Connect()
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

		frameType, data, err := consumer.UnpackResponse(resp)
		if err != nil {
			log.Fatal(err)
		}
		switch frameType {
		case nsq.FrameTypeMessage:
			msg := data.(*nsq.Message)
			log.Printf("%s - %s", util.UuidToStr(msg.Uuid()), msg.Body())
			consumer.WriteCommand(consumer.Finish(util.UuidToStr(msg.Uuid())))
		}
	}
}
