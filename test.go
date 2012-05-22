package main

import (
	"./client"
	"./util"
	"log"
	"./message"
)

func main() {
	client := client.NewClient(nil)
	err := client.Connect("127.0.0.1", 5152)
	if err != nil {
		log.Fatal(err)
	}
	client.Version("  V2")
	client.WriteCommand(client.Subscribe("test", "ch"))
	client.WriteCommand(client.Ready(10))

	for {
		resp, err := client.ReadResponse()
		if err != nil {
			log.Fatal(err)
		}
		switch resp.FrameType {
		case 2:
			msg := resp.Data.(*message.Message)
			log.Printf("%s - %s", util.UuidToStr(msg.Uuid()), msg.Body())
			client.WriteCommand(client.Finish(util.UuidToStr(msg.Uuid())))
		}
	}
}
