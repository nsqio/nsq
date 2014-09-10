package nsq

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func BenchmarkCommand(b *testing.B) {
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	data := make([]byte, 2048)
	cmd := Publish("test", data)
	var buf bytes.Buffer
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		cmd.WriteTo(&buf)
	}
}
