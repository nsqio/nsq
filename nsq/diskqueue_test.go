package nsq

import (
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestDiskQueue(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	dq := NewDiskQueue(dqName, os.TempDir(), 1024)
	assert.NotEqual(t, dq, nil)
	assert.Equal(t, dq.Depth(), int64(0))

	msg := []byte("test")
	err := dq.Put(msg)
	assert.Equal(t, err, nil)
	assert.Equal(t, dq.Depth(), int64(1))

	<-dq.ReadReadyChan()
	msgOut, err := dq.Get()
	assert.Equal(t, err, nil)
	assert.Equal(t, msgOut, msg)
}

func TestDiskQueueRoll(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	dqName := "test_disk_queue_roll" + strconv.Itoa(int(time.Now().Unix()))
	dq := NewDiskQueue(dqName, os.TempDir(), 100)
	assert.NotEqual(t, dq, nil)
	assert.Equal(t, dq.Depth(), int64(0))

	msg := []byte("aaaaaaaaaa")
	for i := 0; i < 10; i++ {
		err := dq.Put(msg)
		assert.Equal(t, err, nil)
		assert.Equal(t, dq.Depth(), int64(i+1))
	}

	assert.Equal(t, dq.writeFileNum, int64(1))
	assert.Equal(t, dq.writePos, int64(28))
}

func BenchmarkDiskQueuePut(b *testing.B) {
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	dqName := "bench_disk_queue_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	dq := NewDiskQueue(dqName, os.TempDir(), 1024)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		dq.Put([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	}
}

// this benchmark should be run via:
//    $ go test -test.bench 'DiskQueueGet' -test.benchtime 0.1
// (so that it does not perform too many iterations)
func BenchmarkDiskQueueGet(b *testing.B) {
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	dqName := "bench_disk_queue_get" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	dq := NewDiskQueue(dqName, os.TempDir(), 1024768)
	for i := 0; i < b.N; i++ {
		dq.Put([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		<-dq.ReadReadyChan()
		dq.Get()
	}
}
