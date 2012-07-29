package main

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
	dq := NewDiskQueue(dqName, os.TempDir(), 1024, 2500)
	assert.NotEqual(t, dq, nil)
	assert.Equal(t, dq.Depth(), int64(0))

	msg := []byte("test")
	err := dq.Put(msg)
	assert.Equal(t, err, nil)
	assert.Equal(t, dq.Depth(), int64(1))

	msgOut := <-dq.ReadChan()
	assert.Equal(t, msgOut, msg)
}

func TestDiskQueueRoll(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	dqName := "test_disk_queue_roll" + strconv.Itoa(int(time.Now().Unix()))
	dq := NewDiskQueue(dqName, os.TempDir(), 100, 2500)
	assert.NotEqual(t, dq, nil)
	assert.Equal(t, dq.Depth(), int64(0))

	msg := []byte("aaaaaaaaaa")
	for i := 0; i < 10; i++ {
		err := dq.Put(msg)
		assert.Equal(t, err, nil)
		assert.Equal(t, dq.Depth(), int64(i+1))
	}

	assert.Equal(t, dq.(*DiskQueue).writeFileNum, int64(1))
	assert.Equal(t, dq.(*DiskQueue).writePos, int64(28))
}

func TestDiskQueueEmpty(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	dqName := "test_disk_queue_empty" + strconv.Itoa(int(time.Now().Unix()))
	dq := NewDiskQueue(dqName, os.TempDir(), 100, 2500)
	assert.NotEqual(t, dq, nil)
	assert.Equal(t, dq.Depth(), int64(0))

	msg := []byte("aaaaaaaaaa")

	for i := 0; i < 100; i++ {
		err := dq.Put(msg)
		assert.Equal(t, err, nil)
		assert.Equal(t, dq.Depth(), int64(i+1))
	}

	for i := 0; i < 3; i++ {
		<-dq.ReadChan()
	}
	assert.Equal(t, dq.Depth(), int64(97))

	// cheat and use the lower level method so that the test doesn't
	// break due to timing
	dq.(*DiskQueue).doEmpty()

	assert.Equal(t, dq.Depth(), int64(0))
	assert.Equal(t, dq.(*DiskQueue).readFileNum, dq.(*DiskQueue).writeFileNum)
	assert.Equal(t, dq.(*DiskQueue).readPos, dq.(*DiskQueue).writePos)
	assert.Equal(t, dq.(*DiskQueue).nextReadPos, dq.(*DiskQueue).readPos)

	for i := 0; i < 100; i++ {
		err := dq.Put(msg)
		assert.Equal(t, err, nil)
		assert.Equal(t, dq.Depth(), int64(i+1))
	}

	for i := 0; i < 100; i++ {
		<-dq.ReadChan()
	}

	assert.Equal(t, dq.Depth(), int64(0))
	assert.Equal(t, dq.(*DiskQueue).readFileNum, dq.(*DiskQueue).writeFileNum)
	assert.Equal(t, dq.(*DiskQueue).readPos, dq.(*DiskQueue).writePos)
	assert.Equal(t, dq.(*DiskQueue).nextReadPos, dq.(*DiskQueue).readPos)
}

// THIS TEST IS COMMENTED OUT BECAUSE IT TAKES FOREVER
// import "runtime"
// import "sync/atomic"
// func TestDiskQueueTorture(t *testing.T) {
// 	dqName := "test_disk_queue_torture" + strconv.Itoa(int(time.Now().Unix()))
// 	dq := NewDiskQueue(dqName, os.TempDir(), 1024768, 2500)
// 	assert.NotEqual(t, dq, nil)
// 	assert.Equal(t, dq.Depth(), int64(0))
// 
// 	msg := []byte("aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff")
// 
// 	numWriters := 4
// 	numReaders := 4
// 	readExitChan := make(chan int)
// 	readExitSyncChan := make(chan int)
// 	writeExitChan := make(chan int)
// 	writeExitSyncChan := make(chan int)
// 
// 	var depth int64
// 	for i := 0; i < numWriters; i++ {
// 		go func() {
// 			for {
// 				runtime.Gosched()
// 				select {
// 				case <-writeExitChan:
// 					writeExitSyncChan <- 1
// 					return
// 				default:
// 					err := dq.Put(msg)
// 					if err == nil {
// 						atomic.AddInt64(&depth, 1)
// 					}
// 				}
// 			}
// 		}()
// 	}
// 
// 	time.Sleep(5 * time.Second)
// 
// 	dq.Close()
// 
// 	log.Printf("closing writeExitChan")
// 	close(writeExitChan)
// 	for i := 0; i < numWriters; i++ {
// 		<-writeExitSyncChan
// 	}
// 
// 	dq = NewDiskQueue(dqName, os.TempDir(), 1024768, 2500)
// 	assert.NotEqual(t, dq, nil)
// 	assert.Equal(t, dq.Depth(), depth)
// 
// 	var read int64
// 	for i := 0; i < numReaders; i++ {
// 		go func() {
// 			for {
// 				runtime.Gosched()
// 				select {
// 				case m := <-dq.ReadChan():
// 					assert.Equal(t, msg, m)
// 					atomic.AddInt64(&read, 1)
// 				case <-readExitChan:
// 					readExitSyncChan <- 1
// 					return
// 				}
// 			}
// 		}()
// 	}
// 
// 	log.Printf("waiting for depth 0")
// 	for {
// 		if dq.Depth() == 0 {
// 			break
// 		}
// 		runtime.Gosched()
// 	}
// 
// 	log.Printf("closing readExitChan")
// 	close(readExitChan)
// 	for i := 0; i < numReaders; i++ {
// 		<-readExitSyncChan
// 	}
// 
// 	assert.Equal(t, read, depth)
// 
// 	dq.Close()
// }

func BenchmarkDiskQueuePut(b *testing.B) {
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	dqName := "bench_disk_queue_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	dq := NewDiskQueue(dqName, os.TempDir(), 1024, 2500)
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
	dq := NewDiskQueue(dqName, os.TempDir(), 1024768, 2500)
	for i := 0; i < b.N; i++ {
		dq.Put([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		<-dq.ReadChan()
	}
}
