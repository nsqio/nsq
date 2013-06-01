package main

import (
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiskQueue(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	dq := NewDiskQueue(dqName, os.TempDir(), 1024, 2500, 2*time.Second)
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
	dq := NewDiskQueue(dqName, os.TempDir(), 100, 2500, 2*time.Second)
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

func assertFileNotExist(t *testing.T, fn string) {
	f, err := os.OpenFile(fn, os.O_RDONLY, 0600)
	assert.Equal(t, f, (*os.File)(nil))
	assert.Equal(t, os.IsNotExist(err), true)
}

func TestDiskQueueEmpty(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	dqName := "test_disk_queue_empty" + strconv.Itoa(int(time.Now().Unix()))
	dq := NewDiskQueue(dqName, os.TempDir(), 100, 2500, 2*time.Second)
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

	for {
		if dq.Depth() == 97 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.Equal(t, dq.Depth(), int64(97))

	numFiles := dq.(*DiskQueue).writeFileNum
	dq.Empty()

	assertFileNotExist(t, dq.(*DiskQueue).metaDataFileName())
	for i := int64(0); i <= numFiles; i++ {
		assertFileNotExist(t, dq.(*DiskQueue).fileName(i))
	}
	assert.Equal(t, dq.Depth(), int64(0))
	assert.Equal(t, dq.(*DiskQueue).readFileNum, dq.(*DiskQueue).writeFileNum)
	assert.Equal(t, dq.(*DiskQueue).readPos, dq.(*DiskQueue).writePos)
	assert.Equal(t, dq.(*DiskQueue).nextReadPos, dq.(*DiskQueue).readPos)
	assert.Equal(t, dq.(*DiskQueue).nextReadFileNum, dq.(*DiskQueue).readFileNum)

	for i := 0; i < 100; i++ {
		err := dq.Put(msg)
		assert.Equal(t, err, nil)
		assert.Equal(t, dq.Depth(), int64(i+1))
	}

	for i := 0; i < 100; i++ {
		<-dq.ReadChan()
	}

	for {
		if dq.Depth() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	assert.Equal(t, dq.Depth(), int64(0))
	assert.Equal(t, dq.(*DiskQueue).readFileNum, dq.(*DiskQueue).writeFileNum)
	assert.Equal(t, dq.(*DiskQueue).readPos, dq.(*DiskQueue).writePos)
	assert.Equal(t, dq.(*DiskQueue).nextReadPos, dq.(*DiskQueue).readPos)
}

func TestDiskQueueCorruption(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	dqName := "test_disk_queue_corruption" + strconv.Itoa(int(time.Now().Unix()))
	dq := NewDiskQueue(dqName, os.TempDir(), 1000, 5, 2*time.Second)

	msg := make([]byte, 123)
	for i := 0; i < 25; i++ {
		dq.Put(msg)
	}

	assert.Equal(t, dq.Depth(), int64(25))

	// corrupt the 2nd file
	dqFn := dq.(*DiskQueue).fileName(1)
	os.Truncate(dqFn, 500)

	for i := 0; i < 19; i++ {
		assert.Equal(t, <-dq.ReadChan(), msg)
	}

	// corrupt the 4th (current) file
	dqFn = dq.(*DiskQueue).fileName(3)
	os.Truncate(dqFn, 100)

	dq.Put(msg)

	assert.Equal(t, <-dq.ReadChan(), msg)
}

func TestDiskQueueTorture(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	var wg sync.WaitGroup

	dqName := "test_disk_queue_torture" + strconv.Itoa(int(time.Now().Unix()))
	dq := NewDiskQueue(dqName, os.TempDir(), 262144, 2500, 2*time.Second)
	assert.NotEqual(t, dq, nil)
	assert.Equal(t, dq.Depth(), int64(0))

	msg := []byte("aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff")

	numWriters := 4
	numReaders := 4
	readExitChan := make(chan int)
	writeExitChan := make(chan int)

	var depth int64
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				time.Sleep(100000 * time.Nanosecond)
				select {
				case <-writeExitChan:
					return
				default:
					err := dq.Put(msg)
					if err == nil {
						atomic.AddInt64(&depth, 1)
					}
				}
			}
		}()
	}

	time.Sleep(1 * time.Second)

	dq.Close()

	log.Printf("closing writeExitChan")
	close(writeExitChan)
	wg.Wait()

	log.Printf("restarting diskqueue")
	dq = NewDiskQueue(dqName, os.TempDir(), 262144, 2500, 2*time.Second)
	assert.NotEqual(t, dq, nil)
	assert.Equal(t, dq.Depth(), depth)

	var read int64
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				time.Sleep(100000 * time.Nanosecond)
				select {
				case m := <-dq.ReadChan():
					assert.Equal(t, msg, m)
					atomic.AddInt64(&read, 1)
				case <-readExitChan:
					return
				}
			}
		}()
	}

	log.Printf("waiting for depth 0")
	for {
		if dq.Depth() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	log.Printf("closing readExitChan")
	close(readExitChan)
	wg.Wait()

	assert.Equal(t, read, depth)

	dq.Close()
}

func BenchmarkDiskQueuePut(b *testing.B) {
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	dqName := "bench_disk_queue_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	dq := NewDiskQueue(dqName, os.TempDir(), 1024, 2500, 2*time.Second)
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
	dq := NewDiskQueue(dqName, os.TempDir(), 1024768, 2500, 2*time.Second)
	for i := 0; i < b.N; i++ {
		dq.Put([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		<-dq.ReadChan()
	}
}
