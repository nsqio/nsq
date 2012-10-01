package main

import (
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

func TestRegistrationDB(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	beginningOfTime := time.Unix(1348797047, 0)
	p1 := &Producer{"1", "addr", 1, 2, "v1", beginningOfTime}
	p2 := &Producer{"2", "addr", 2, 3, "v1", beginningOfTime}
	p3 := &Producer{"3", "addr", 3, 4, "v1", beginningOfTime}

	db := NewRegistrationDB()
	db.Add(Registration{"c", "a", ""}, p1)
	db.Add(Registration{"c", "a", ""}, p2)
	db.Add(Registration{"c", "a", "b"}, p2)
	db.Add(Registration{"d", "a", ""}, p3)

	r := db.FindRegistrations("c", "*", "").Keys()
	assert.Equal(t, len(r), 1)
	assert.Equal(t, r[0], "a")

	p := db.FindProducers("c", "*", "")
	assert.Equal(t, len(p), 2)
	p = db.FindProducers("c", "a", "")
	assert.Equal(t, len(p), 2)
	p = db.FindProducers("c", "*", "b")
	assert.Equal(t, len(p), 1)
	assert.Equal(t, p[0].producerId, p2.producerId)
	assert.Equal(t, len(p.CurrentProducers()), 0)
	p2.LastUpdate = time.Now()
	assert.Equal(t, len(p.CurrentProducers()), 1)
	p = db.FindProducers("c", "*", "")
	assert.Equal(t, len(p.CurrentProducers()), 1)

	k := db.FindRegistrations("c", "b", "").Keys()
	assert.Equal(t, len(k), 0)
	k = db.FindRegistrations("c", "a", "").Keys()
	assert.Equal(t, len(k), 1)
	assert.Equal(t, k[0], "a")
	k = db.FindRegistrations("c", "*", "b").SubKeys()
	assert.Equal(t, len(k), 1)
	assert.Equal(t, k[0], "b")

	db.Remove(Registration{"c", "a", ""}, p1)
	p = db.FindProducers("c", "*", "*")
	assert.Equal(t, len(p), 1)
	db.Remove(Registration{"c", "a", ""}, p2)
	db.Remove(Registration{"c", "a", "b"}, p2)
	p = db.FindProducers("c", "*", "*")
	assert.Equal(t, len(p), 0)

	// do some key removals
	k = db.FindRegistrations("c", "*", "*").Keys()
	assert.Equal(t, len(k), 2)
	db.RemoveRegistration(Registration{"c", "a", ""})
	db.RemoveRegistration(Registration{"c", "a", "b"})
	k = db.FindRegistrations("c", "*", "*").Keys()
	assert.Equal(t, len(k), 0)

}
