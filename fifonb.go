package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"time"

	"github.com/reservoird/icd"
)

// FifoCfg contains config
type FifoCfg struct {
	Name          string
	SleepDuration string
	Capacity      int
}

// FifoStats contains stats
type FifoStats struct {
	Name             string
	MessagesReceived uint64
	MessagesSent     uint64
	Len              uint64
	Cap              uint64
	Closed           bool
	Monitoring       bool
}

// Fifo contains what is needed for queue
type Fifo struct {
	cfg    FifoCfg
	mutex  sync.Mutex
	stats  FifoStats
	queue  chan interface{}
	sleep  time.Duration
	closed bool
}

// New is what reservoird to create a queue
func New(cfg string) (icd.Queue, error) {
	c := FifoCfg{
		Name:          "com.reservoird.queue.fifo",
		SleepDuration: "1s",
		Capacity:      1000,
	}
	if cfg != "" {
		d, err := ioutil.ReadFile(cfg)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(d, &c)
		if err != nil {
			return nil, err
		}
	}
	sleep, err := time.ParseDuration(c.SleepDuration)
	if err != nil {
		return nil, fmt.Errorf("error parsing duration")
	}
	o := &Fifo{
		cfg:   c,
		mutex: sync.Mutex{},
		stats: FifoStats{
			Name: c.Name,
		},
		queue:  make(chan interface{}, c.Capacity),
		sleep:  sleep,
		closed: false,
	}
	return o, nil
}

// Name returns the name
func (o *Fifo) Name() string {
	return o.cfg.Name
}

// Put sends data to queue
func (o *Fifo) Put(item interface{}) error {
	atomic.AddUint64(&o.stats.MessagesReceived, 1)
	if o.Closed() == true {
		return fmt.Errorf("fifo is closed")
	}
	select {
	case o.queue <- item:
	default:
		return fmt.Errorf("fifo full")
	}
	return nil
}

// Get receives data from queue
func (o *Fifo) Get() (interface{}, error) {
	if o.Closed() == true {
		return nil, fmt.Errorf("fifo is closed")
	}
	select {
	case item, ok := <-o.queue:
		if ok == false {
			return nil, fmt.Errorf("fifo is closed")
		}
		atomic.AddUint64(&o.stats.MessagesSent, 1)
		return item, nil
	default:
		return nil, nil // fifo is empty not an error
	}
}

// Len returns the current length of the Queue
func (o *Fifo) Len() int {
	return len(o.queue)
}

// Cap returns the current length of the Queue
func (o *Fifo) Cap() int {
	return cap(o.queue)
}

// Clear clears the Queue
func (o *Fifo) Clear() {
	for {
		select {
		case <-o.queue:
		default:
			break
		}
	}
}

// Closed returns where or not the queue is closed
func (o *Fifo) Closed() bool {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	return o.closed
}

// Close closes the channel
func (o *Fifo) Close() error {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.closed = true
	o.stats.Closed = o.closed
	close(o.queue)
	return nil
}

func (o *Fifo) getStats(monitoring bool) *FifoStats {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	fifoStats := &FifoStats{
		Name:             o.cfg.Name,
		MessagesReceived: o.stats.MessagesReceived,
		MessagesSent:     o.stats.MessagesSent,
		Len:              uint64(len(o.queue)),
		Cap:              uint64(cap(o.queue)),
		Closed:           o.closed,
		Monitoring:       monitoring,
	}

	return fifoStats
}

func (o *Fifo) clearStats() {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.stats = FifoStats{
		Name:   o.cfg.Name,
		Closed: o.closed,
	}
}

// Monitor provides statistics and clear
func (o *Fifo) Monitor(mc *icd.MonitorControl) {
	defer mc.WaitGroup.Done() // required

	run := true
	for run == true {
		// clear
		select {
		case <-mc.ClearChan:
			o.clearStats()
		default:
		}

		// send stats
		select {
		case mc.StatsChan <- o.getStats(run):
		default:
		}

		// done
		select {
		case <-mc.DoneChan:
			run = false
		case <-time.After(o.sleep):
		}
	}

	// send final stats blocking
	mc.FinalStatsChan <- o.getStats(run)
}
