package goworker

import (
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/cihub/seelog"
	"github.com/youtube/vitess/go/pools"
)

var logger seelog.LoggerInterface

// Call this function to run goworker. Check for errors in
// the return value. Work will take over the Go executable
// and will run until a QUIT, INT, or TERM signal is
// received, or until the queues are empty if the
// -exit-on-complete flag is set.
func Work() error {
	err := initEnv()
	if err != nil {
		return err
	}
	p := newRedisPool(uri, connections, connections, time.Minute)
	defer p.Close()
	return startWithPool(p)
}

func WorkWithQueues(queues []string) error {
	options := WorkerOptions{
		Queues:         queues,
		MinConnections: connections,
		MaxConnections: connections,
		Timeout:        time.Minute,
	}
	return WorkWithOptions(options)
}

type WorkerOptions struct {
	Queues         []string
	MinConnections int
	MaxConnections int
	Timeout        time.Duration
}

func WorkWithOptions(options WorkerOptions) error {
	err := initEnvWithQueues(options.Queues)
	if err != nil {
		return err
	}
	p := newRedisPool(uri, options.MinConnections, options.MaxConnections, options.Timeout)
	defer p.Close()
	return startWithPool(p)
}

// Call this function to run goworker with the given pool.
func WorkWithPool(pool *pools.ResourcePool) error {
	err := initEnv()
	if err != nil {
		return err
	}
	return startWithPool(pool)
}

// Init logger and flags.
func initLogger() error {
	var err error
	logger, err = seelog.LoggerFromWriterWithMinLevel(os.Stdout, seelog.InfoLvl)
	if err != nil {
		return err
	}
	return nil
}

func initEnv() error {
	initLogger()
	if err := flags(); err != nil {
		return err
	}
	return nil
}

func initEnvWithQueues(queues []string) error {
	initLogger()
	if err := flagsWithQueues(queues); err != nil {
		return err
	}
	return nil
}

// Start worker with the given pool.
func startWithPool(p *pools.ResourcePool) error {
	pool = p
	quit := signals()

	poller, err := newPoller(queues, isStrict)
	if err != nil {
		return err
	}
	jobs := poller.poll(p, time.Duration(interval), quit)

	var monitor sync.WaitGroup

	for id := 0; id < concurrency; id++ {
		worker, err := newWorker(strconv.Itoa(id), queues)
		if err != nil {
			return err
		}
		worker.work(p, jobs, &monitor)
	}

	monitor.Wait()
	return nil
}
