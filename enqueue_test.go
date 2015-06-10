package goworker

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"golang.org/x/net/context"
)

func TestEnqueueHasNoDeadlock(t *testing.T) {
	p := newRedisPool(uri, 1, 1, time.Minute)
	defer p.Close()

	exitOnComplete = true
	queues.Set("test_enqueue_has_no_deadlock")
	jobProcessed := false
	Register("NoDeadLock", func(q string, args ...interface{}) error {
		Enqueue("dummy", "Dummy", nil)
		jobProcessed = true
		return nil
	})
	Enqueue("test_enqueue_has_no_deadlock", "NoDeadLock", nil)
	err := WorkWithPool(p)
	if !jobProcessed {
		t.Error("job has not been processed")
	}
	if err != nil {
		t.Errorf("Error occured %v", err)
	}
	if p.IsClosed() {
		t.Error("Pool should not be closed")
	}
	ctx := context.Background()
	resource, _ := p.Get(ctx)
	conn := resource.(*redisConn)
	defer p.Put(conn)
	defer conn.Do("DEL", fmt.Sprintf("%squeue:dummy", namespace))
	defer conn.Do("DEL", fmt.Sprintf("%squeue:test_enqueue_has_no_deadlock", namespace))
}

func TestEnqueueWriteToRedis(t *testing.T) {
	p := newRedisPool(uri, 1, 1, time.Minute)
	defer p.Close()

	queues.Set("no")
	Enqueue("test2", "TestEnqueueWriteToRedis", nil)
	WorkWithPool(p)
	ctx := context.Background()
	resource, _ := pool.Get(ctx)
	conn := resource.(*redisConn)
	defer pool.Put(conn)
	defer conn.Do("DEL", fmt.Sprintf("%squeue:test2", namespace))
	res, err := conn.Do("LPOP", fmt.Sprintf("%squeue:test2", namespace))
	if err != nil {
		t.Errorf("%v", err)
	}
	jsonData, _ := redis.Bytes(res, nil)
	var data map[string]interface{}
	json.Unmarshal(jsonData, &data)
	if data["class"] != "TestEnqueueWriteToRedis" {
		t.Error(data["class"])
	}
}
