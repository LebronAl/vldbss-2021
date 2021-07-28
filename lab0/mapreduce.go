package main

import (
	"bytes"
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync"

	"github.com/natefinch/atomic"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// ReduceF function from MIT 6.824 LAB1
type ReduceF func(key string, values []string) string

// MapF function from MIT 6.824 LAB1
type MapF func(filename string, contents string) []KeyValue

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase uint8

const (
	mapPhase jobPhase = iota
	reducePhase
)

type task struct {
	dataDir    string
	jobName    string
	mapFile    string   // only for map, the input file
	phase      jobPhase // are we in mapPhase or reducePhase?
	taskNumber int      // this task's index in the current phase
	nMap       int      // number of map tasks
	nReduce    int      // number of reduce tasks
	mapF       MapF     // map function used in this job
	reduceF    ReduceF  // reduce function used in this job
	wg         sync.WaitGroup
}

// MRCluster represents a map-reduce cluster.
type MRCluster struct {
	nWorkers int
	wg       sync.WaitGroup
	taskCh   chan *task
	exit     chan struct{}
}

var singleton = &MRCluster{
	nWorkers: runtime.NumCPU(),
	taskCh:   make(chan *task),
	exit:     make(chan struct{}),
}

func init() {
	singleton.Start()
}

// GetMRCluster returns a reference to a MRCluster.
func GetMRCluster() *MRCluster {
	return singleton
}

// NWorkers returns how many workers there are in this cluster.
func (c *MRCluster) NWorkers() int { return c.nWorkers }

// Start starts this cluster.
func (c *MRCluster) Start() {
	for i := 0; i < c.nWorkers; i++ {
		c.wg.Add(1)
		go c.worker()
	}
}

func (c *MRCluster) worker() {
	defer c.wg.Done()
	for {
		select {
		case t := <-c.taskCh:
			if t.phase == mapPhase {
				file, err := os.Open(t.mapFile)
				if err != nil {
					log.Fatalf("cannot open %v", t.mapFile)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", t.mapFile)
				}
				if err := file.Close(); err != nil {
					log.Fatalf("cannot close %v", t.mapFile)
				}
				kvs := t.mapF(t.mapFile, string(content))
				intermediates := make([][]KeyValue, t.nReduce)
				for _, kv := range kvs {
					index := ihash(kv.Key) % t.nReduce
					intermediates[index] = append(intermediates[index], kv)
				}
				var wg sync.WaitGroup
				for index, intermediate := range intermediates {
					wg.Add(1)
					go func(index int, intermediate []KeyValue) {
						defer wg.Done()
						intermediateFilePath := reduceName(t.dataDir, t.jobName, t.taskNumber, index)
						var buf bytes.Buffer
						enc := json.NewEncoder(&buf)
						for _, kv := range intermediate {
							err := enc.Encode(&kv)
							if err != nil {
								log.Fatalf("cannot encode json %v", kv.Key)
							}
						}
						if err := atomic.WriteFile(intermediateFilePath, &buf); err != nil {
							log.Fatalf(err.Error())
						}
					}(index, intermediate)
				}
				wg.Wait()
			} else {
				var kvs []KeyValue
				for i := 0; i < t.nMap; i++ {
					intermediateFilePath := reduceName(t.dataDir, t.jobName, i, t.taskNumber)
					file, err := os.Open(intermediateFilePath)
					if err != nil {
						log.Fatalf("cannot open %v", intermediateFilePath)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kvs = append(kvs, kv)
					}
					if err := file.Close(); err != nil {
						log.Fatalf("cannot close %v", intermediateFilePath)
					}
				}
				results := make(map[string][]string)
				// Maybe we need merge sort for larger data
				for _, kv := range kvs {
					results[kv.Key] = append(results[kv.Key], kv.Value)
				}
				buf := bytes.NewBufferString("")
				for key, values := range results {
					output := t.reduceF(key, values)
					buf.WriteString(output)
				}
				if err := atomic.WriteFile(mergeName(t.dataDir, t.jobName, t.taskNumber), buf); err != nil {
					log.Fatalf(err.Error())
				}
			}
			t.wg.Done()
		case <-c.exit:
			return
		}
	}
}

// Shutdown shutdowns this cluster.
func (c *MRCluster) Shutdown() {
	close(c.exit)
	c.wg.Wait()
}

// Submit submits a job to this cluster.
func (c *MRCluster) Submit(jobName, dataDir string, mapF MapF, reduceF ReduceF, mapFiles []string, nReduce int) <-chan []string {
	notify := make(chan []string)
	go c.run(jobName, dataDir, mapF, reduceF, mapFiles, nReduce, notify)
	return notify
}

func (c *MRCluster) run(jobName, dataDir string, mapF MapF, reduceF ReduceF, mapFiles []string, nReduce int, notify chan<- []string) {
	// map phase
	nMap := len(mapFiles)
	tasks := make([]*task, 0, nMap)
	for i := 0; i < nMap; i++ {
		t := &task{
			dataDir:    dataDir,
			jobName:    jobName,
			mapFile:    mapFiles[i],
			phase:      mapPhase,
			taskNumber: i,
			nReduce:    nReduce,
			mapF:       mapF,
		}
		t.wg.Add(1)
		tasks = append(tasks, t)
		go func() { c.taskCh <- t }()
	}
	for _, t := range tasks {
		t.wg.Wait()
	}

	// reduce phase
	tasks = make([]*task, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		t := &task{
			dataDir:    dataDir,
			jobName:    jobName,
			phase:      reducePhase,
			taskNumber: i,
			nMap:       nMap,
			reduceF:    reduceF,
		}
		t.wg.Add(1)
		tasks = append(tasks, t)
		go func() { c.taskCh <- t }()
	}
	for _, t := range tasks {
		t.wg.Wait()
	}

	results := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		results[i] = mergeName(dataDir, jobName, i)
	}

	notify <- results
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

func reduceName(dataDir, jobName string, mapTask int, reduceTask int) string {
	return path.Join(dataDir, "mrtmp."+jobName+"-"+strconv.Itoa(mapTask)+"-"+strconv.Itoa(reduceTask))
}

func mergeName(dataDir, jobName string, reduceTask int) string {
	return path.Join(dataDir, "mrtmp."+jobName+"-res-"+strconv.Itoa(reduceTask))
}
