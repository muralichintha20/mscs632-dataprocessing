// Package main implements a concurrent Data Processing System in Go.
//
// A pool of worker goroutines receives tasks from a buffered channel that
// serves as a concurrency-safe queue, processes each task with a simulated
// delay, and sends results to a single writer goroutine that owns the output
// file. Synchronization relies on channels and a sync.WaitGroup; the writer
// owning the file means no mutex is needed for the file itself.
//
// Error handling follows Go idioms: functions return errors, the caller checks
// them, and deferred calls guarantee the log and output files are closed even
// when an error occurs.
package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Task is a unit of work placed on the queue channel.
type Task struct {
	ID     int
	Record string // "label,n"
}

// Result is the outcome of processing one Task.
type Result struct {
	TaskID  int
	Worker  string
	Value   int64
	Summary string
}

// process performs the simulated computation for a task. It returns an error
// (rather than panicking) when the record cannot be parsed, so the worker can
// log it and move on. This mirrors Go's "errors are values" philosophy.
func process(t Task) (Result, error) {
	time.Sleep(time.Duration(40+rand.Intn(120)) * time.Millisecond)

	parts := strings.SplitN(t.Record, ",", 2)
	if len(parts) != 2 {
		return Result{}, fmt.Errorf("malformed record %q", t.Record)
	}
	label := strings.TrimSpace(parts[0])
	n, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return Result{}, fmt.Errorf("bad number in %q: %w", t.Record, err)
	}

	var sumOfSquares int64
	for i := int64(1); i <= int64(n); i++ {
		sumOfSquares += i * i
	}
	return Result{
		TaskID:  t.ID,
		Value:   sumOfSquares,
		Summary: fmt.Sprintf("%s[1..%d]", label, n),
	}, nil
}

// worker consumes tasks from the queue channel until it is closed and drained,
// then signals completion through the WaitGroup. Results go to the results
// channel; parse errors are counted and logged.
func worker(id int, queue <-chan Task, results chan<- Result,
	processed, errors *atomic.Int64, wg *sync.WaitGroup) {

	defer wg.Done()
	name := fmt.Sprintf("worker-%d", id)
	log.Printf("[%s] started", name)

	for t := range queue { // ranges until the channel is closed and empty
		res, err := process(t)
		if err != nil {
			errors.Add(1)
			log.Printf("[%s] ERROR task#%d: %v", name, t.ID, err)
			continue
		}
		res.Worker = name
		results <- res
		processed.Add(1)
		log.Printf("[%s] completed task#%d -> value=%d", name, t.ID, res.Value)
	}
	log.Printf("[%s] finished", name)
}

// writeResults owns the output file and is the single goroutine that touches
// it, so writes are serialized without an explicit lock. The deferred Close
// guarantees the file is flushed and released even on an early return.
func writeResults(path string, results <-chan Result, done chan<- int) {
	count := 0
	f, err := os.Create(path)
	if err != nil {
		log.Printf("[writer] ERROR creating %s: %v", path, err)
		done <- count
		return
	}
	defer f.Close()

	if _, err := fmt.Fprintln(f, "taskId,worker,value,summary"); err != nil {
		log.Printf("[writer] ERROR writing header: %v", err)
	}
	for r := range results {
		if _, err := fmt.Fprintf(f, "%d,%s,%d,%s\n",
			r.TaskID, r.Worker, r.Value, r.Summary); err != nil {
			log.Printf("[writer] ERROR writing task#%d: %v", r.TaskID, err)
			continue
		}
		count++
	}
	done <- count
}

const workerCount = 4

func main() {
	// Mirror logs to stdout and a log file using an io.MultiWriter.
	logFile, err := os.Create("log_go.txt")
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot create log file:", err)
		return
	}
	defer logFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	log.Printf("=== Go Data Processing System starting with %d workers ===", workerCount)

	queue := make(chan Task, 16) // buffered channel acts as the shared queue
	results := make(chan Result, 16)
	done := make(chan int, 1)

	var processed, errors atomic.Int64
	var wg sync.WaitGroup

	// Start the single writer goroutine.
	go writeResults("results_go.csv", results, done)

	// Start the worker pool.
	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go worker(i, queue, results, &processed, &errors, &wg)
	}

	// Produce work. Record id 5 is intentionally malformed.
	records := []string{
		"alpha,10", "bravo,25", "charlie,7", "delta,40",
		"echo,not_a_number", "foxtrot,15", "golf,33", "hotel,5",
		"india,28", "juliet,12",
	}
	for i, rec := range records {
		queue <- Task{ID: i + 1, Record: rec}
	}
	close(queue) // closing the queue lets every worker's range loop end
	log.Printf("enqueued %d tasks; queue closed", len(records))

	wg.Wait()      // wait for all workers to drain the queue
	close(results) // safe to close once no worker will send again
	written := <-done

	log.Printf("=== Done. processed=%d errors=%d written=%d ===",
		processed.Load(), errors.Load(), written)
}
