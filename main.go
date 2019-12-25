package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
)

/* Job stuff */
type Job interface {
	Run(workerID int, resultChan chan<- *JobResults) error
	Finished()
}

type randomNumberJob struct {
	jobID    uuid.UUID
	workerID int
	Results  *JobResults
}

type JobResults map[string]interface{}

func (r *randomNumberJob) Run(workerID int, resultChan chan<- *JobResults) error {
	result := rand.Int()
	r.workerID = workerID
	//fmt.Printf("\n|Worker %d| Job: %d| %d \n", r.workerID, r.jobID, result)
	fields := make(JobResults)
	tag := "Test " + uuid.UUID.String(r.jobID)
	fields[tag] = result
	resultChan <- &fields
	return nil
}

func (r *randomNumberJob) Finished() {
	//fmt.Printf("|Worker %d| Job: %d| Finished.\n", r.workerID, r.jobID)
}

func newRandomNumberJob(jobID uuid.UUID) *randomNumberJob {
	rn := &randomNumberJob{
		jobID: jobID,
	}
	return rn
}

/* manager stuff */
type manager struct {
	workQueue        chan Job
	resultsChan      chan *JobResults
	workerCount      int
	managerCtx       context.Context
	managerCtxCancel context.CancelFunc
}

func newManager() *manager {
	ctx, cancelCtx := context.WithCancel(context.Background())
	nm := &manager{
		workQueue:        make(chan Job),
		resultsChan:      make(chan *JobResults),
		workerCount:      5,
		managerCtx:       ctx,
		managerCtxCancel: cancelCtx,
	}

	for idx := 0; idx < nm.workerCount; idx++ {
		worker := newWorker(idx, nm)
		go worker.Run(nm.managerCtx)
	}
	return nm
}

func (m *manager) scheduleWork(workItem Job) {
	m.workQueue <- workItem
}

/* worker */
type worker struct {
	id          int
	workManager *manager
	jobQueue    <-chan Job
	resultsChan chan<- *JobResults
}

func newWorker(id int, aManager *manager) *worker {
	nw := &worker{
		id:          id,
		workManager: aManager,
		jobQueue:    aManager.workQueue,
		resultsChan: aManager.resultsChan,
	}
	return nw
}

func (w *worker) Run(ctx context.Context) {
	fmt.Printf("\n|%d Worker Started|", w.id)

	for {
		// This is interval amount of time per period
		// (NUM_WORKERS x INTERVAL) i.e 5 api requests per second
		<-time.After(1 * time.Second)
		select {
		case <-ctx.Done():
			fmt.Printf("\n|%d Worker Ending|", w.id)
			return
		case workItem, ok := <-w.jobQueue:
			if !ok {
				return
			}
			err := w.workRun(workItem)
			if err != nil {
				return
			}
		}
	}
}

func (w *worker) workRun(workItem Job) error {
	defer workItem.Finished()

	err := workItem.Run(w.id, w.resultsChan)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	ticker := time.NewTicker(1 * time.Second)
	done := make(chan bool)

	go func() {
		for {
			select {
			case <-done:
				return
			case t := <-ticker.C:
				fmt.Println("Tick at", t)
			}
		}
	}()

	exManager := newManager()
	fields := make(map[string]interface{})

	go func() {
		for {
			select {
			case resultItem, ok := <-exManager.resultsChan:
				if !ok {
					return
				}
				for k, v := range *resultItem {
					fields[k] = v
					fmt.Printf("Tag: %s | Result: %d\n", k, v)
				}
			}
		}
	}()

	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			select {
			case <-termChan:
				exManager.managerCtxCancel()
			}
		}
	}()

	for {
		fmt.Printf("\n=== New Job Run ===\n")
		for i := 0; i < 3; i++ {
			jobID := uuid.New()
			exManager.scheduleWork(newRandomNumberJob(jobID))
		}
		<-time.After(10 * time.Second)
		for k, v := range fields {
			fmt.Printf("\n %s FIELD ITEM: %d", k, v)
		}
	}

	//ticker.Stop()
	//done <- true
	//fmt.Println("Ticker stopped")
}
