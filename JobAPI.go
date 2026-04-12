package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	queued = iota + 1
	processing
	done
	failed
)

const WorkerCount = 3

const MaxJobRetryAttempts = 3

type Job struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Status      uint   `json:"status"`
	Attempts    int    `json:"attempts"`
	MaxAttempts int    `json:"max_attempts"`
}

type CreateJobRequest struct {
	Name string `json:"name"`
}

type JobAPI struct {
	jobs   []Job
	queue  chan int
	mu     *sync.RWMutex
	server *http.Server
	logger *slog.Logger
	wg     sync.WaitGroup
}

func NewJobAPI(Addr string) *JobAPI {
	if !strings.HasPrefix(Addr, ":") {
		Addr = fmt.Sprintf(":%v", Addr)
	}

	mu := &sync.RWMutex{}
	server := &http.Server{Addr: Addr}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	return &JobAPI{
		jobs:   make([]Job, 0),
		mu:     mu,
		server: server,
		logger: logger,
		queue:  make(chan int, 50),
		wg:     sync.WaitGroup{},
	}
}

func (a *JobAPI) Serve() error {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go a.handleShutdown(ctx, cancel, sigChan)
	a.logger.Info("server starting at", "port", a.server.Addr)

	// workers

	if WorkerCount > 0 {
		for range WorkerCount {
			a.wg.Add(1)
			go a.Worker(ctx)
		}
	}

	http.HandleFunc("/jobs", a.JobHandler)
	return a.server.ListenAndServe()
}

func (a *JobAPI) JobHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		err, j := a.handleCreateJob(r.Body)
		if err != nil {
			a.logger.Error("job creation", "error", err)
			WriteJSON(w, resp{"bad request"}, http.StatusBadRequest)
			return
		}
		WriteJSON(w, j, http.StatusCreated)
	} else if r.Method == http.MethodGet {
		id := r.URL.Query().Get("id")
		if id == "" {
			// Retrieve all jobs
			js := a.handleRetrieveJobs()
			WriteJSON(w, js, http.StatusOK)
			return
		} else {
			// Retrieve single job
			idInt, err := strconv.Atoi(id)
			if err != nil {
				a.logger.Error("invalid job id", "id", id)
				WriteJSON(w, resp{"invalid job id"}, http.StatusBadRequest)
				return
			}

			ok, j := a.handleRetrieveJob(idInt)
			if !ok {
				WriteJSON(w, resp{"job not found"}, http.StatusNotFound)
				return
			}

			WriteJSON(w, j, http.StatusOK)
		}

	} else {
		WriteJSON(w, resp{"method not allowed"}, http.StatusMethodNotAllowed)
	}
}

func (a *JobAPI) handleRetrieveJob(id int) (bool, Job) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, j := range a.jobs {
		if j.ID == id {
			return true, j
		}
	}
	return false, Job{}
}

func (a *JobAPI) handleRetrieveJobs() []Job {
	a.mu.RLock()
	defer a.mu.RUnlock()

	dist := make([]Job, len(a.jobs))
	copy(dist, a.jobs)

	return dist
}

func (a *JobAPI) handleCreateJob(r io.Reader) (error, Job) {
	req := &CreateJobRequest{}
	if err := json.NewDecoder(r).Decode(req); err != nil {
		return err, Job{}
	}

	a.mu.Lock()
	JobID := getNewJobID(a.jobs)
	j := Job{
		ID:          JobID,
		Name:        req.Name,
		Status:      queued,
		MaxAttempts: MaxJobRetryAttempts,
	}
	a.jobs = append(a.jobs, j)
	a.mu.Unlock()

	a.queue <- JobID

	return nil, j
}

func (a *JobAPI) handleShutdown(ctx context.Context, cancel context.CancelFunc, sigChan chan os.Signal) {
	<-sigChan
	a.logger.Info("shutdown signal received")
	cancel()

	shutdownCtx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	a.server.Shutdown(shutdownCtx)

	a.wg.Wait()
}

func (a *JobAPI) Worker(ctx context.Context) {
	a.logger.Info("Worker started", "time", time.Now())
	for {
		select {
		case id, ok := <-a.queue:
			if !ok {
				a.wg.Done()
				return
			}
			a.JobProcessing(id)
		case <-ctx.Done():
			a.logger.Info("worker stop", "error", ctx.Err())
			a.wg.Done()
			return
		}
	}
}

func (a *JobAPI) JobProcessing(id int) {
	ok := a.UpdateJobStatus(id, processing)
	if !ok {
		a.logger.Error("update job status", "status", getStatusText(processing))
		return
	}

	a.logger.Info("processing job", "id", id)
	<-time.After(time.Second * 2)

	if id%2 == 0 {
		// failed simulation
		ok = a.UpdateJobStatus(id, failed)
		a.logger.Error("job failed", "id", id)
		if !ok {
			a.logger.Error("update job status", "status", getStatusText(failed))
			return
		}
		a.FailedJobProcessing(id)
		return
	} else {
		ok = a.UpdateJobStatus(id, done)
		if !ok {
			a.logger.Error("update job status", "status", getStatusText(done))
			return
		}
	}

	a.logger.Info("job done", "id", id)
}

func (a *JobAPI) FailedJobProcessing(id int) {
	a.mu.RLock()
	jobIndex, ok := getJobID(a.jobs, id)
	if !ok {
		a.mu.RUnlock()
		a.logger.Error("failed job not found", "id", id)
		return
	}
	j := a.jobs[jobIndex]
	a.mu.RUnlock()

	if j.Attempts < j.MaxAttempts {
		// resend to queue
		ok = a.UpdateJobStatus(id, queued)
		if ok {
			go func() {
				<-time.After(time.Second)
				a.queue <- id
			}()
		}
	} else {
		// permanently fail
		a.UpdateJobStatus(id, failed)
		a.logger.Error("job failed permanently", "status", getStatusText(done))
	}
}

func (a *JobAPI) UpdateJobStatus(id int, status uint) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	for k, j := range a.jobs {
		if j.ID == id {
			//update status
			a.jobs[k].Status = status
			if status == processing {
				a.jobs[k].Attempts++
			}
			return true
		}
	}
	return false
}


type resp struct {
	Message string `json:"message" `
}

func WriteJSON(w http.ResponseWriter, data any, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func getNewJobID(j []Job) int {
	return len(j) + 1
}

func getJobID(j []Job, id int) (int, bool) {
	for k, job := range j {
		if job.ID == id {
			return k, true
		}
	}
	return 0, false
}

func getStatusText(status uint) string {
	switch status {
	case queued:
		return "queued"
	case processing:
		return "processing"
	case done:
		return "done"
	case failed:
		return "failed"
	default:
		return "unknown"
	}
}
