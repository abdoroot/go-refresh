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
	dispatchStatusQueued     = "queued"
	dispatchStatusProcessing = "processing"
	dispatchStatusSent       = "sent"
	dispatchStatusFailed     = "failed"
)

const dispatchWorkerCount = 3

const dispatchMaxJobRetryAttempts = 3

const channelWhatsapp = "whatsapp"
const channelEmail = "email"

type DispatchJob struct {
	ID          int       `json:"id"`
	Channel     string    `json:"channel"`
	Recipient   string    `json:"recipient"`
	Message     string    `json:"message"`
	Status      string    `json:"status"`
	Attempts    int       `json:"attempts"`
	MaxAttempts int       `json:"max_attempts"`
	CreatedAt   time.Time `json:"created_at"`
}

type CreateDispatchRequest struct {
	Channel   string `json:"channel"`
	Recipient string `json:"recipient"`
	Message   string `json:"message"`
}

func (r CreateDispatchRequest) validate() error {
	if r.Channel != channelWhatsapp && r.Channel != channelEmail {
		return fmt.Errorf("channel should be whatsapp or email only")
	}

	if r.Channel == channelEmail && !isEmailValid(r.Recipient) {
		return fmt.Errorf("please enter valid recipient")
	}

	if r.Channel == channelWhatsapp && !isMobileNumValid(r.Recipient) {
		return fmt.Errorf("please enter valid recipient")
	}

	if !isMesssageValid(r.Message) {
		return fmt.Errorf("please enter valid message")
	}

	return nil
}

type CreateBulkDispatchRequest struct {
	Channel    string   `json:"channel"`
	Recipients []string `json:"recipients"`
	Message    string   `json:"message"`
}

func (r CreateBulkDispatchRequest) validate() error {
	if r.Channel != channelWhatsapp && r.Channel != channelEmail {
		return fmt.Errorf("channel should be whatsapp or email only")
	}

	if len(r.Recipients) == 0 {
		return fmt.Errorf("recipients should not be empty")
	} else {
		switch r.Channel {
		case channelWhatsapp:
			for _, m := range r.Recipients {
				if !isMobileNumValid(m) {
					return fmt.Errorf("recipients mobile % not valid", m)
				}
			}
		case channelEmail:
			for _, e := range r.Recipients {
				if !isEmailValid(e) {
					return fmt.Errorf("recipients emobil% not valid", e)
				}
			}
		default:
			return fmt.Errorf("unkonw recipients type")
		}
	}

	if isMesssageValid(r.Message) {
		return fmt.Errorf("please enter valid message")
	}

	return nil
}

type dispatchResp struct {
	Message string `json:"message"`
}

type DispatcherAPI struct {
	jobs   []DispatchJob
	queue  chan int
	mu     *sync.RWMutex
	server *http.Server
	logger *slog.Logger
	wg     sync.WaitGroup
}

func NewDispatcherAPI(Addr string) *DispatcherAPI {
	if !strings.HasPrefix(Addr, ":") {
		Addr = fmt.Sprintf(":%v", Addr)
	}

	mu := &sync.RWMutex{}
	server := &http.Server{Addr: Addr}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	return &DispatcherAPI{
		jobs:   make([]DispatchJob, 0),
		mu:     mu,
		server: server,
		logger: logger,
		queue:  make(chan int, 50),
		wg:     sync.WaitGroup{},
	}
}

func (a *DispatcherAPI) Serve() error {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go a.handleShutdown(cancel, sigChan)
	a.logger.Info("server starting at", "port", a.server.Addr)

	// workers

	if dispatchWorkerCount > 0 {
		for range dispatchWorkerCount {
			a.wg.Add(1)
			go a.Worker(ctx)
		}
	}

	http.HandleFunc("/dispatch", a.dispatchHandler)
	http.HandleFunc("/dispatch/bulk", a.bulkDispatchHandler)
	return a.server.ListenAndServe()
}

func (a *DispatcherAPI) dispatchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		err, j := a.handleCreateJob(r.Body)
		if err != nil {
			a.logger.Error("job creation", "error", err)
			a.writeJSON(w, dispatchResp{"bad request"}, http.StatusBadRequest)
			return
		}
		a.writeJSON(w, j, http.StatusCreated)
	} else if r.Method == http.MethodGet {
		id := r.URL.Query().Get("id")
		if id == "" {
			// Retrieve all jobs
			js := a.handleRetrieveJobs()
			a.writeJSON(w, js, http.StatusOK)
			return
		} else {
			// Retrieve single job
			idInt, err := strconv.Atoi(id)
			if err != nil {
				a.logger.Error("invalid job id", "id", id)
				a.writeJSON(w, dispatchResp{"invalid job id"}, http.StatusBadRequest)
				return
			}

			ok, j := a.handleRetrieveJob(idInt)
			if !ok {
				a.writeJSON(w, dispatchResp{"job not found"}, http.StatusNotFound)
				return
			}

			a.writeJSON(w, j, http.StatusOK)
		}

	} else {
		a.writeJSON(w, dispatchResp{"method not allowed"}, http.StatusMethodNotAllowed)
	}
}

func (a *DispatcherAPI) bulkDispatchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		err, jobs := a.handleCreateBulkJobs(r.Body)
		if err != nil {
			a.logger.Error("bulk job creation", "error", err)
			a.writeJSON(w, dispatchResp{"bad request"}, http.StatusBadRequest)
			return
		}
		a.writeJSON(w,
			struct {
				Count int           `json:"count"`
				Jobs  []DispatchJob `json:"jobs"`
			}{
				Count: len(jobs),
				Jobs:  jobs,
			},
			http.StatusCreated)
	} else {
		a.writeJSON(w, dispatchResp{"method not allowed"}, http.StatusMethodNotAllowed)
	}
}

func (a *DispatcherAPI) handleRetrieveJob(id int) (bool, DispatchJob) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, j := range a.jobs {
		if j.ID == id {
			return true, j
		}
	}
	return false, DispatchJob{}
}

func (a *DispatcherAPI) handleRetrieveJobs() []DispatchJob {
	a.mu.RLock()
	defer a.mu.RUnlock()

	dist := make([]DispatchJob, len(a.jobs))
	copy(dist, a.jobs)

	return dist
}

func (a *DispatcherAPI) handleCreateBulkJobs(r io.Reader) (error, []DispatchJob) {
	req := &CreateBulkDispatchRequest{}
	if err := json.NewDecoder(r).Decode(req); err != nil {
		return err, nil
	}

	if err := req.validate(); err != nil {
		return err, nil
	}

	a.mu.Lock()
	jobs := []DispatchJob{}

	jobID := a.getNewJobID()
	for i, re := range req.Recipients {
		if i != 0 {
			jobID++
		}
		j := DispatchJob{
			ID:          jobID,
			Channel:     req.Channel,
			Recipient:   re,
			Message:     req.Message,
			Status:      dispatchStatusQueued,
			MaxAttempts: dispatchMaxJobRetryAttempts,
			CreatedAt:   time.Now(),
		}
		jobs = append(jobs, j)
	}
	a.mu.Unlock()

	return nil, jobs
}

func (a *DispatcherAPI) handleCreateJob(r io.Reader) (error, DispatchJob) {
	req := &CreateDispatchRequest{}
	if err := json.NewDecoder(r).Decode(req); err != nil {
		return err, DispatchJob{}
	}

	if err := req.validate(); err != nil {
		return err, DispatchJob{}
	}

	a.mu.Lock()
	jobID := a.getNewJobID()
	j := DispatchJob{
		ID:          jobID,
		Channel:     req.Channel,
		Recipient:   req.Recipient,
		Message:     req.Message,
		Status:      dispatchStatusQueued,
		MaxAttempts: dispatchMaxJobRetryAttempts,
		CreatedAt:   time.Now(),
	}
	a.jobs = append(a.jobs, j)
	a.mu.Unlock()

	a.queue <- jobID

	return nil, j
}

func (a *DispatcherAPI) handleShutdown(cancel context.CancelFunc, sigChan chan os.Signal) {
	<-sigChan
	a.logger.Info("shutdown signal received")
	cancel()

	shutdownCtx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	a.server.Shutdown(shutdownCtx)

	a.wg.Wait()
}

func (a *DispatcherAPI) Worker(ctx context.Context) {
	a.logger.Info("dispatcher worker started", "time", time.Now())
	defer a.wg.Done()

	for {
		select {
		case id, ok := <-a.queue:
			if !ok {
				return
			}
			a.simulateSend(id)
		case <-ctx.Done():
			a.logger.Info("dispatcher worker stop", "error", ctx.Err())
			return
		}
	}
}

func (a *DispatcherAPI) simulateSend(id int) {
	ok := a.updateDispatchJobStatus(id, dispatchStatusProcessing)
	if !ok {
		a.logger.Error("update dispatch job status", "status", dispatchStatusProcessing)
		return
	}

	a.logger.Info("processing dispatch job", "id", id)
	<-time.After(time.Second * 2)

	ok, j := a.handleRetrieveJob(id)
	if !ok {
		a.logger.Error("dispatch job not found", "id", id)
		return
	}

	if a.shouldFailFirst(j) {
		a.logger.Error("dispatch job failed", "id", id)
		a.processFailedDispatchJob(id)
		return
	}

	ok = a.updateDispatchJobStatus(id, dispatchStatusSent)
	if !ok {
		a.logger.Error("update dispatch job status", "status", dispatchStatusSent)
		return
	}

	a.logger.Info("dispatch job sent", "id", id)
}

func (a *DispatcherAPI) processFailedDispatchJob(id int) {
	a.mu.RLock()
	jobIndex, ok := a.getJobIndex(id)
	if !ok {
		a.mu.RUnlock()
		a.logger.Error("failed dispatch job not found", "id", id)
		return
	}
	j := a.jobs[jobIndex]
	a.mu.RUnlock()

	if j.Attempts < j.MaxAttempts {
		ok = a.updateDispatchJobStatus(id, dispatchStatusQueued)
		if ok {
			go func() {
				<-time.After(time.Second)
				a.queue <- id
			}()
		}
		return
	}

	a.updateDispatchJobStatus(id, dispatchStatusFailed)
	a.logger.Error("dispatch job failed permanently", "status", dispatchStatusFailed)
}

func (a *DispatcherAPI) updateDispatchJobStatus(id int, status string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	for k, j := range a.jobs {
		if j.ID == id {
			a.jobs[k].Status = status
			if status == dispatchStatusProcessing {
				a.jobs[k].Attempts++
			}
			return true
		}
	}
	return false
}

func (a *DispatcherAPI) shouldFailFirst(j DispatchJob) bool {
	return j.ID%2 == 0 && j.Attempts == 1
}

func (a *DispatcherAPI) writeJSON(w http.ResponseWriter, data any, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func (a *DispatcherAPI) getNewJobID() int {
	return len(a.jobs) + 1
}

func (a *DispatcherAPI) getJobIndex(id int) (int, bool) {
	for k, job := range a.jobs {
		if job.ID == id {
			return k, true
		}
	}
	return 0, false
}

func isEmailValid(email string) bool {
	return strings.Contains(email, "@")
}

func isMobileNumValid(mobile string) bool {
	return len(mobile) > 0 && strings.Contains(mobile, "+")
}

func isMesssageValid(message string) bool {
	return len(message) > 0
}
