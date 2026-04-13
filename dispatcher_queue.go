package main

import (
	"bytes"
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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	dispatchStatusQueued     = "queued"
	dispatchStatusProcessing = "processing"
	dispatchStatusSent       = "sent"
	dispatchStatusFailed     = "failed"

	redisQueueKey     = "dispatch:queue"
	redisJobKey       = "dispatch:job"
	redisLastQueueKey = "dispatch:last_id"
)

const (
	dispatchWorkerCount         = 3
	dispatchMaxJobRetryAttempts = 3

	redisConnectionTimeout = time.Second * 2
)

const (
	channelWhatsapp = "whatsapp"
	channelEmail    = "email"
)

type DispatchJob struct {
	ID          uint32    `json:"id" redis:"id"`
	Channel     string    `json:"channel" redis:"channel"`
	Recipient   string    `json:"recipient" redis:"recipient"`
	Message     string    `json:"message" redis:"message"`
	Status      string    `json:"status" redis:"status"`
	Attempts    int       `json:"attempts" redis:"attempts"`
	MaxAttempts int       `json:"max_attempts" redis:"max_attempts"`
	CreatedAt   time.Time `json:"created_at" redis:"created_at"`
}

func (r DispatchJob) ToMap() (map[string]any, error) {
	return map[string]any{
		"id":           r.ID,
		"channel":      r.Channel,
		"recipient":    r.Recipient,
		"message":      r.Message,
		"status":       r.Status,
		"attempts":     r.Attempts,
		"max_attempts": r.MaxAttempts,
		"created_at":   r.CreatedAt.Format(time.RFC3339Nano),
	}, nil
}

func (r DispatchJob) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

func (r *DispatchJob) Unmarshal(s string) error {
	if err := json.Unmarshal([]byte(s), r); err != nil {
		return err
	}
	return nil
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

	if !isMessageValid(r.Message) {
		return fmt.Errorf("please enter valid message")
	}

	return nil
}

func (r CreateDispatchRequest) Marshal() ([]byte, error) {
	return json.Marshal(r)
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
					return fmt.Errorf("recipients mobile %v not valid", m)
				}
			}
		case channelEmail:
			for _, e := range r.Recipients {
				if !isEmailValid(e) {
					return fmt.Errorf("recipients email %v not valid", e)
				}
			}
		default:
			return fmt.Errorf("unknown recipients type")
		}
	}

	if !isMessageValid(r.Message) {
		return fmt.Errorf("please enter valid message")
	}

	return nil
}

type dispatchResp struct {
	Message string `json:"message"`
}

type DispatcherAPI struct {
	rdb     *redis.Client
	server  *http.Server
	logger  *slog.Logger
	lastKey *atomic.Uint32
	wg      sync.WaitGroup
}

func NewDispatcherAPI(addr, redisHost string) *DispatcherAPI {
	if !strings.HasPrefix(addr, ":") {
		addr = fmt.Sprintf(":%v", addr)
	}

	server := &http.Server{Addr: addr}

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisHost,
		Password: "",
		DB:       0,
	})

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	lastKey := &atomic.Uint32{}

	lastDBKey, err := rdbGetLastJobId(rdb)
	if err != nil {
		logger.Error("getting last job id from the queue", "err", err)
	}

	logger.Info("last job id from the queue", "id", lastDBKey)

	if lastDBKey != 0 {
		lastKey.Add(lastDBKey)
	}

	return &DispatcherAPI{
		server:  server,
		logger:  logger,
		rdb:     rdb,
		lastKey: lastKey,
		wg:      sync.WaitGroup{},
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
		j, err := a.handleCreateJob(r.Body)
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
			js, err := a.handleRetrieveJobs()
			if err != nil {
				a.logger.Error("retrieve dispatch jobs", "error", err)
				a.writeJSON(w, dispatchResp{"internal error"}, http.StatusInternalServerError)
				return
			}
			a.writeJSON(w, js, http.StatusOK)
			return
		} else {
			// Retrieve single job
			idUint, err := strconv.ParseUint(id, 10, 32)
			if err != nil {
				a.logger.Error("invalid job id", "id", id)
				a.writeJSON(w, dispatchResp{"invalid job id"}, http.StatusBadRequest)
				return
			}

			j, err := a.handleRetrieveJob(uint32(idUint))
			if err != nil {
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
		jobs, err := a.handleCreateBulkJobs(r.Body)
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

func (a *DispatcherAPI) handleRetrieveJob(id uint32) (DispatchJob, error) {
	ctx, cancel := context.WithTimeout(context.Background(), redisConnectionTimeout)
	defer cancel()

	qk := getJobKey(id)
	fields, err := a.rdb.HGetAll(ctx, qk).Result()
	if err != nil {
		return DispatchJob{}, err
	}
	if len(fields) == 0 {
		return DispatchJob{}, fmt.Errorf("job %d not found", id)
	}

	return mapToDispatchJob(fields)
}

func (a *DispatcherAPI) handleRetrieveJobs() ([]DispatchJob, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := []DispatchJob{}
	r := fmt.Sprintf("%v:*", redisJobKey)
	iter := a.rdb.Scan(ctx, 0, r, 0).Iterator()

	for iter.Next(ctx) {
		key := iter.Val()
		idString := strings.TrimPrefix(key, fmt.Sprintf("%v:", redisJobKey))
		id, err := strconv.ParseUint(idString, 10, 32)
		if err != nil {
			return nil, err
		}

		idInt32 := uint32(id)
		job, err := a.handleRetrieveJob(idInt32)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

func (a *DispatcherAPI) handleCreateBulkJobs(r io.Reader) ([]DispatchJob, error) {
	bulkReq := &CreateBulkDispatchRequest{}
	if err := json.NewDecoder(r).Decode(bulkReq); err != nil {
		return nil, err
	}

	if err := bulkReq.validate(); err != nil {
		return nil, err
	}

	jobs := []DispatchJob{}

	for _, re := range bulkReq.Recipients {
		singleReq := CreateDispatchRequest{
			Channel:   bulkReq.Channel,
			Recipient: re,
			Message:   bulkReq.Message,
		}

		b, err := singleReq.Marshal()
		if err != nil {
			return nil, err
		}

		r := bytes.NewReader(b)
		j, err := a.handleCreateJob(r)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, j)
	}

	return jobs, nil
}

func (a *DispatcherAPI) handleCreateJob(r io.Reader) (DispatchJob, error) {
	req := &CreateDispatchRequest{}
	if err := json.NewDecoder(r).Decode(req); err != nil {
		return DispatchJob{}, err
	}

	if err := req.validate(); err != nil {
		return DispatchJob{}, err
	}

	jobID, err := a.getNewJobID()
	if err != nil {
		return DispatchJob{}, err
	}

	job := DispatchJob{
		ID:          jobID,
		Channel:     req.Channel,
		Recipient:   req.Recipient,
		Message:     req.Message,
		Status:      dispatchStatusQueued,
		MaxAttempts: dispatchMaxJobRetryAttempts,
		CreatedAt:   time.Now(),
	}

	// save the job
	qk := getJobKey(jobID)
	m, err := job.ToMap()
	if err != nil {
		return DispatchJob{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisConnectionTimeout)
	err = a.rdb.HSet(ctx, qk, m).Err()
	cancel()
	if err != nil {
		return DispatchJob{}, err
	}

	// push the job to queue
	ctx2, cancel2 := context.WithTimeout(context.Background(), redisConnectionTimeout)
	err = a.rdb.RPush(ctx2, redisQueueKey, jobID).Err()
	cancel2()
	if err != nil {
		return DispatchJob{}, err
	}

	return job, nil
}

func rdbGetLastJobId(rdb *redis.Client) (uint32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), redisConnectionTimeout)
	defer cancel()
	val, err := rdb.Get(ctx, redisLastQueueKey).Result()
	if err != nil {
		return 0, fmt.Errorf("redis retrieve %v:%v", redisLastQueueKey, err)
	}

	id, err := strconv.ParseUint(val, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("converting string to uint32 %v:%v", redisLastQueueKey, err)
	}
	return uint32(id), nil
}

func (a *DispatcherAPI) handleShutdown(cancel context.CancelFunc, sigChan chan os.Signal) {
	<-sigChan
	a.logger.Info("shutdown signal received")
	cancel()

	shutdownCtx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	a.server.Shutdown(shutdownCtx)

	a.wg.Wait()
	// workers finish
	if err := a.rdb.Close(); err != nil {
		a.logger.Error("closing redis conn", "err", err)
	}
}

func (a *DispatcherAPI) Worker(ctx context.Context) {
	a.logger.Info("dispatcher worker started", "time", time.Now())
	defer a.wg.Done()

	for {
		select {
		case <-ctx.Done():
			a.logger.Info("dispatcher worker stop", "error", ctx)
			return
		default:
			ctx, cancel := context.WithTimeout(context.Background(), redisConnectionTimeout)
			res, err := a.rdb.BLPop(ctx, redisConnectionTimeout, redisQueueKey).Result()
			cancel()
			if err != nil {
				if err != redis.Nil {
					a.logger.Error("redis BLPop", "error", err)
				}
				continue
			}
			idString := res[1]
			idUint, err := strconv.ParseUint(idString, 10, 32)
			if err != nil {
				a.logger.Error("string to uint32", "str", idString)
				continue
			}
			a.simulateSend(uint32(idUint))
		}
	}
}

func (a *DispatcherAPI) simulateSend(id uint32) {
	if err := a.updateDispatchJobStatus(id, dispatchStatusProcessing); err != nil {
		a.logger.Error("update dispatch job status", "status", dispatchStatusProcessing, "err", err)
		return
	}

	a.logger.Info("processing dispatch job", "id", id)
	<-time.After(time.Second * 2)

	j, err := a.handleRetrieveJob(id)
	if err != nil {
		a.logger.Error("dispatch job not found", "id", id)
		return
	}

	if a.shouldFailFirst(j) {
		a.logger.Error("dispatch job failed", "id", id)
		a.processFailedDispatchJob(id)
		return
	}

	if err := a.updateDispatchJobStatus(id, dispatchStatusSent); err != nil {
		a.logger.Error("update dispatch job status", "status", dispatchStatusSent, "err", err)
		return
	}

	a.logger.Info("dispatch job sent", "id", id)
}

func (a *DispatcherAPI) processFailedDispatchJob(id uint32) {
	j, err := a.handleRetrieveJob(id)
	if err != nil {
		a.logger.Error("dispatch job not found", "id", id)
		return
	}

	if j.Attempts < j.MaxAttempts {
		if err := a.updateDispatchJobStatus(id, dispatchStatusQueued); err != nil {
			a.logger.Error("update dispatch job status", "status", dispatchStatusQueued, "err", err)
			return
		}
		go func() {
			<-time.After(time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), redisConnectionTimeout)
			defer cancel()
			if err := a.rdb.RPush(ctx, redisQueueKey, id).Err(); err != nil {
				a.logger.Error("redis RPush", "error", err, "id", id)
			}
		}()
		return
	}

	if err := a.updateDispatchJobStatus(id, dispatchStatusFailed); err != nil {
		a.logger.Error("update dispatch job status", "status", dispatchStatusFailed, "err", err)
		return
	}
	a.logger.Error("dispatch job failed permanently", "status", dispatchStatusFailed)
}

func (a *DispatcherAPI) updateDispatchJobStatus(id uint32, status string) error {
	j, err := a.handleRetrieveJob(id)
	if err != nil {
		return err
	}

	j.Status = status
	if status == dispatchStatusProcessing {
		j.Attempts++
	}

	m, err := j.ToMap()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisConnectionTimeout)
	defer cancel()

	err = a.rdb.Watch(ctx, func(tx *redis.Tx) error {
		if err := tx.HSet(ctx, getJobKey(id), m).Err(); err != nil {
			return err
		}
		return nil
	}, getJobKey(id))

	if err != nil {
		return err
	}
	return nil
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

func (a *DispatcherAPI) getNewJobID() (uint32, error) {
	id := a.lastKey.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), redisConnectionTimeout)
	defer cancel()
	if err := a.rdb.Set(ctx, redisLastQueueKey, id, 0).Err(); err != nil {
		return 0, err
	}

	return id, nil
}

func getJobKey(id uint32) string {
	return fmt.Sprintf("%v:%v", redisJobKey, id)
}

func mapToDispatchJob(fields map[string]string) (DispatchJob, error) {
	id, err := strconv.ParseUint(fields["id"], 10, 32)
	if err != nil {
		return DispatchJob{}, fmt.Errorf("parsing job id: %w", err)
	}

	attempts, err := strconv.Atoi(fields["attempts"])
	if err != nil {
		return DispatchJob{}, fmt.Errorf("parsing job attempts: %w", err)
	}

	maxAttempts, err := strconv.Atoi(fields["max_attempts"])
	if err != nil {
		return DispatchJob{}, fmt.Errorf("parsing job max attempts: %w", err)
	}

	createdAt, err := time.Parse(time.RFC3339Nano, fields["created_at"])
	if err != nil {
		return DispatchJob{}, fmt.Errorf("parsing job created at: %w", err)
	}

	return DispatchJob{
		ID:          uint32(id),
		Channel:     fields["channel"],
		Recipient:   fields["recipient"],
		Message:     fields["message"],
		Status:      fields["status"],
		Attempts:    attempts,
		MaxAttempts: maxAttempts,
		CreatedAt:   createdAt,
	}, nil
}

func isEmailValid(email string) bool {
	return strings.Contains(email, "@")
}

func isMobileNumValid(mobile string) bool {
	return len(mobile) > 0 && strings.Contains(mobile, "+")
}

func isMessageValid(message string) bool {
	return len(message) > 0
}
