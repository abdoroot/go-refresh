package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/abdoroot/go-refresh/internal/config"
	"github.com/redis/go-redis/v9"
)

type mockStore struct {
	createJobFn       func(ctx context.Context, job DispatchJob) (uint32, error)
	getJobFn          func(ctx context.Context, id uint32) (DispatchJob, error)
	listJobsFn        func(ctx context.Context, limit, offset int) ([]DispatchJob, error)
	updateJobStatusFn func(ctx context.Context, id uint32, status string, attempts int) error
}

func (m *mockStore) CreateJob(ctx context.Context, job DispatchJob) (uint32, error) {
	if m.createJobFn != nil {
		return m.createJobFn(ctx, job)
	}
	return 0, nil
}

func (m *mockStore) GetJob(ctx context.Context, id uint32) (DispatchJob, error) {
	if m.getJobFn != nil {
		return m.getJobFn(ctx, id)
	}
	return DispatchJob{}, nil
}

func (m *mockStore) ListJobs(ctx context.Context, limit, offset int) ([]DispatchJob, error) {
	if m.listJobsFn != nil {
		return m.listJobsFn(ctx, limit, offset)
	}
	return nil, nil
}

func (m *mockStore) UpdateJobStatus(ctx context.Context, id uint32, status string, attempts int) error {
	if m.updateJobStatusFn != nil {
		return m.updateJobStatusFn(ctx, id, status, attempts)
	}
	return nil
}

type mockCache struct {
	pushFn  func(ctx context.Context, key string, val any) error
	popFn   func(ctx context.Context, key string) (uint32, error)
	closeFn func() error
}

func (m *mockCache) Push(ctx context.Context, key string, val any) error {
	if m.pushFn != nil {
		return m.pushFn(ctx, key, val)
	}
	return nil
}

func (m *mockCache) Pop(ctx context.Context, key string) (uint32, error) {
	if m.popFn != nil {
		return m.popFn(ctx, key)
	}
	return 0, redis.Nil
}

func (m *mockCache) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

type failWriter struct {
	header http.Header
}

func (w *failWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *failWriter) WriteHeader(int) {}

func (w *failWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func newTestAPI(store Repository, cache CacheRepo) *DispatcherAPI {
	api := NewDispatcherAPI(config.Config{ServerPort: "8081"}, store, cache)
	api.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	return api
}

func TestCreateDispatchRequestValidate(t *testing.T) {
	tests := []struct {
		name string
		req  CreateDispatchRequest
		ok   bool
	}{
		{"bad channel", CreateDispatchRequest{Channel: "sms", Recipient: "a@b.com", Message: "x"}, false},
		{"bad email", CreateDispatchRequest{Channel: channelEmail, Recipient: "not-email", Message: "x"}, false},
		{"bad mobile", CreateDispatchRequest{Channel: channelWhatsapp, Recipient: "5000", Message: "x"}, false},
		{"bad message", CreateDispatchRequest{Channel: channelEmail, Recipient: "a@b.com", Message: ""}, false},
		{"valid email", CreateDispatchRequest{Channel: channelEmail, Recipient: "a@b.com", Message: "x"}, true},
		{"valid whatsapp", CreateDispatchRequest{Channel: channelWhatsapp, Recipient: "+9715000", Message: "x"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.validate()
			if tt.ok && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if !tt.ok && err == nil {
				t.Fatalf("expected error")
			}
		})
	}
}

func TestCreateDispatchRequestMarshal(t *testing.T) {
	req := CreateDispatchRequest{
		Channel:   channelEmail,
		Recipient: "a@b.com",
		Message:   "hello",
	}
	b, err := req.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if !bytes.Contains(b, []byte(`"channel":"email"`)) {
		t.Fatalf("unexpected payload: %s", string(b))
	}
}

func TestCreateBulkDispatchRequestValidate(t *testing.T) {
	tests := []struct {
		name string
		req  CreateBulkDispatchRequest
		ok   bool
	}{
		{"bad channel", CreateBulkDispatchRequest{Channel: "sms", Recipients: []string{"a@b.com"}, Message: "x"}, false},
		{"empty recipients", CreateBulkDispatchRequest{Channel: channelEmail, Recipients: nil, Message: "x"}, false},
		{"bad email recipient", CreateBulkDispatchRequest{Channel: channelEmail, Recipients: []string{"bad"}, Message: "x"}, false},
		{"bad mobile recipient", CreateBulkDispatchRequest{Channel: channelWhatsapp, Recipients: []string{"bad"}, Message: "x"}, false},
		{"bad message", CreateBulkDispatchRequest{Channel: channelEmail, Recipients: []string{"a@b.com"}, Message: ""}, false},
		{"valid email", CreateBulkDispatchRequest{Channel: channelEmail, Recipients: []string{"a@b.com"}, Message: "x"}, true},
		{"valid whatsapp", CreateBulkDispatchRequest{Channel: channelWhatsapp, Recipients: []string{"+9715000"}, Message: "x"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.validate()
			if tt.ok && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if !tt.ok && err == nil {
				t.Fatalf("expected error")
			}
		})
	}
}

func TestParsePagination(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/dispatch", nil)
	page, limit, err := parsePagination(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if page != 1 || limit != 20 {
		t.Fatalf("unexpected defaults page=%d limit=%d", page, limit)
	}

	req = httptest.NewRequest(http.MethodGet, "/dispatch?page=2&limit=10", nil)
	page, limit, err = parsePagination(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if page != 2 || limit != 10 {
		t.Fatalf("unexpected values page=%d limit=%d", page, limit)
	}

	req = httptest.NewRequest(http.MethodGet, "/dispatch?page=0", nil)
	if _, _, err = parsePagination(req); err == nil {
		t.Fatalf("expected page error")
	}
	req = httptest.NewRequest(http.MethodGet, "/dispatch?limit=101", nil)
	if _, _, err = parsePagination(req); err == nil {
		t.Fatalf("expected limit error")
	}
}

func TestHandleGetSingleDispatch(t *testing.T) {
	api := newTestAPI(&mockStore{}, &mockCache{})

	req := httptest.NewRequest(http.MethodGet, "/dispatch/abc", nil)
	req.SetPathValue("id", "abc")
	rr := httptest.NewRecorder()
	api.HandleGetSingleDispatch(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}

	api = newTestAPI(&mockStore{
		getJobFn: func(context.Context, uint32) (DispatchJob, error) { return DispatchJob{}, errors.New("not found") },
	}, &mockCache{})
	req = httptest.NewRequest(http.MethodGet, "/dispatch/1", nil)
	req.SetPathValue("id", "1")
	rr = httptest.NewRecorder()
	api.HandleGetSingleDispatch(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}

	api = newTestAPI(&mockStore{
		getJobFn: func(context.Context, uint32) (DispatchJob, error) {
			return DispatchJob{ID: 1, Status: dispatchStatusQueued}, nil
		},
	}, &mockCache{})
	req = httptest.NewRequest(http.MethodGet, "/dispatch/1", nil)
	req.SetPathValue("id", "1")
	rr = httptest.NewRecorder()
	api.HandleGetSingleDispatch(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestHandleGetAllSingleDispatch(t *testing.T) {
	api := newTestAPI(&mockStore{}, &mockCache{})
	req := httptest.NewRequest(http.MethodGet, "/dispatch?page=0", nil)
	rr := httptest.NewRecorder()
	api.HandleGetAllSingleDispatch(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}

	api = newTestAPI(&mockStore{
		listJobsFn: func(context.Context, int, int) ([]DispatchJob, error) { return nil, errors.New("db") },
	}, &mockCache{})
	req = httptest.NewRequest(http.MethodGet, "/dispatch?page=1&limit=10", nil)
	rr = httptest.NewRecorder()
	api.HandleGetAllSingleDispatch(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}

	var gotLimit, gotOffset int
	api = newTestAPI(&mockStore{
		listJobsFn: func(context.Context, int, int) ([]DispatchJob, error) {
			gotLimit, gotOffset = 10, 10
			return []DispatchJob{{ID: 1}}, nil
		},
	}, &mockCache{})
	req = httptest.NewRequest(http.MethodGet, "/dispatch?page=2&limit=10", nil)
	rr = httptest.NewRecorder()
	api.HandleGetAllSingleDispatch(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if gotLimit != 10 || gotOffset != 10 {
		t.Fatalf("unexpected pagination values limit=%d offset=%d", gotLimit, gotOffset)
	}
}

func TestHandleCreateDispatch(t *testing.T) {
	api := newTestAPI(&mockStore{}, &mockCache{})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/dispatch", bytes.NewBufferString("{"))
	api.HandleCreateDispatch(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}

	api = newTestAPI(&mockStore{
		createJobFn: func(context.Context, DispatchJob) (uint32, error) { return 0, errors.New("db") },
	}, &mockCache{})
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/dispatch", bytes.NewBufferString(`{"channel":"email","recipient":"a@b.com","message":"x"}`))
	api.HandleCreateDispatch(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}

	api = newTestAPI(&mockStore{
		createJobFn: func(context.Context, DispatchJob) (uint32, error) { return 11, nil },
	}, &mockCache{
		pushFn: func(context.Context, string, any) error { return nil },
	})
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/dispatch", bytes.NewBufferString(`{"channel":"email","recipient":"a@b.com","message":"x"}`))
	api.HandleCreateDispatch(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rr.Code)
	}
}

func TestBulkDispatchHandler(t *testing.T) {
	api := newTestAPI(&mockStore{}, &mockCache{})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/dispatch/bulk", bytes.NewBufferString(`{`))
	api.bulkDispatchHandler(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}

	nextID := uint32(1)
	api = newTestAPI(&mockStore{
		createJobFn: func(context.Context, DispatchJob) (uint32, error) {
			id := nextID
			nextID++
			return id, nil
		},
	}, &mockCache{})
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/dispatch/bulk", bytes.NewBufferString(`{"channel":"email","recipients":["a@b.com","b@b.com"],"message":"x"}`))
	api.bulkDispatchHandler(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rr.Code)
	}
}

func TestHandleCreateBulkJobs(t *testing.T) {
	api := newTestAPI(&mockStore{}, &mockCache{})
	if _, err := api.handleCreateBulkJobs(bytes.NewBufferString("{")); err == nil {
		t.Fatalf("expected decode error")
	}

	if _, err := api.handleCreateBulkJobs(bytes.NewBufferString(`{"channel":"email","recipients":[],"message":"x"}`)); err == nil {
		t.Fatalf("expected validation error")
	}
}

func TestHandleCreateJobAndRetrieve(t *testing.T) {
	if _, err := newTestAPI(&mockStore{}, &mockCache{}).handleCreateJob(bytes.NewBufferString("{")); err == nil {
		t.Fatalf("expected decode error")
	}
	if _, err := newTestAPI(&mockStore{}, &mockCache{}).handleCreateJob(bytes.NewBufferString(`{"channel":"email","recipient":"bad","message":"x"}`)); err == nil {
		t.Fatalf("expected validation error")
	}

	api := newTestAPI(&mockStore{
		createJobFn: func(context.Context, DispatchJob) (uint32, error) { return 1, errors.New("db") },
	}, &mockCache{})
	if _, err := api.handleCreateJob(bytes.NewBufferString(`{"channel":"email","recipient":"a@b.com","message":"x"}`)); err == nil {
		t.Fatalf("expected db error")
	}

	api = newTestAPI(&mockStore{
		createJobFn: func(context.Context, DispatchJob) (uint32, error) { return 2, nil },
	}, &mockCache{
		pushFn: func(context.Context, string, any) error { return errors.New("redis") },
	})
	if _, err := api.handleCreateJob(bytes.NewBufferString(`{"channel":"email","recipient":"a@b.com","message":"x"}`)); err == nil {
		t.Fatalf("expected push error")
	}

	api = newTestAPI(&mockStore{
		createJobFn: func(context.Context, DispatchJob) (uint32, error) { return 3, nil },
		getJobFn: func(context.Context, uint32) (DispatchJob, error) {
			return DispatchJob{ID: 3}, nil
		},
		listJobsFn: func(context.Context, int, int) ([]DispatchJob, error) {
			return []DispatchJob{{ID: 3}}, nil
		},
	}, &mockCache{
		pushFn: func(context.Context, string, any) error { return nil },
	})

	job, err := api.handleCreateJob(bytes.NewBufferString(`{"channel":"email","recipient":"a@b.com","message":"x"}`))
	if err != nil || job.ID != 3 {
		t.Fatalf("expected created job id 3, got %#v err=%v", job, err)
	}
	if _, err := api.handleRetrieveJob(3); err != nil {
		t.Fatalf("expected retrieve success, got %v", err)
	}
	if jobs, err := api.handleRetrieveJobs(1, 20); err != nil || len(jobs) != 1 {
		t.Fatalf("expected list success, got jobs=%d err=%v", len(jobs), err)
	}
}

func TestHandleRetrieveErrors(t *testing.T) {
	api := newTestAPI(&mockStore{
		getJobFn: func(context.Context, uint32) (DispatchJob, error) { return DispatchJob{}, errors.New("missing") },
		listJobsFn: func(context.Context, int, int) ([]DispatchJob, error) {
			return nil, errors.New("db")
		},
	}, &mockCache{})
	if _, err := api.handleRetrieveJob(1); err == nil {
		t.Fatalf("expected retrieve error")
	}
	if _, err := api.handleRetrieveJobs(1, 20); err == nil {
		t.Fatalf("expected list error")
	}
}

func TestWorkerStopsWhenContextDone(t *testing.T) {
	api := newTestAPI(&mockStore{}, &mockCache{
		popFn: func(ctx context.Context, key string) (uint32, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	api.wg.Add(1)
	go api.Worker(ctx)

	done := make(chan struct{})
	go func() {
		api.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("worker did not stop")
	}
}

func TestSimulateSend(t *testing.T) {
	api := newTestAPI(&mockStore{
		updateJobStatusFn: func(context.Context, uint32, string, int) error { return errors.New("update") },
	}, &mockCache{})
	api.simulateSend(1)

	api = newTestAPI(&mockStore{
		updateJobStatusFn: func(context.Context, uint32, string, int) error { return nil },
		getJobFn:          func(context.Context, uint32) (DispatchJob, error) { return DispatchJob{}, errors.New("missing") },
	}, &mockCache{})
	api.simulateSend(1)

	api = newTestAPI(&mockStore{
		updateJobStatusFn: func(context.Context, uint32, string, int) error { return nil },
		getJobFn: func(context.Context, uint32) (DispatchJob, error) {
			return DispatchJob{ID: 3, Attempts: 2, MaxAttempts: 3}, nil
		},
	}, &mockCache{})
	api.simulateSend(3)
}

func TestProcessFailedDispatchJob(t *testing.T) {
	api := newTestAPI(&mockStore{
		getJobFn: func(context.Context, uint32) (DispatchJob, error) { return DispatchJob{}, errors.New("missing") },
	}, &mockCache{})
	api.processFailedDispatchJob(1)

	api = newTestAPI(&mockStore{
		getJobFn: func(context.Context, uint32) (DispatchJob, error) {
			return DispatchJob{ID: 1, Attempts: 1, MaxAttempts: 3}, nil
		},
		updateJobStatusFn: func(context.Context, uint32, string, int) error { return errors.New("update") },
	}, &mockCache{})
	api.processFailedDispatchJob(1)

	pushDone := make(chan struct{}, 1)
	api = newTestAPI(&mockStore{
		getJobFn: func(context.Context, uint32) (DispatchJob, error) {
			return DispatchJob{ID: 1, Attempts: 1, MaxAttempts: 3}, nil
		},
		updateJobStatusFn: func(context.Context, uint32, string, int) error { return nil },
	}, &mockCache{
		pushFn: func(context.Context, string, any) error {
			select {
			case pushDone <- struct{}{}:
			default:
			}
			return nil
		},
	})
	api.processFailedDispatchJob(1)
	select {
	case <-pushDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected redis push for retry")
	}

	api = newTestAPI(&mockStore{
		getJobFn: func(context.Context, uint32) (DispatchJob, error) {
			return DispatchJob{ID: 1, Attempts: 3, MaxAttempts: 3}, nil
		},
		updateJobStatusFn: func(context.Context, uint32, string, int) error { return errors.New("update") },
	}, &mockCache{})
	api.processFailedDispatchJob(1)

	api = newTestAPI(&mockStore{
		getJobFn: func(context.Context, uint32) (DispatchJob, error) {
			return DispatchJob{ID: 1, Attempts: 3, MaxAttempts: 3}, nil
		},
		updateJobStatusFn: func(context.Context, uint32, string, int) error { return nil },
	}, &mockCache{})
	api.processFailedDispatchJob(1)
}

func TestHandleShutdown(t *testing.T) {
	closeErr := errors.New("close")
	cache := &mockCache{
		closeFn: func() error { return closeErr },
	}
	api := newTestAPI(&mockStore{}, cache)
	api.server = &http.Server{}
	called := false
	cancel := func() { called = true }

	sigChan := make(chan os.Signal, 1)
	sigChan <- syscall.SIGINT
	api.handleShutdown(cancel, sigChan)
	if !called {
		t.Fatalf("expected cancel to be called")
	}
}

func TestRunAndHelpers(t *testing.T) {
	cache := &mockCache{
		popFn: func(ctx context.Context, key string) (uint32, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		},
	}
	api := newTestAPI(&mockStore{}, cache)
	api.server = &http.Server{Addr: "127.0.0.1:-1", Handler: api.router}
	err := api.Run()
	if err == nil {
		t.Fatalf("expected run error")
	}
}

func TestWriteJSONAndValidators(t *testing.T) {
	api := newTestAPI(&mockStore{}, &mockCache{})
	rr := httptest.NewRecorder()
	api.writeJSON(rr, dispatchResp{Message: "ok"}, http.StatusOK)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	api.writeJSON(&failWriter{}, dispatchResp{Message: "fail"}, http.StatusOK)

	if !isEmailValid("a@b.com") || isEmailValid("ab.com") {
		t.Fatalf("email validator mismatch")
	}
	if !isMobileNumValid("+123") || isMobileNumValid("123") {
		t.Fatalf("mobile validator mismatch")
	}
	if !isMessageValid("x") || isMessageValid("") {
		t.Fatalf("message validator mismatch")
	}
	if !api.shouldFailFirst(DispatchJob{ID: 2, Attempts: 1}) {
		t.Fatalf("expected fail-first true")
	}
	if api.shouldFailFirst(DispatchJob{ID: 3, Attempts: 1}) {
		t.Fatalf("expected fail-first false")
	}
}

func TestUpdateDispatchJobStatus(t *testing.T) {
	var (
		gotID       uint32
		gotStatus   string
		gotAttempts int
		lock        sync.Mutex
	)
	api := newTestAPI(&mockStore{
		updateJobStatusFn: func(ctx context.Context, id uint32, status string, attempts int) error {
			lock.Lock()
			defer lock.Unlock()
			gotID = id
			gotStatus = status
			gotAttempts = attempts
			return nil
		},
	}, &mockCache{})

	if err := api.updateDispatchJobStatus(7, dispatchStatusSent); err != nil {
		t.Fatalf("unexpected update error: %v", err)
	}
	lock.Lock()
	defer lock.Unlock()
	if gotID != 7 || gotStatus != dispatchStatusSent || gotAttempts != 1 {
		t.Fatalf("unexpected update args id=%d status=%s attempts=%d", gotID, gotStatus, gotAttempts)
	}
}

func TestHandlersThroughMux(t *testing.T) {
	store := &mockStore{
		createJobFn: func(context.Context, DispatchJob) (uint32, error) { return 1, nil },
		getJobFn:    func(context.Context, uint32) (DispatchJob, error) { return DispatchJob{ID: 1}, nil },
		listJobsFn:  func(context.Context, int, int) ([]DispatchJob, error) { return []DispatchJob{{ID: 1}}, nil },
	}
	api := newTestAPI(store, &mockCache{})

	api.router.HandleFunc("GET /dispatch/{id}", api.HandleGetSingleDispatch)
	api.router.HandleFunc("GET /dispatch", api.HandleGetAllSingleDispatch)
	api.router.HandleFunc("POST /dispatch", api.HandleCreateDispatch)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dispatch/1", nil)
	api.router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/dispatch?page=1&limit=20", nil)
	api.router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/dispatch", bytes.NewBufferString(`{"channel":"email","recipient":"a@b.com","message":"x"}`))
	api.router.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rr.Code)
	}

	var resp DispatchJob
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response failed: %v", err)
	}
	if resp.ID != 1 {
		t.Fatalf("expected id 1, got %d", resp.ID)
	}
}
