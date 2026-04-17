package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/abdoroot/go-refresh/internal/dispatch"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeConn struct {
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func (f *fakeConn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return f.queryRowFn(ctx, sql, args...)
}

func (f *fakeConn) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return f.queryFn(ctx, sql, args...)
}

func (f *fakeConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return f.execFn(ctx, sql, args...)
}

type fakeRow struct {
	scanFn func(dest ...any) error
}

func (r fakeRow) Scan(dest ...any) error {
	return r.scanFn(dest...)
}

type fakeRows struct {
	items   []dispatch.DispatchJob
	idx     int
	scanErr error
	err     error
	closed  bool
}

func (r *fakeRows) Close() { r.closed = true }

func (r *fakeRows) Err() error { return r.err }

func (r *fakeRows) CommandTag() pgconn.CommandTag { return pgconn.NewCommandTag("SELECT 0") }

func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *fakeRows) Next() bool {
	if r.idx >= len(r.items) {
		return false
	}
	r.idx++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	current := r.items[r.idx-1]
	*(dest[0].(*uint32)) = current.ID
	*(dest[1].(*string)) = current.Channel
	*(dest[2].(*string)) = current.Recipient
	*(dest[3].(*string)) = current.Message
	*(dest[4].(*string)) = current.Status
	*(dest[5].(*int)) = current.Attempts
	*(dest[6].(*int)) = current.MaxAttempts
	*(dest[7].(*time.Time)) = current.CreatedAt
	return nil
}

func (r *fakeRows) Values() ([]any, error) { return nil, nil }

func (r *fakeRows) RawValues() [][]byte { return nil }

func (r *fakeRows) Conn() *pgx.Conn { return nil }

func TestCreateJob(t *testing.T) {
	d := &Dispatcher{
		conn: &fakeConn{
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{
					scanFn: func(dest ...any) error {
						*(dest[0].(*uint32)) = 42
						return nil
					},
				}
			},
			queryFn: nil,
			execFn:  nil,
		},
	}

	id, err := d.CreateJob(context.Background(), dispatch.DispatchJob{Channel: "email", Recipient: "a@b.com", Message: "x"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != 42 {
		t.Fatalf("expected id 42, got %d", id)
	}

	d = &Dispatcher{
		conn: &fakeConn{
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFn: func(dest ...any) error { return errors.New("scan") }}
			},
			queryFn: nil,
			execFn:  nil,
		},
	}
	if _, err := d.CreateJob(context.Background(), dispatch.DispatchJob{}); err == nil {
		t.Fatalf("expected scan error")
	}
}

func TestGetJob(t *testing.T) {
	now := time.Now()
	d := &Dispatcher{
		conn: &fakeConn{
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{
					scanFn: func(dest ...any) error {
						*(dest[0].(*uint32)) = 10
						*(dest[1].(*string)) = "email"
						*(dest[2].(*string)) = "a@b.com"
						*(dest[3].(*string)) = "msg"
						*(dest[4].(*string)) = "queued"
						*(dest[5].(*int)) = 1
						*(dest[6].(*int)) = 3
						*(dest[7].(*time.Time)) = now
						return nil
					},
				}
			},
			queryFn: nil,
			execFn:  nil,
		},
	}

	job, err := d.GetJob(context.Background(), 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if job.ID != 10 || job.Channel != "email" {
		t.Fatalf("unexpected job: %#v", job)
	}

	d = &Dispatcher{
		conn: &fakeConn{
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFn: func(dest ...any) error { return errors.New("missing") }}
			},
			queryFn: nil,
			execFn:  nil,
		},
	}
	if _, err := d.GetJob(context.Background(), 99); err == nil {
		t.Fatalf("expected get error")
	}
}

func TestListJobs(t *testing.T) {
	now := time.Now()
	rows := &fakeRows{
		items: []dispatch.DispatchJob{
			{ID: 2, Channel: "email", Recipient: "b@b.com", Message: "x", Status: "queued", Attempts: 0, MaxAttempts: 3, CreatedAt: now},
			{ID: 1, Channel: "whatsapp", Recipient: "+9715", Message: "y", Status: "sent", Attempts: 1, MaxAttempts: 3, CreatedAt: now},
		},
	}
	d := &Dispatcher{
		conn: &fakeConn{
			queryRowFn: nil,
			queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return rows, nil
			},
			execFn: nil,
		},
	}

	jobs, err := d.ListJobs(context.Background(), 20, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(jobs) != 2 || jobs[0].ID != 2 || jobs[1].ID != 1 {
		t.Fatalf("unexpected jobs: %#v", jobs)
	}
	if !rows.closed {
		t.Fatalf("expected rows to be closed")
	}

	d = &Dispatcher{
		conn: &fakeConn{
			queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return nil, errors.New("query error")
			},
		},
	}
	if _, err := d.ListJobs(context.Background(), 20, 0); err == nil {
		t.Fatalf("expected query error")
	}

	d = &Dispatcher{
		conn: &fakeConn{
			queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return &fakeRows{items: []dispatch.DispatchJob{{ID: 1}}, scanErr: errors.New("scan error")}, nil
			},
		},
	}
	if _, err := d.ListJobs(context.Background(), 20, 0); err == nil {
		t.Fatalf("expected scan error")
	}

	d = &Dispatcher{
		conn: &fakeConn{
			queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return &fakeRows{items: []dispatch.DispatchJob{{ID: 1}}, err: errors.New("rows err")}, nil
			},
		},
	}
	if _, err := d.ListJobs(context.Background(), 20, 0); err == nil {
		t.Fatalf("expected rows err")
	}
}

func TestUpdateJobStatus(t *testing.T) {
	d := &Dispatcher{
		conn: &fakeConn{
			execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
				return pgconn.NewCommandTag("UPDATE 1"), nil
			},
		},
	}
	if err := d.UpdateJobStatus(context.Background(), 1, "sent", 1); err != nil {
		t.Fatalf("unexpected update error: %v", err)
	}

	d = &Dispatcher{
		conn: &fakeConn{
			execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
				return pgconn.CommandTag{}, errors.New("exec")
			},
		},
	}
	if err := d.UpdateJobStatus(context.Background(), 1, "sent", 1); err == nil {
		t.Fatalf("expected exec error")
	}
}
