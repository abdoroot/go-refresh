package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/abdoroot/go-refresh/internal/dispatch"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type dbConn interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type Dispatcher struct {
	conn dbConn
}

func NewDispatcherRepo(conn *pgxpool.Pool) *Dispatcher {
	return &Dispatcher{
		conn: conn,
	}
}

func (r *Dispatcher) CreateJob(ctx context.Context, job dispatch.DispatchJob) (uint32, error) {
	var id uint32
	query := `
		INSERT INTO dispatch_jobs
		(channel, recipient, message, status, attempts, max_attempts, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id;
	`

	row := r.conn.QueryRow(
		ctx,
		query,
		job.Channel,
		job.Recipient,
		job.Message,
		job.Status,
		job.Attempts,
		job.MaxAttempts,
		time.Now(),
	)

	if err := row.Scan(&id); err != nil {
		return 0, err
	}

	return id, nil
}

func (r *Dispatcher) GetJob(ctx context.Context, id uint32) (dispatch.DispatchJob, error) {
	query := `
		SELECT id, channel, recipient, message, status,
		       attempts, max_attempts, created_at
		FROM dispatch_jobs
		WHERE id = $1
	`

	job := dispatch.DispatchJob{}

	err := r.conn.QueryRow(ctx, query, id).Scan(
		&job.ID,
		&job.Channel,
		&job.Recipient,
		&job.Message,
		&job.Status,
		&job.Attempts,
		&job.MaxAttempts,
		&job.CreatedAt,
	)
	if err != nil {
		return dispatch.DispatchJob{}, fmt.Errorf("get job %d: %w", id, err)
	}

	return job, nil
}

func (r *Dispatcher) ListJobs(ctx context.Context, limit, offset int) ([]dispatch.DispatchJob, error) {
	query := `
		SELECT id, channel, recipient, message, status,
		       attempts, max_attempts, created_at
		FROM dispatch_jobs
		ORDER BY id DESC
		LIMIT $1 OFFSET $2
	`

	jobs := []dispatch.DispatchJob{}

	rows, err := r.conn.Query(ctx, query, limit, offset)
	if err != nil {
		return jobs, err
	}

	defer rows.Close()

	for rows.Next() {
		var job dispatch.DispatchJob
		err := rows.Scan(
			&job.ID,
			&job.Channel,
			&job.Recipient,
			&job.Message,
			&job.Status,
			&job.Attempts,
			&job.MaxAttempts,
			&job.CreatedAt,
		)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return jobs, nil
}

func (r *Dispatcher) UpdateJobStatus(ctx context.Context, id uint32, status string, attempts int) error {
	query := `
		UPDATE dispatch_jobs SET status = $1,attempts=attempts+$2 where id = $3`

	_, err := r.conn.Exec(
		ctx,
		query,
		status,
		attempts,
		id,
	)

	return err
}
