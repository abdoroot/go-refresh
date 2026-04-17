package dispatch

import "context"

type Repository interface {
	CreateJob(ctx context.Context, job DispatchJob) (uint32, error)
	GetJob(ctx context.Context, id uint32) (DispatchJob, error)
	ListJobs(ctx context.Context, limit, offset int) ([]DispatchJob, error)
	UpdateJobStatus(ctx context.Context, id uint32, status string, attempts int) error
}

type CacheRepo interface {
	Push(ctx context.Context, key string, val any) error
	Pop(ctx context.Context, key string) (uint32, error)
	Close() error
}
