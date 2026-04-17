# go-refresh

Simple Go dispatch queue API backed by PostgreSQL and Redis.

## What it does

- Creates single and bulk dispatch jobs (`whatsapp` or `email`).
- Stores jobs in PostgreSQL.
- Pushes job IDs to a Redis queue.
- Runs background workers that process queued jobs and update status.
- Supports listing jobs with pagination.

## Tech stack

- Go 1.25
- PostgreSQL
- Redis

## Configuration

The app reads these environment variables:

- `PORT` (default: `8080`)
- `REDIS_HOST` (default: `localhost:6379`)
- `DATABASE_URL` (required)

Example:

```bash
export PORT=8080
export REDIS_HOST=localhost:6379
export DATABASE_URL=postgres://user:password@localhost:5432/go_refresh?sslmode=disable
```

## Database

Run the SQL migration files in `migrations/`:

- `000001_create_dispatch_jobs_table.up.sql`
- `000001_create_dispatch_jobs_table.down.sql`

## Run

```bash
make run
```

Or:

```bash
go run ./cmd
```

## Test

```bash
make test
```

## API

### Create dispatch job

- `POST /dispatch`

Body:

```json
{
  "channel": "email",
  "recipient": "user@example.com",
  "message": "hello"
}
```

### Create bulk dispatch jobs

- `POST /dispatch/bulk`

Body:

```json
{
  "channel": "whatsapp",
  "recipients": ["+971500000001", "+971500000002"],
  "message": "hello"
}
```

### Get one job

- `GET /dispatch/{id}`

### List jobs (paginated)

- `GET /dispatch?page=1&limit=20`
- Defaults: `page=1`, `limit=20`
- Max `limit`: `100`

## Job statuses

- `queued`
- `processing`
- `sent`
- `failed`

