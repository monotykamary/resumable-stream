# resumable-stream

Library for wrapping streams of strings (Like for example SSE web responses) in a way that
a client can resume them after they lost a connection, or to allow a second client to follow along.

Designed for use in serverless environments without sticky load balancing.

The library relies on a pubsub mechanism and ships Redis transports by default. It was designed to
minimize latency impact and Redis usage for the common case that stream recovery is not needed.
In that common case the library performs a single `INCR` and `SUBSCRIBE` per stream. A Postgres
transport is also available when you want durable persistence across producer restarts.

## Usage

### Idempotent API

```typescript
import { createResumableStreamContext } from "resumable-stream";
import { after } from "next/server";

const streamContext = createResumableStreamContext({
  waitUntil: after,
  // Optionally pass in your own Redis publisher and subscriber
});

export async function GET(req: NextRequest, { params }: { params: Promise<{ streamId: string }> }) {
  const { streamId } = await params;
  const resumeAt = req.nextUrl.searchParams.get("resumeAt");
  const stream = await streamContext.resumableStream(
    streamId,
    makeTestStream,
    resumeAt ? parseInt(resumeAt) : undefined
  );
  if (!stream) {
    return new Response("Stream is already done", {
      status: 422,
    });
  }
  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
    },
  });
}
```

### Usage with explicit resumption

```typescript
import { createResumableStreamContext } from "resumable-stream";
import { after } from "next/server";

const streamContext = createResumableStreamContext({
  waitUntil: after,
  // Optionally pass in your own Redis publisher and subscriber
});

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ streamId: string }> }
) {
  const { streamId } = await params;
  const stream = await streamContext.createNewResumableStream(streamId, makeTestStream);
  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
    },
  });
}

export async function GET(req: NextRequest, { params }: { params: Promise<{ streamId: string }> }) {
  const { streamId } = await params;
  const resumeAt = req.nextUrl.searchParams.get("resumeAt");
  const stream = await streamContext.resumeExistingStream(
    streamId,
    resumeAt ? parseInt(resumeAt) : undefined
  );
  if (!stream) {
    return new Response("Stream is already done", {
      status: 422,
    });
  }
  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
    },
  });
}
```

### Usage with ioredis

If you are using `ioredis` instead of `redis`, you can import from `resumable-stream/ioredis` instead. This changes the default Redis client to `ioredis`.

```typescript
import { createResumableStreamContext } from "resumable-stream/ioredis";

const streamContext = createResumableStreamContext({
  waitUntil: after,
  // Optionally pass in your own Redis publisher and subscriber
});
```

### Usage with Postgres

```typescript
import { Pool } from "pg";
import { createPostgresResumableStreamContext } from "resumable-stream";

const streamContext = createPostgresResumableStreamContext({
  pool: new Pool({ connectionString: process.env.POSTGRES_URL! }),
  waitUntil: after,
  // Optional: dedicate a listener pool if you want LISTEN/NOTIFY isolation
  // listenerPool: new Pool({ connectionString: process.env.POSTGRES_URL! }),
  // keyPrefix and retentionSeconds share the same defaults as the Redis context
  // pollIntervalMs / listenTimeoutMs let you tune LISTEN fallback vs. polling
  // chunkBatchSize / chunkBatchIntervalMs let you trade latency for throughput
});

export async function GET(req: NextRequest) {
  const stream = await streamContext.resumableStream("my-stream", makeTestStream);
  return new Response(stream, {
    headers: { "Content-Type": "text/event-stream" },
  });
}
```

Unlike the Redis transport (which keeps all buffered chunks in memory), the Postgres adapter persists backlog to tables. If a producer crashes, the session status becomes `failed` instead of deleting the saved output. Followers can still drain the backlog and a future producer call will automatically reclaim the ID and start fresh:

```ts
const status = await streamContext.hasExistingStream("my-stream");
if (status === "FAILED") {
  const backlog = await streamContext.resumeExistingStream("my-stream");
  if (backlog) {
    // Send the partial transcript to the reconnecting client.
  }
}

// Whenever you're ready to restart the LLM call, just invoke resumableStream again.
const restarted = await streamContext.resumableStream("my-stream", makeStream);
```

Because Redis drops state when the producer process dies, it never reports `FAILED`. Seeing that status is specific to the Postgres transport, and simply indicates “last producer crashed but chunks are still stored.”

If your application never reuses stream IDs after a failure, you can simply start a new ID and wait for cleanup to purge the old data. But if you treat the ID as the logical conversation (the usual resumable-stream use case), just call `resumableStream` with the same ID after rerunning your LLM call; the adapter automatically clears the failed backlog and begins emitting new chunks under that identifier.

> `hasExistingStream` uses the uppercase `"FAILED"` sentinel (similar to `"DONE"`) while the database column stores `failed`.

#### Tuning throughput

Chunk writes are buffered so you can trade a small amount of latency for higher throughput:

- `chunkBatchSize` accumulates N chunks before issuing a single multi-row `INSERT` (defaults to `0`, meaning interval-only flushing).
- `chunkBatchIntervalMs` flushes partial batches after a timeout so slow producers still persist data (defaults to `5ms`).
- `maxBufferedChunks` caps the in-memory queue before producers start waiting (defaults to 1024 when `chunkBatchSize=0`, or 4× the batch size otherwise).

```ts
const fastContext = createPostgresResumableStreamContext({
  pool: new Pool({ connectionString: process.env.POSTGRES_URL! }),
  chunkBatchSize: 16,
  chunkBatchIntervalMs: 10,
  maxBufferedChunks: 128,
});
```

With batching enabled the producer issues fewer round trips, and followers still rely on the same persisted backlog.

Before running the Postgres adapter, apply the schema:

```bash
export POSTGRES_URL=postgres://user:pass@host:5432/db
npx resumable-stream-postgres-setup
```

For local development a Postgres 16 compose file is included:

```bash
docker compose up -d postgres
POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5545/resumable_stream npx resumable-stream-postgres-setup
```

Run the Postgres-focused tests (or the entire suite) with the same environment variable:

```bash
POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5545/resumable_stream pnpm vitest run src/__tests__/postgres.test.ts
POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5545/resumable_stream pnpm vitest run
```

To simulate a crash/WAL recovery with Docker, use the helper script (requires the compose setup above):

```bash
POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5545/resumable_stream pnpm tsx scripts/postgres-wal-test.ts
```

### Benchmark

Spin up the included Postgres service via Docker Compose, then run the benchmark to measure chunk throughput under different batching settings:

```bash
POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5545/resumable_stream pnpm postgres:benchmark
```

Use environment variables to tweak the scenarios:

- `POSTGRES_BENCH_BATCH_SIZES` (comma separated, defaults to `0,1,16,32` — `0` enables interval-only flushing)
- `POSTGRES_BENCH_CHUNKS` (defaults to 2000)
- `POSTGRES_BENCH_CHUNK_LENGTH` (defaults to 128 characters)
- `POSTGRES_BENCH_INTERVALS_MS` (comma separated, defaults to `5,50,100,150,200,250,300,350,400,450,500`)

The script loops over every batch-size/interval combination, applies the schema, truncates tables, and reports chunks/sec for each run.

Sample metrics (`POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5545/resumable_stream pnpm postgres:benchmark`, 2k chunks, 128-char payload):

```sh
$ POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5545/resumable_stream pnpm postgres:benchmark

> resumable-stream@2.2.8 postgres:benchmark
> tsx scripts/postgres-benchmark.ts

Batch size   0 | interval   5ms :: chunks 28169.0/sec (71 ms for 2000) | flush 25.0/sec avg 1000.0 rows (25.74 ms)
Batch size   0 | interval  50ms :: chunks 31250.0/sec (64 ms for 2000) | flush 29.2/sec avg 1000.0 rows (21.57 ms)
Batch size   0 | interval 100ms :: chunks 32258.1/sec (62 ms for 2000) | flush 30.3/sec avg 1000.0 rows (19.63 ms)
Batch size   0 | interval 150ms :: chunks 37735.8/sec (53 ms for 2000) | flush 34.5/sec avg 1000.0 rows (17.59 ms)
Batch size   0 | interval 200ms :: chunks 32786.9/sec (61 ms for 2000) | flush 30.3/sec avg 1000.0 rows (18.56 ms)
Batch size   0 | interval 250ms :: chunks 37735.8/sec (53 ms for 2000) | flush 35.2/sec avg 1000.0 rows (15.50 ms)
Batch size   0 | interval 300ms :: chunks 40816.3/sec (49 ms for 2000) | flush 37.7/sec avg 1000.0 rows (16.06 ms)
Batch size   0 | interval 350ms :: chunks 38461.5/sec (52 ms for 2000) | flush 35.4/sec avg 1000.0 rows (16.20 ms)
Batch size   0 | interval 400ms :: chunks 41666.7/sec (48 ms for 2000) | flush 37.5/sec avg 1000.0 rows (15.06 ms)
Batch size   0 | interval 450ms :: chunks 45454.5/sec (44 ms for 2000) | flush 42.0/sec avg 1000.0 rows (14.69 ms)
Batch size   0 | interval 500ms :: chunks 48780.5/sec (41 ms for 2000) | flush 45.1/sec avg 1000.0 rows (12.39 ms)
Batch size   1 | interval   5ms :: chunks 293.9/sec (6806 ms for 2000) | flush 293.7/sec avg 1.0 rows (1.57 ms)
Batch size   1 | interval  50ms :: chunks 453.1/sec (4414 ms for 2000) | flush 452.9/sec avg 1.0 rows (1.01 ms)
Batch size   1 | interval 100ms :: chunks 466.3/sec (4289 ms for 2000) | flush 466.1/sec avg 1.0 rows (0.97 ms)
Batch size   1 | interval 150ms :: chunks 458.2/sec (4365 ms for 2000) | flush 458.1/sec avg 1.0 rows (1.00 ms)
Batch size   1 | interval 200ms :: chunks 468.7/sec (4267 ms for 2000) | flush 468.5/sec avg 1.0 rows (0.99 ms)
Batch size   1 | interval 250ms :: chunks 452.4/sec (4421 ms for 2000) | flush 452.2/sec avg 1.0 rows (1.02 ms)
Batch size   1 | interval 300ms :: chunks 467.9/sec (4274 ms for 2000) | flush 467.7/sec avg 1.0 rows (0.98 ms)
Batch size   1 | interval 350ms :: chunks 483.0/sec (4141 ms for 2000) | flush 482.9/sec avg 1.0 rows (0.95 ms)
Batch size   1 | interval 400ms :: chunks 297.9/sec (6714 ms for 2000) | flush 297.8/sec avg 1.0 rows (1.57 ms)
Batch size   1 | interval 450ms :: chunks 481.7/sec (4152 ms for 2000) | flush 481.6/sec avg 1.0 rows (0.95 ms)
Batch size   1 | interval 500ms :: chunks 483.3/sec (4138 ms for 2000) | flush 483.1/sec avg 1.0 rows (0.95 ms)
Batch size  16 | interval   5ms :: chunks 6802.7/sec (294 ms for 2000) | flush 421.4/sec avg 16.0 rows (1.19 ms)
Batch size  16 | interval  50ms :: chunks 6944.4/sec (288 ms for 2000) | flush 431.8/sec avg 16.0 rows (1.13 ms)
Batch size  16 | interval 100ms :: chunks 6872.9/sec (291 ms for 2000) | flush 426.3/sec avg 16.0 rows (1.15 ms)
Batch size  16 | interval 150ms :: chunks 6825.9/sec (293 ms for 2000) | flush 424.3/sec avg 16.0 rows (1.18 ms)
Batch size  16 | interval 200ms :: chunks 6944.4/sec (288 ms for 2000) | flush 432.2/sec avg 16.0 rows (1.15 ms)
Batch size  16 | interval 250ms :: chunks 6849.3/sec (292 ms for 2000) | flush 425.2/sec avg 16.0 rows (1.15 ms)
Batch size  16 | interval 300ms :: chunks 7168.5/sec (279 ms for 2000) | flush 446.7/sec avg 16.0 rows (1.11 ms)
Batch size  16 | interval 350ms :: chunks 6779.7/sec (295 ms for 2000) | flush 421.3/sec avg 16.0 rows (1.15 ms)
Batch size  16 | interval 400ms :: chunks 6802.7/sec (294 ms for 2000) | flush 422.3/sec avg 16.0 rows (1.16 ms)
Batch size  16 | interval 450ms :: chunks 6993.0/sec (286 ms for 2000) | flush 435.0/sec avg 16.0 rows (1.13 ms)
Batch size  16 | interval 500ms :: chunks 6622.5/sec (302 ms for 2000) | flush 412.3/sec avg 16.0 rows (1.20 ms)
Batch size  32 | interval   5ms :: chunks 12195.1/sec (164 ms for 2000) | flush 382.0/sec avg 31.7 rows (1.34 ms)
Batch size  32 | interval  50ms :: chunks 12422.4/sec (161 ms for 2000) | flush 387.3/sec avg 31.7 rows (1.34 ms)
Batch size  32 | interval 100ms :: chunks 11049.7/sec (181 ms for 2000) | flush 346.0/sec avg 31.7 rows (1.49 ms)
Batch size  32 | interval 150ms :: chunks 10582.0/sec (189 ms for 2000) | flush 330.9/sec avg 31.7 rows (1.56 ms)
Batch size  32 | interval 200ms :: chunks 9615.4/sec (208 ms for 2000) | flush 298.7/sec avg 31.7 rows (1.73 ms)
Batch size  32 | interval 250ms :: chunks 9132.4/sec (219 ms for 2000) | flush 283.8/sec avg 31.7 rows (1.85 ms)
Batch size  32 | interval 300ms :: chunks 9434.0/sec (212 ms for 2000) | flush 294.2/sec avg 31.7 rows (1.79 ms)
Batch size  32 | interval 350ms :: chunks 7168.5/sec (279 ms for 2000) | flush 223.9/sec avg 31.7 rows (2.37 ms)
Batch size  32 | interval 400ms :: chunks 3571.4/sec (560 ms for 2000) | flush 112.0/sec avg 31.7 rows (4.73 ms)
Batch size  32 | interval 450ms :: chunks 5618.0/sec (356 ms for 2000) | flush 175.3/sec avg 31.7 rows (3.02 ms)
Batch size  32 | interval 500ms :: chunks 4376.4/sec (457 ms for 2000) | flush 136.6/sec avg 31.7 rows (3.95 ms)
```

When a producer crashes mid-stream, the Postgres transport marks the session as `failed` and keeps the persisted backlog available for reconnecting followers. The next successful producer call automatically resets the session and starts a fresh stream.

### Retention cleanup

Postgres never deletes old stream state automatically. Schedule the cleanup script (or run it manually) to purge rows whose `expires_at` has elapsed:

```bash
POSTGRES_URL=postgres://user:pass@host:5432/db npx resumable-stream-postgres-cleanup
```

If you override the default session table name, set `POSTGRES_SESSION_TABLE` before running the script. The chunk table uses `ON DELETE CASCADE`, so removing expired sessions removes their chunks as well.

You can also automate cleanup instead of invoking the script manually:

```sql
-- With pg_cron (https://github.com/citusdata/pg_cron)
SELECT cron.schedule(
  'rs-session-cleanup',
  '*/15 * * * *',
  $$DELETE FROM rs_stream_sessions WHERE expires_at IS NOT NULL AND expires_at < NOW();$$
);
```

```ts
// Or schedule it inside your server if you don't have pg_cron available
import { Pool } from "pg";
import { DEFAULT_SESSION_TABLE } from "resumable-stream/postgres/schema";

const pool = new Pool({ connectionString: process.env.POSTGRES_URL! });

async function cleanupExpiredSessions() {
  await pool.query(
    `DELETE FROM ${DEFAULT_SESSION_TABLE} WHERE expires_at IS NOT NULL AND expires_at < NOW()`
  );
}

setInterval(
  () => {
    cleanupExpiredSessions().catch((err) => {
      console.error("cleanup failed", err);
    });
  },
  5 * 60 * 1000
);
```

## Type Docs

[Type Docs](https://github.com/vercel/resumable-stream/blob/main/docs/README.md)

## How it works

- The first time a resumable stream is invoked for a given `streamId`, a standard stream is created.
- This is now the producer.
- The producer will always complete the stream, even if the reader of the original stream goes away.
- Additionally, the producer starts listening on the pubsub for additional consumers.
- When a second resumable stream is invoked for a given `streamId`, it publishes a messages to alert the producer that it would like to receive the stream.
- The second consumer now expects messages of stream content via the pubsub.
- The producer receives the request, and starts publishing the buffered messages and then publishes additional chunks of the stream.
