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
if (status === "failed") {
  const backlog = await streamContext.resumeExistingStream("my-stream");
  if (backlog) {
    // Send the partial transcript to the reconnecting client.
  }
}

// Whenever you're ready to restart the LLM call, just invoke resumableStream again.
const restarted = await streamContext.resumableStream("my-stream", makeStream);
```

Because Redis drops state when the producer process dies, it never reports `failed`. Seeing that status is specific to the Postgres transport, and simply indicates “last producer crashed but chunks are still stored.”

If your application never reuses stream IDs after a failure, you can simply start a new ID and wait for cleanup to purge the old data. But if you treat the ID as the logical conversation (the usual resumable-stream use case), just call `resumableStream` with the same ID after rerunning your LLM call; the adapter automatically clears the failed backlog and begins emitting new chunks under that identifier.

Before running the Postgres adapter, apply the schema:

```bash
export POSTGRES_URL=postgres://user:pass@host:5432/db
pnpm postgres:setup
```

For local development a Postgres 16 compose file is included:

```bash
docker compose up -d postgres
POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5545/resumable_stream pnpm postgres:setup
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

When a producer crashes mid-stream, the Postgres transport marks the session as `failed` and keeps the persisted backlog available for reconnecting followers. The next successful producer call automatically resets the session and starts a fresh stream.

### Retention cleanup

Postgres never deletes old stream state automatically. Schedule the cleanup script (or run it manually) to purge rows whose `expires_at` has elapsed:

```bash
POSTGRES_URL=postgres://user:pass@host:5432/db pnpm postgres:cleanup
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
