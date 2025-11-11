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
