import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Pool } from "pg";

import { resumableStreamTests } from "./tests";
import { createPostgresResumableStreamContext } from "../postgres";
import { DEFAULT_CHUNK_TABLE, DEFAULT_SCHEMA, DEFAULT_SESSION_TABLE } from "../postgres/schema";
import { quoteIdentifier } from "../postgres/utils";
import { createTestingStream, streamToBuffer } from "../../testing-utils/testing-stream";

const POSTGRES_URL = process.env.POSTGRES_URL;

if (!POSTGRES_URL) {
  console.error("POSTGRES_URL is not set, skipping Postgres tests");
  describe("Postgres tests", () => {
    it("should be skipped", () => {
      expect(true).toBe(true);
    });
  });
} else {
  const pool = new Pool({ connectionString: POSTGRES_URL });
  const listenerPool = new Pool({ connectionString: POSTGRES_URL });
  const chunkTable = quoteIdentifier(DEFAULT_CHUNK_TABLE);
  const sessionTable = quoteIdentifier(DEFAULT_SESSION_TABLE);

  const baseOptions = (keyPrefix = "test-postgres-resumable-" + crypto.randomUUID()) => ({
    pool,
    listenerPool,
    waitUntil: () => Promise.resolve(),
    keyPrefix,
    retentionSeconds: 60,
  });

  let context: ReturnType<typeof createPostgresResumableStreamContext> | null = null;

  beforeAll(async () => {
    await pool.query(DEFAULT_SCHEMA);
  });

  beforeEach(async () => {
    await pool.query(`TRUNCATE ${chunkTable}, ${sessionTable} RESTART IDENTITY`);
  });

  afterAll(async () => {
    await context?.close?.();
    await listenerPool.end();
    await pool.end();
  });

  resumableStreamTests(() => {
    context = createPostgresResumableStreamContext(baseOptions());
    return context;
  }, "postgres");

  describe("postgres-specific behaviors", () => {
    let localContext: ReturnType<typeof createPostgresResumableStreamContext> | null = null;

    afterEach(async () => {
      await localContext?.close?.();
      localContext = null;
    });

    it("clears persisted chunks when a stream id is reused", async () => {
      const keyPrefix = "postgres-specific-" + crypto.randomUUID();
      const streamKey = `${keyPrefix}:rs:repeat`;

      localContext = createPostgresResumableStreamContext(baseOptions(keyPrefix));
      const { readable, writer } = createTestingStream();
      const stream = await localContext.createNewResumableStream("repeat", () => readable);
      writer.write("first\n");
      writer.close();
      await streamToBuffer(stream);
      await localContext.close();

      const firstCount = await pool.query<{ count: string }>(
        `SELECT COUNT(*)::int AS count FROM ${chunkTable} WHERE stream_id = $1`,
        [streamKey]
      );
      expect(Number(firstCount.rows[0]?.count ?? 0)).toBe(1);

      localContext = createPostgresResumableStreamContext(baseOptions(keyPrefix));
      const { readable: readable2, writer: writer2 } = createTestingStream();
      const stream2 = await localContext.createNewResumableStream("repeat", () => readable2);
      writer2.write("second\n");
      writer2.close();
      await streamToBuffer(stream2);

      const secondCount = await pool.query<{ count: string }>(
        `SELECT COUNT(*)::int AS count FROM ${chunkTable} WHERE stream_id = $1`,
        [streamKey]
      );
      expect(Number(secondCount.rows[0]?.count ?? 0)).toBe(1);
    });

    it("replays persisted backlog when a new context attaches mid-stream", async () => {
      const keyPrefix = "postgres-specific-replay-" + crypto.randomUUID();
      localContext = createPostgresResumableStreamContext(baseOptions(keyPrefix));
      const { readable, writer } = createTestingStream();
      const producerStream = await localContext.resumableStream("chat", () => readable);

      writer.write("hello\n");
      const initialChunk = await streamToBuffer(producerStream, 1);
      expect(initialChunk).toEqual("hello\n");

      const followerContext = createPostgresResumableStreamContext(baseOptions(keyPrefix));
      const follower = await followerContext.resumableStream("chat", () => readable);
      writer.write("world\n");
      writer.close();
      const producerRemainder = await streamToBuffer(producerStream);
      const followerResult = await streamToBuffer(follower);
      expect(producerRemainder).toEqual("world\n");
      expect(followerResult).toEqual("hello\nworld\n");
      await followerContext.close();
    });
  });
}
