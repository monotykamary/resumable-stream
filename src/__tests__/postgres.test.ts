import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Pool } from "pg";

import { resumableStreamTests } from "./tests";
import { createPostgresResumableStreamContext } from "../postgres";
import { DEFAULT_CHUNK_TABLE, DEFAULT_SCHEMA, DEFAULT_SESSION_TABLE } from "../postgres/schema";
import { quoteIdentifier } from "../postgres/utils";
import type { PostgresPoolLike } from "../postgres/types";
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
  function silenceAdminTermination(p: Pool) {
    p.on("error", (error) => {
      if ((error as { code?: string })?.code === "57P01") {
        return;
      }
      console.error(error);
    });
  }

  const pool = new Pool({ connectionString: POSTGRES_URL });
  silenceAdminTermination(pool);
  const listenerPool = new Pool({ connectionString: POSTGRES_URL });
  silenceAdminTermination(listenerPool);
  const chunkTable = quoteIdentifier(DEFAULT_CHUNK_TABLE);
  const sessionTable = quoteIdentifier(DEFAULT_SESSION_TABLE);

  const baseOptions = (keyPrefix = "test-postgres-resumable-" + crypto.randomUUID()) => ({
    pool,
    listenerPool,
    waitUntil: () => Promise.resolve(),
    keyPrefix,
    retentionSeconds: 60,
    pollIntervalMs: 25,
    listenTimeoutMs: 200,
  });

  let context: ReturnType<typeof createPostgresResumableStreamContext> | null = null;

  const closePool = async (client: Pool) => {
    await Promise.race([
      client.end(),
      new Promise((resolve) => setTimeout(resolve, 5000)),
    ]);
  };

  beforeAll(async () => {
    await pool.query(DEFAULT_SCHEMA);
  });

  beforeEach(async () => {
    await pool.query(`TRUNCATE ${chunkTable}, ${sessionTable} RESTART IDENTITY`);
  });

  afterAll(async () => {
    await context?.close?.();
    await closePool(listenerPool);
    await closePool(pool);
  }, 20000);

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

    it("delivers chunks to multiple concurrent followers", async () => {
      const keyPrefix = "postgres-concurrent-" + crypto.randomUUID();
      localContext = createPostgresResumableStreamContext(baseOptions(keyPrefix));
      const { readable, writer } = createTestingStream();
      const producer = await localContext.resumableStream("group", () => readable);

      const follower1Promise = localContext.resumableStream("group", () => readable);
      const follower2Promise = localContext.resumableStream("group", () => readable);

      writer.write("1\n");
      writer.write("2\n");
      writer.close();

      const follower1Stream = await follower1Promise;
      const follower2Stream = await follower2Promise;

      const [producerResult, follower1Result, follower2Result] = await Promise.all([
        streamToBuffer(producer),
        streamToBuffer(follower1Stream),
        streamToBuffer(follower2Stream),
      ]);

      expect(producerResult).toEqual("1\n2\n");
      expect(follower1Result).toEqual("1\n2\n");
      expect(follower2Result).toEqual("1\n2\n");
    });

    it("continues persisting after the original consumer cancels", async () => {
      const keyPrefix = "postgres-producer-cancel-" + crypto.randomUUID();
      localContext = createPostgresResumableStreamContext(baseOptions(keyPrefix));
      const { readable, writer } = createTestingStream();
      const producer = await localContext.resumableStream("linger", () => readable);
      const followerPromise = localContext.resumableStream("linger", () => readable);

      writer.write("hello\n");
      const reader = producer.getReader();
      await reader.read();
      await reader.cancel();

      writer.write("world\n");
      writer.close();

      const follower = await followerPromise;
      const followerResult = await streamToBuffer(follower);
      expect(followerResult).toEqual("hello\nworld\n");
      expect(await localContext.hasExistingStream("linger")).toBe("DONE");
    });

    it("falls back to polling when LISTEN is unavailable", async () => {
      const keyPrefix = "postgres-polling-" + crypto.randomUUID();
      const listenerStub: PostgresPoolLike = {
        query: async () => {
          throw new Error("notify disabled");
        },
        connect: async () => ({
          query: async () => {
            throw new Error("listen disabled");
          },
          release: async () => {},
        }),
      };

      localContext = createPostgresResumableStreamContext({
        ...baseOptions(keyPrefix),
        listenerPool: listenerStub,
      });
      const { readable, writer } = createTestingStream();
      const producer = await localContext.resumableStream("poll", () => readable);
      const followerPromise = localContext.resumableStream("poll", () => readable);

      writer.write("chunk\n");
      writer.close();

      const follower = await followerPromise;
      const [producerResult, followerResult] = await Promise.all([
        streamToBuffer(producer),
        streamToBuffer(follower),
      ]);

      expect(producerResult).toEqual("chunk\n");
      expect(followerResult).toEqual("chunk\n");
    });

    it("falls back to polling when LISTEN connection fails", async () => {
      const keyPrefix = "postgres-polling-fail-" + crypto.randomUUID();
      const listenerStub: PostgresPoolLike = {
        query: async () => {
          throw new Error("notify disabled");
        },
        connect: async () => {
          throw new Error("listen connect failed");
        },
      };

      localContext = createPostgresResumableStreamContext({
        ...baseOptions(keyPrefix),
        listenerPool: listenerStub,
      });
      const { readable, writer } = createTestingStream();
      const producer = await localContext.resumableStream("poll-fail", () => readable);
      const followerPromise = localContext.resumableStream("poll-fail", () => readable);

      writer.write("chunk\n");
      writer.close();

      const follower = await followerPromise;
      const [producerResult, followerResult] = await Promise.all([
        streamToBuffer(producer),
        streamToBuffer(follower),
      ]);

      expect(producerResult).toEqual("chunk\n");
      expect(followerResult).toEqual("chunk\n");
    });

    it("streams hundreds of chunks without loss", async () => {
      const keyPrefix = "postgres-bulk-" + crypto.randomUUID();
      localContext = createPostgresResumableStreamContext(baseOptions(keyPrefix));
      const { readable, writer } = createTestingStream();
      const producer = await localContext.resumableStream("bulk", () => readable);
      const follower = await localContext.resumableStream("bulk", () => readable);
      for (let i = 0; i < 200; i++) {
        writer.write(`line-${i}\n`);
      }
      writer.close();
      const [producerResult, followerResult] = await Promise.all([
        streamToBuffer(producer),
        streamToBuffer(follower),
      ]);
      expect(producerResult).toEqual(followerResult);
      expect(followerResult.split("\n").filter(Boolean).length).toBe(200);
    });

    it("runs makeStream once even with simultaneous producer calls", async () => {
      const keyPrefix = "postgres-single-producer-" + crypto.randomUUID();
      const ctxA = createPostgresResumableStreamContext(baseOptions(keyPrefix));
      const ctxB = createPostgresResumableStreamContext(baseOptions(keyPrefix));
      const { readable, writer } = createTestingStream();
      let invocations = 0;
      const streamFactory = () => {
        invocations++;
        return readable;
      };
      const [streamA, streamB] = await Promise.all([
        ctxA.resumableStream("race", streamFactory),
        ctxB.resumableStream("race", streamFactory),
      ]);
      writer.write("winning\n");
      writer.close();
      const [resultA, resultB] = await Promise.all([streamToBuffer(streamA), streamToBuffer(streamB)]);
      expect(resultA).toEqual("winning\n");
      expect(resultB).toEqual("winning\n");
      expect(invocations).toBe(1);
      await ctxA.close();
      await ctxB.close();
    });

    it("supports explicit retention cleanup", async () => {
      const keyPrefix = "postgres-retention-" + crypto.randomUUID();
      const streamId = "retained";
      const namespacedId = `${keyPrefix}:rs:${streamId}`;
      localContext = createPostgresResumableStreamContext({
        ...baseOptions(keyPrefix),
        retentionSeconds: 1,
      });

      const { readable, writer } = createTestingStream();
      const producer = await localContext.resumableStream(streamId, () => readable);
      writer.write("ttl\n");
      writer.close();
      await streamToBuffer(producer);

      await pool.query(
        `UPDATE ${sessionTable} SET expires_at = NOW() - INTERVAL '1 minute' WHERE stream_id = $1`,
        [namespacedId]
      );
      await pool.query(`DELETE FROM ${chunkTable} WHERE stream_id = $1`, [namespacedId]);
      await pool.query(`DELETE FROM ${sessionTable} WHERE expires_at < NOW()`);

      expect(await localContext.hasExistingStream(streamId)).toBeNull();
    });

    it("resumes mid-chunk using skipCharacters", async () => {
      const keyPrefix = "postgres-skip-" + crypto.randomUUID();
      localContext = createPostgresResumableStreamContext(baseOptions(keyPrefix));
      const { readable, writer } = createTestingStream();
      const producer = await localContext.resumableStream("chunky", () => readable);
      writer.write("abcdef");
      const followerPromise = localContext.resumableStream("chunky", () => readable, 3);
      writer.close();
      await streamToBuffer(producer);

      const follower = await followerPromise;
      const followerResult = await streamToBuffer(follower);
      expect(followerResult).toEqual("def");
    });

    it("returns empty string when skipping entire backlog", async () => {
      const keyPrefix = "postgres-skip-empty-" + crypto.randomUUID();
      localContext = createPostgresResumableStreamContext(baseOptions(keyPrefix));
      const { readable, writer } = createTestingStream();
      const producer = await localContext.resumableStream("empty", () => readable);
      writer.write("small");
      const followerPromise = localContext.resumableStream("empty", () => readable, 10);
      writer.close();
      await streamToBuffer(producer);
      const follower = await followerPromise;
      const followerResult = await streamToBuffer(follower);
      expect(followerResult).toEqual("");
    });

    it("closes followers after stream completion", async () => {
      const keyPrefix = "postgres-done-" + crypto.randomUUID();
      localContext = createPostgresResumableStreamContext(baseOptions(keyPrefix));
      const { readable, writer } = createTestingStream();
      const producer = await localContext.resumableStream("close", () => readable);
      const follower = await localContext.resumableStream("close", () => readable);
      writer.write("done\n");
      writer.close();
      await Promise.all([streamToBuffer(producer), streamToBuffer(follower)]);
      expect(await localContext.hasExistingStream("close")).toBe("DONE");
    });

    it("allows followers to cancel without impacting producers", async () => {
      const keyPrefix = "postgres-cancel-" + crypto.randomUUID();
      localContext = createPostgresResumableStreamContext(baseOptions(keyPrefix));
      const { readable, writer } = createTestingStream();
      const producer = await localContext.resumableStream("cancel", () => readable);
      const follower = await localContext.resumableStream("cancel", () => readable);
      const reader = follower.getReader();
      await reader.cancel();
      writer.write("still\n");
      writer.close();
      const producerResult = await streamToBuffer(producer);
      expect(producerResult).toEqual("still\n");
    });

    it("recovers when the producer context is closed mid-stream", async () => {
      const keyPrefix = "postgres-crash-" + crypto.randomUUID();
      localContext = createPostgresResumableStreamContext(baseOptions(keyPrefix));
      const { readable, writer } = createTestingStream();
      const producer = await localContext.resumableStream("crash", () => readable);
      writer.write("partial\n");
      await streamToBuffer(producer, 1);
      await localContext.close();

      const followerContext = createPostgresResumableStreamContext(baseOptions(keyPrefix));
      const follower = await followerContext.resumableStream("crash", () => readable);
      const followerResultPromise = streamToBuffer(follower, 1);
      await pool.query(
        `UPDATE ${sessionTable} SET status = 'done', updated_at = NOW() WHERE stream_id = $1`,
        [`${keyPrefix}:rs:crash`]
      );
      const followerResult = await followerResultPromise;
      expect(followerResult).toEqual("partial\n");
      await followerContext.close();
    });

    it("handles repeated retention cleanup cycles", async () => {
      const keyPrefix = "postgres-retention-cycle-" + crypto.randomUUID();

      for (let i = 0; i < 3; i++) {
        const ctx = createPostgresResumableStreamContext({
          ...baseOptions(keyPrefix),
          retentionSeconds: 1,
        });
        const { readable, writer } = createTestingStream();
        const stream = await ctx.resumableStream(`cycle-${i}`, () => readable);
        writer.write(`cycle-${i}\n`);
        writer.close();
        await streamToBuffer(stream);
        await ctx.close();
        await pool.query(
          `DELETE FROM ${chunkTable} WHERE stream_id LIKE $1 AND created_at < NOW()`,
          [`${keyPrefix}:rs:%`]
        );
        await pool.query(`DELETE FROM ${sessionTable} WHERE stream_id LIKE $1`, [
          `${keyPrefix}:rs:%`,
        ]);
      }

      await pool.query(`VACUUM ${chunkTable}`);
      await pool.query(`VACUUM ${sessionTable}`);
    });

    it("recovers when the upstream stream throws", async () => {
      const keyPrefix = "postgres-producer-error-" + crypto.randomUUID();
      localContext = createPostgresResumableStreamContext(baseOptions(keyPrefix));

      const {
        readable: failingReadable,
        writer: failingWriter,
      } = createTestingStream();
      const failing = await localContext.resumableStream("boom", () => failingReadable);
      failingWriter.write("partial\n");
      const firstChunk = await streamToBuffer(failing, 1);
      expect(firstChunk).toEqual("partial\n");
      await failingWriter.abort(new Error("boom"));

      await expect(streamToBuffer(failing)).rejects.toThrow("boom");
      expect(await localContext.hasExistingStream("boom")).toBe(true);

      const backlog = await localContext.resumeExistingStream("boom");
      expect(backlog).toBeDefined();
      expect(await streamToBuffer(backlog!)).toEqual("partial\n");
      expect(await localContext.hasExistingStream("boom")).toBe(true);

      const { readable, writer } = createTestingStream();
      const recovery = await localContext.resumableStream("boom", () => readable);
      writer.write("ok\n");
      writer.close();
      const recovered = await streamToBuffer(recovery);
      expect(recovered).toEqual("ok\n");
      expect(await localContext.hasExistingStream("boom")).toBe("DONE");
    });
  });

}
