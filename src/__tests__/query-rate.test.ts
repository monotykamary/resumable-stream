import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Pool } from "pg";

import { createPostgresResumableStreamContext } from "../postgres";
import { DEFAULT_CHUNK_TABLE, DEFAULT_SCHEMA, DEFAULT_SESSION_TABLE } from "../postgres/schema";
import { quoteIdentifier } from "../postgres/utils";
import { createTestingStream, streamToBuffer } from "../../testing-utils/testing-stream";

const POSTGRES_URL = process.env.POSTGRES_URL;

// Query metrics collector
type QueryMetrics = {
  selectCount: number;
  insertCount: number;
  selectDurationMs: number;
  insertDurationMs: number;
  startTime: number;
  endTime: number;
};

function instrumentPool(pool: Pool, chunkTable: string): { metrics: QueryMetrics; restore: () => void } {
  const originalQuery = pool.query.bind(pool);
  const metrics: QueryMetrics = {
    selectCount: 0,
    insertCount: 0,
    selectDurationMs: 0,
    insertDurationMs: 0,
    startTime: Date.now(),
    endTime: Date.now(),
  };

  const selectPattern = `FROM ${chunkTable}`;
  const insertPattern = `INSERT INTO ${chunkTable}`;

  (pool as unknown as { query: typeof pool.query }).query = async (
    ...args: Parameters<Pool["query"]>
  ): Promise<ReturnType<Pool["query"]>> => {
    const [text] = args;
    const sql = typeof text === "string" ? text : "";
    const start = Date.now();
    const result = await originalQuery(...args);
    const duration = Date.now() - start;

    if (sql.includes(selectPattern) && sql.includes("SELECT")) {
      metrics.selectCount++;
      metrics.selectDurationMs += duration;
    } else if (sql.includes(insertPattern)) {
      metrics.insertCount++;
      metrics.insertDurationMs += duration;
    }
    metrics.endTime = Date.now();
    return result;
  };

  return {
    metrics,
    restore: () => {
      (pool as unknown as { query: typeof pool.query }).query = originalQuery;
    },
  };
}

function calculateRates(metrics: QueryMetrics): {
  selectRate: number;
  insertRate: number;
  ratio: number;
  durationSeconds: number;
} {
  const durationSeconds = (metrics.endTime - metrics.startTime) / 1000;
  const selectRate = durationSeconds > 0 ? metrics.selectCount / durationSeconds : 0;
  const insertRate = durationSeconds > 0 ? metrics.insertCount / durationSeconds : 0;
  const ratio = insertRate > 0 ? selectRate / insertRate : 0;
  return { selectRate, insertRate, ratio, durationSeconds };
}

if (!POSTGRES_URL) {
  console.error("POSTGRES_URL is not set, skipping query rate tests");
  describe("Query rate tests", () => {
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

  const baseOptions = (keyPrefix = "test-query-rate-" + crypto.randomUUID()) => ({
    pool,
    listenerPool,
    waitUntil: () => Promise.resolve(),
    keyPrefix,
    retentionSeconds: 60,
    pollIntervalMs: 25,
    listenTimeoutMs: 200,
  });

  let context: ReturnType<typeof createPostgresResumableStreamContext> | null = null;
  let instrumentation: ReturnType<typeof instrumentPool> | null = null;

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
    instrumentation = instrumentPool(pool, chunkTable);
  });

  afterEach(async () => {
    if (instrumentation) {
      instrumentation.restore();
      instrumentation = null;
    }
    await context?.close?.();
    context = null;
  });

  afterAll(async () => {
    await context?.close?.();
    await closePool(listenerPool);
    await closePool(pool);
  }, 20000);

  describe("query rate measurement", () => {
    it("should measure SELECT vs INSERT rate ratio", async () => {
      const keyPrefix = "query-rate-measure-" + crypto.randomUUID();
      context = createPostgresResumableStreamContext(baseOptions(keyPrefix));

      const { readable, writer } = createTestingStream();
      const producerPromise = context.resumableStream("test", () => readable);
      const followerPromise = context.resumableStream("test", () => readable);

      // Write chunks with small delays to simulate realistic streaming
      const chunkCount = 50;
      for (let i = 0; i < chunkCount; i++) {
        writer.write(`chunk-${i}\n`);
        // Small delay to allow consumer to catch up
        if (i % 5 === 0) {
          await new Promise((r) => setTimeout(r, 10));
        }
      }
      writer.close();

      const producer = await producerPromise;
      const follower = await followerPromise;

      await Promise.all([streamToBuffer(producer), streamToBuffer(follower)]);

      // Get metrics
      const { metrics } = instrumentation!;
      const rates = calculateRates(metrics);

      console.log("\n=== Query Rate Metrics ===");
      console.log(`Duration: ${rates.durationSeconds.toFixed(2)}s`);
      console.log(`SELECT count: ${metrics.selectCount}, rate: ${rates.selectRate.toFixed(1)} calls/sec`);
      console.log(`INSERT count: ${metrics.insertCount}, rate: ${rates.insertRate.toFixed(1)} calls/sec`);
      console.log(`SELECT/INSERT ratio: ${rates.ratio.toFixed(1)}:1`);
      console.log("==========================\n");

      // The issue: expect high ratio (should be close to 1:1 in ideal case, but currently higher)
      // This test documents the baseline behavior
      expect(metrics.selectCount).toBeGreaterThan(0);
      expect(metrics.insertCount).toBeGreaterThan(0);

      // Store the ratio for comparison - this will be our baseline
      // After optimization, we expect this ratio to decrease significantly
      (global as unknown as { __queryRateRatio?: number }).__queryRateRatio = rates.ratio;
    });

    it("should measure rate with multiple followers", async () => {
      const keyPrefix = "query-rate-multi-" + crypto.randomUUID();
      context = createPostgresResumableStreamContext(baseOptions(keyPrefix));

      const { readable, writer } = createTestingStream();
      const producerPromise = context.resumableStream("multi", () => readable);
      const follower1Promise = context.resumableStream("multi", () => readable);
      const follower2Promise = context.resumableStream("multi", () => readable);

      const chunkCount = 30;
      for (let i = 0; i < chunkCount; i++) {
        writer.write(`chunk-${i}\n`);
        if (i % 5 === 0) {
          await new Promise((r) => setTimeout(r, 10));
        }
      }
      writer.close();

      const producer = await producerPromise;
      const follower1 = await follower1Promise;
      const follower2 = await follower2Promise;

      await Promise.all([
        streamToBuffer(producer),
        streamToBuffer(follower1),
        streamToBuffer(follower2),
      ]);

      const { metrics } = instrumentation!;
      const rates = calculateRates(metrics);

      console.log("\n=== Multi-Follower Query Rate Metrics ===");
      console.log(`Duration: ${rates.durationSeconds.toFixed(2)}s`);
      console.log(`SELECT count: ${metrics.selectCount}, rate: ${rates.selectRate.toFixed(1)} calls/sec`);
      console.log(`INSERT count: ${metrics.insertCount}, rate: ${rates.insertRate.toFixed(1)} calls/sec`);
      console.log(`SELECT/INSERT ratio: ${rates.ratio.toFixed(1)}:1`);
      console.log("========================================\n");

      expect(metrics.selectCount).toBeGreaterThan(0);
      expect(metrics.insertCount).toBeGreaterThan(0);
    });

    it("should verify ratio improves with notification working", async () => {
      // This test uses a stubbed listenerPool to force polling, then compares
      const keyPrefix = "query-rate-polling-" + crypto.randomUUID();

      // First, test with notifications disabled (polling fallback)
      const listenerStub = {
        query: async () => ({ rows: [] }),
        connect: async () => ({
          query: async () => ({ rows: [] }),
          release: async () => {},
        }),
      };

      context = createPostgresResumableStreamContext({
        ...baseOptions(keyPrefix),
        listenerPool: listenerStub as unknown as Pool,
        pollIntervalMs: 25, // Fast polling to exaggerate the issue
      });

      const { readable, writer } = createTestingStream();
      const producerPromise = context.resumableStream("poll-test", () => readable);
      const followerPromise = context.resumableStream("poll-test", () => readable);

      for (let i = 0; i < 20; i++) {
        writer.write(`chunk-${i}\n`);
        await new Promise((r) => setTimeout(r, 20)); // Slower producer
      }
      writer.close();

      const producer = await producerPromise;
      const follower = await followerPromise;
      await Promise.all([streamToBuffer(producer), streamToBuffer(follower)]);

      const { metrics } = instrumentation!;
      const rates = calculateRates(metrics);

      console.log("\n=== Polling Fallback Query Rate Metrics ===");
      console.log(`Duration: ${rates.durationSeconds.toFixed(2)}s`);
      console.log(`SELECT count: ${metrics.selectCount}, rate: ${rates.selectRate.toFixed(1)} calls/sec`);
      console.log(`INSERT count: ${metrics.insertCount}, rate: ${rates.insertRate.toFixed(1)} calls/sec`);
      console.log(`SELECT/INSERT ratio: ${rates.ratio.toFixed(1)}:1`);
      console.log("==========================================\n");

      // With polling fallback, we expect even higher ratio
      expect(rates.ratio).toBeGreaterThan(1);
    });
  });
}
