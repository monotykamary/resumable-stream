#!/usr/bin/env tsx
/**
 * Query Rate Benchmark
 * Measures the SELECT vs INSERT query ratio for resumable streams.
 * This quantifies the N+1 query issue where consumers poll faster than
 * producers can insert.
 */
import { Pool } from "pg";
import { randomUUID } from "crypto";

import { createPostgresResumableStreamContext } from "../src/postgres";
import { DEFAULT_CHUNK_TABLE, DEFAULT_SCHEMA, DEFAULT_SESSION_TABLE } from "../src/postgres/schema";
import { quoteIdentifier } from "../src/postgres/utils";
import { createTestingStream } from "../testing-utils/testing-stream";

type BenchmarkConfig = {
  chunkCount: number;
  pollIntervalMs: number;
  listenTimeoutMs: number;
  chunkBatchSize: number;
  chunkBatchIntervalMs: number;
  followerCount: number;
};

type QueryMetrics = {
  selectCount: number;
  insertCount: number;
  selectDurationMs: number;
  insertDurationMs: number;
  startTime: number;
  endTime: number;
};

type BenchmarkResult = {
  config: BenchmarkConfig;
  metrics: QueryMetrics;
  selectRate: number; // calls/sec
  insertRate: number; // calls/sec
  ratio: number; // select/insert
  durationSeconds: number;
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

async function runBenchmark(
  pool: Pool,
  listenerPool: Pool,
  config: BenchmarkConfig
): Promise<BenchmarkResult> {
  const chunkTable = quoteIdentifier(DEFAULT_CHUNK_TABLE);
  const sessionTable = quoteIdentifier(DEFAULT_SESSION_TABLE);

  await pool.query(`TRUNCATE ${chunkTable}, ${sessionTable} RESTART IDENTITY`);

  const { metrics, restore } = instrumentPool(pool, chunkTable);

  const keyPrefix = `query-rate-bench-${randomUUID()}`;
  const context = createPostgresResumableStreamContext({
    pool,
    listenerPool,
    waitUntil: () => Promise.resolve(),
    keyPrefix,
    retentionSeconds: 60,
    pollIntervalMs: config.pollIntervalMs,
    listenTimeoutMs: config.listenTimeoutMs,
    chunkBatchSize: config.chunkBatchSize,
    chunkBatchIntervalMs: config.chunkBatchIntervalMs,
  });

  const { readable, writer } = createTestingStream();

  try {
    // Create producer and followers
    const producerPromise = context.resumableStream("bench", () => readable);
    const followerPromises = Array.from({ length: config.followerCount }, () =>
      context.resumableStream("bench", () => readable)
    );

    const producer = await producerPromise;
    const followers = await Promise.all(followerPromises);

    // Produce chunks with realistic delays
    for (let i = 0; i < config.chunkCount; i++) {
      writer.write(`chunk-${i}\n`);
      // Simulate realistic streaming - occasional small delays
      if (i % 10 === 0) {
        await new Promise((r) => setTimeout(r, 5));
      }
    }
    writer.close();

    // Wait for all streams to complete
    await Promise.all([
      drainStream(producer),
      ...followers.map((f) => drainStream(f)),
    ]);
  } finally {
    await context.close();
    restore();
  }

  const durationSeconds = (metrics.endTime - metrics.startTime) / 1000;
  const selectRate = durationSeconds > 0 ? metrics.selectCount / durationSeconds : 0;
  const insertRate = durationSeconds > 0 ? metrics.insertCount / durationSeconds : 0;
  const ratio = insertRate > 0 ? selectRate / insertRate : 0;

  return {
    config,
    metrics,
    selectRate,
    insertRate,
    ratio,
    durationSeconds,
  };
}

async function drainStream(stream: ReadableStream<string>): Promise<void> {
  const reader = stream.getReader();
  while (true) {
    const { done } = await reader.read();
    if (done) return;
  }
}

function formatResult(result: BenchmarkResult): string {
  const c = result.config;
  return [
    `chunks=${c.chunkCount} followers=${c.followerCount} poll=${c.pollIntervalMs}ms batchSize=${c.chunkBatchSize}`,
    `  SELECT: ${result.metrics.selectCount} calls (${result.selectRate.toFixed(1)}/sec)`,
    `  INSERT: ${result.metrics.insertCount} calls (${result.insertRate.toFixed(1)}/sec)`,
    `  RATIO: ${result.ratio.toFixed(2)}:1 (lower is better)`,
    `  Duration: ${result.durationSeconds.toFixed(2)}s`,
  ].join("\n");
}

async function main() {
  const connectionString = process.env.POSTGRES_URL;
  if (!connectionString) {
    console.error("POSTGRES_URL is required");
    process.exit(1);
    return;
  }

  const pool = new Pool({ connectionString });
  const listenerPool = new Pool({ connectionString });

  try {
    await pool.query(DEFAULT_SCHEMA);

    // Test configurations to measure the baseline
    const configs: BenchmarkConfig[] = [
      // Baseline: 1 follower, default settings
      {
        chunkCount: 50,
        pollIntervalMs: 50,
        listenTimeoutMs: 500,
        chunkBatchSize: 0,
        chunkBatchIntervalMs: 5,
        followerCount: 1,
      },
      // Stress test: multiple followers
      {
        chunkCount: 30,
        pollIntervalMs: 50,
        listenTimeoutMs: 500,
        chunkBatchSize: 0,
        chunkBatchIntervalMs: 5,
        followerCount: 3,
      },
      // Fast polling scenario (exaggerates the issue)
      {
        chunkCount: 30,
        pollIntervalMs: 10,
        listenTimeoutMs: 200,
        chunkBatchSize: 0,
        chunkBatchIntervalMs: 5,
        followerCount: 1,
      },
    ];

    console.log("\n=== Query Rate Benchmark ===");
    console.log("Measuring SELECT vs INSERT query ratio\n");

    const results: BenchmarkResult[] = [];
    for (const config of configs) {
      const result = await runBenchmark(pool, listenerPool, config);
      results.push(result);
      console.log(formatResult(result));
      console.log();
    }

    // Summary
    const avgRatio = results.reduce((sum, r) => sum + r.ratio, 0) / results.length;
    console.log("=== Summary ===");
    console.log(`Average SELECT/INSERT ratio: ${avgRatio.toFixed(2)}:1`);
    console.log(`Target ratio (after optimization): <2:1`);
    console.log();

    // Output JSON for programmatic consumption
    const jsonOutput = {
      results: results.map((r) => ({
        config: r.config,
        selectRate: r.selectRate,
        insertRate: r.insertRate,
        ratio: r.ratio,
        durationSeconds: r.durationSeconds,
      })),
      summary: {
        averageRatio: avgRatio,
      },
    };

    console.log("JSON_OUTPUT:");
    console.log(JSON.stringify(jsonOutput, null, 2));

    // Exit with error code if ratio is too high (for CI/autoresearch)
    const maxAcceptableRatio = Number(process.env.MAX_ACCEPTABLE_RATIO ?? "5");
    if (avgRatio > maxAcceptableRatio) {
      console.error(`\nERROR: Average ratio ${avgRatio.toFixed(2)} exceeds threshold ${maxAcceptableRatio}`);
      process.exit(1);
    }
  } finally {
    await pool.end();
    await listenerPool.end();
  }
}

main().catch((error) => {
  console.error("Benchmark failed:", error);
  process.exit(1);
});
