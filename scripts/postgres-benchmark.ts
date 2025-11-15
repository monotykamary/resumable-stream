#!/usr/bin/env tsx
import { Pool } from "pg";
import { randomUUID } from "crypto";
import { performance } from "perf_hooks";

import { createPostgresResumableStreamContext } from "../src/postgres";
import { DEFAULT_CHUNK_TABLE, DEFAULT_SCHEMA, DEFAULT_SESSION_TABLE } from "../src/postgres/schema";
import { quoteIdentifier } from "../src/postgres/utils";
import { createTestingStream } from "../testing-utils/testing-stream";

type BenchmarkResult = {
  batchSize: number;
  batchIntervalMs: number;
  durationMs: number;
  throughput: number;
  chunkCount: number;
  flushCount: number;
  avgFlushSize: number;
  flushThroughput: number;
  avgFlushDurationMs: number;
};

async function main() {
  const connectionString = process.env.POSTGRES_URL;
  if (!connectionString) {
    console.error("POSTGRES_URL is required to run postgres:benchmark");
    process.exit(1);
    return;
  }

  const batchSizes = parseNumericList(process.env.POSTGRES_BENCH_BATCH_SIZES, {
    fallback: "0,1,16,32",
    allowZero: true,
  });
  if (!batchSizes.length) {
    console.error("No valid batch sizes found. Provide POSTGRES_BENCH_BATCH_SIZES or accept the defaults.");
    process.exit(1);
    return;
  }

  const chunkCount = Number(process.env.POSTGRES_BENCH_CHUNKS ?? 2000);
  const chunkLength = Number(process.env.POSTGRES_BENCH_CHUNK_LENGTH ?? 128);
  const batchIntervals = parseNumericList(process.env.POSTGRES_BENCH_INTERVALS_MS, {
    fallback: "5,50,100,150,200,250,300,350,400,450,500",
    allowZero: false,
  });
  if (!batchIntervals.length) {
    console.error("No valid batch intervals found. Provide POSTGRES_BENCH_INTERVALS_MS or accept the defaults.");
    process.exit(1);
    return;
  }

  const pool = new Pool({ connectionString });
  try {
    await pool.query(DEFAULT_SCHEMA);
    const chunkTable = quoteIdentifier(DEFAULT_CHUNK_TABLE);
    const sessionTable = quoteIdentifier(DEFAULT_SESSION_TABLE);
    for (const batchSize of batchSizes) {
      for (const batchIntervalMs of batchIntervals) {
        await pool.query(`TRUNCATE ${chunkTable}, ${sessionTable} RESTART IDENTITY`);
        const restoreInstrumentation = instrumentChunkInserts(pool, chunkTable);
        let flushStats: FlushStats | null = null;
        try {
          const context = createPostgresResumableStreamContext({
            pool,
            listenerPool: pool,
            waitUntil: () => Promise.resolve(),
            keyPrefix: `postgres-bench-${randomUUID()}`,
            retentionSeconds: 60,
            chunkBatchSize: batchSize,
            chunkBatchIntervalMs: batchIntervalMs,
            maxBufferedChunks:
              batchSize > 0 ? Math.max(batchSize * 4, 1024) : 1024,
          });
          let benchmarkResult: Awaited<ReturnType<typeof runBenchmark>>;
          try {
            benchmarkResult = await runBenchmark(context, chunkCount, chunkLength);
          } finally {
            await context.close();
          }
          flushStats = restoreInstrumentation();
          reportResult({
            ...benchmarkResult,
            batchSize,
            batchIntervalMs,
            chunkCount,
            ...flushStats,
          });
        } finally {
          if (!flushStats) {
            restoreInstrumentation();
          }
        }
      }
    }
  } finally {
    await pool.end();
  }
}

type ParseOptions = {
  fallback?: string;
  allowZero?: boolean;
};

function parseNumericList(raw: string | undefined, options: ParseOptions): number[] {
  const value = raw && raw.trim().length > 0 ? raw : options.fallback ?? "";
  return value
    .split(",")
    .map((part) => Number(part.trim()))
    .filter((num) => Number.isFinite(num) && (options.allowZero ? num >= 0 : num > 0));
}

async function runBenchmark(
  context: ReturnType<typeof createPostgresResumableStreamContext>,
  chunkCount: number,
  chunkLength: number
): Promise<Omit<BenchmarkResult, "batchSize" | "batchIntervalMs" | "chunkCount" | "flushCount" | "avgFlushSize" | "flushThroughput" | "avgFlushDurationMs">> {
  const { readable, writer } = createTestingStream();
  const payload = "x".repeat(chunkLength);
  const stream = await context.createNewResumableStream(`bench-${randomUUID()}`, () => readable);
  if (!stream) {
    throw new Error("createNewResumableStream unexpectedly returned null");
  }
  const start = Date.now();
  const producer = (async () => {
    for (let i = 0; i < chunkCount; i++) {
      await writer.write(payload);
    }
    await writer.close();
  })();

  await Promise.all([producer, drainStream(stream)]);
  const durationMs = Date.now() - start;
  const throughput = chunkCount / (durationMs / 1000);
  return { durationMs, throughput };
}

async function drainStream(stream: ReadableStream<string>): Promise<void> {
  const reader = stream.getReader();
  while (true) {
    const { done } = await reader.read();
    if (done) {
      return;
    }
  }
}

function reportResult(result: BenchmarkResult): void {
  const formattedChunks = result.throughput.toFixed(1);
  const formattedFlushRate = result.flushThroughput.toFixed(1);
  const formattedAvgSize = result.avgFlushSize.toFixed(1);
  const formattedAvgDuration = result.avgFlushDurationMs.toFixed(2);
  console.log(
    `Batch size ${result.batchSize.toString().padStart(3, " ")} | interval ${result.batchIntervalMs
      .toString()
      .padStart(3, " ")}ms :: chunks ${formattedChunks}/sec (${result.durationMs} ms for ${
      result.chunkCount
    }) | flush ${formattedFlushRate}/sec avg ${formattedAvgSize} rows (${formattedAvgDuration} ms)`
  );
}

main().catch((error) => {
  console.error("postgres:benchmark failed", error);
  process.exit(1);
});

type FlushStats = {
  flushCount: number;
  avgFlushSize: number;
  flushThroughput: number;
  avgFlushDurationMs: number;
};

type InternalFlushStats = {
  flushCount: number;
  totalRows: number;
  totalDurationMs: number;
};

function instrumentChunkInserts(pool: Pool, chunkTable: string): () => FlushStats {
  const marker = `INSERT INTO ${chunkTable}`;
  const originalQuery = pool.query.bind(pool);
  const stats: InternalFlushStats = {
    flushCount: 0,
    totalRows: 0,
    totalDurationMs: 0,
  };
  const benchStart = performance.now();

  (pool as unknown as { query: typeof pool.query }).query = async (
    ...args: Parameters<Pool["query"]>
  ): Promise<ReturnType<Pool["query"]>> => {
    const [text] = args;
    const sql = typeof text === "string" ? text : "";
    const isChunkInsert = sql.includes(marker);
    if (!isChunkInsert) {
      return originalQuery(...args);
    }
    const start = performance.now();
    const result = await originalQuery(...args);
    const end = performance.now();
    stats.flushCount += 1;
    stats.totalRows += result?.rows?.length ?? 0;
    stats.totalDurationMs += end - start;
    return result;
  };

  return () => {
    (pool as unknown as { query: typeof pool.query }).query = originalQuery;
    const totalSeconds = (performance.now() - benchStart) / 1000;
    const flushCount = stats.flushCount;
    const avgFlushSize = flushCount ? stats.totalRows / flushCount : 0;
    const avgFlushDurationMs = flushCount ? stats.totalDurationMs / flushCount : 0;
    const flushThroughput = flushCount && totalSeconds > 0 ? flushCount / totalSeconds : 0;
    return {
      flushCount,
      avgFlushSize,
      flushThroughput,
      avgFlushDurationMs,
    };
  };
}
