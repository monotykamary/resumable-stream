import { Pool } from "pg";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import crypto from "node:crypto";

import { createPostgresResumableStreamContext } from "../src/postgres";
import { DEFAULT_SCHEMA, DEFAULT_CHUNK_TABLE } from "../src/postgres/schema";
import { createTestingStream, streamToBuffer } from "../testing-utils/testing-stream";

const execFileAsync = promisify(execFile);
const POSTGRES_URL = process.env.POSTGRES_URL;

async function restartPostgresContainer() {
  await execFileAsync("docker", ["compose", "restart", "postgres"], {
    cwd: process.cwd(),
  });
  await waitForPostgres();
}

async function waitForPostgres(attempts = 20) {
  if (!POSTGRES_URL) return;
  let lastError: unknown;
  for (let i = 0; i < attempts; i++) {
    const pool = new Pool({ connectionString: POSTGRES_URL });
    try {
      await pool.query("SELECT 1");
      await pool.end();
      return;
    } catch (error) {
      lastError = error;
      await pool.end().catch(() => {});
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }
  throw lastError;
}

async function main() {
  if (!POSTGRES_URL) {
    console.error("POSTGRES_URL is required for WAL simulation");
    process.exitCode = 1;
    return;
  }
  const keyPrefix = "wal-test-" + crypto.randomUUID();
  const poolA = new Pool({ connectionString: POSTGRES_URL });
  await poolA.query(DEFAULT_SCHEMA);
  const listenerPoolA = new Pool({ connectionString: POSTGRES_URL });
  const ctxA = createPostgresResumableStreamContext({
    pool: poolA,
    listenerPool: listenerPoolA,
    waitUntil: () => Promise.resolve(),
    keyPrefix,
  });

  const { readable, writer } = createTestingStream();
  const producer = await ctxA.resumableStream("wal", () => readable);
  for (let i = 0; i < 100; i++) {
    writer.write(`token-${i}\n`);
  }
  writer.close();
  await streamToBuffer(producer);
  await ctxA.close();
  await listenerPoolA.end();
  await poolA.end();

  console.log("Restarting docker postgres container...");
  await restartPostgresContainer();

  const poolB = new Pool({ connectionString: POSTGRES_URL });
  const listenerPoolB = new Pool({ connectionString: POSTGRES_URL });
  const ctxB = createPostgresResumableStreamContext({
    pool: poolB,
    listenerPool: listenerPoolB,
    waitUntil: () => Promise.resolve(),
    keyPrefix,
  });
  const status = await ctxB.hasExistingStream("wal");
  console.log("Stream status after restart:", status);
  const chunks = await poolB.query<{ chunk: string }>(
    `SELECT chunk FROM ${DEFAULT_CHUNK_TABLE} WHERE stream_id = $1 ORDER BY seq`,
    [`${keyPrefix}:rs:wal`]
  );
  console.log("Persisted chunk count:", chunks.rowCount);
  console.log("First chunk:", chunks.rows[0]?.chunk);
  console.log("Last chunk:", chunks.rows[chunks.rows.length - 1]?.chunk);
  await ctxB.close();
  await listenerPoolB.end();
  await poolB.end();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
