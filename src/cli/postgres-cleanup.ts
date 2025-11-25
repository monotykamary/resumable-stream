#!/usr/bin/env node
import { Pool } from "pg";

import { DEFAULT_SESSION_TABLE } from "../postgres/schema";
import { quoteIdentifier } from "../postgres/utils";

async function main() {
  const connectionString = process.env.POSTGRES_URL;
  if (!connectionString) {
    console.error("POSTGRES_URL is required to run postgres:cleanup");
    process.exitCode = 1;
    return;
  }

  const sessionTableName = process.env.POSTGRES_SESSION_TABLE || DEFAULT_SESSION_TABLE;
  const sessionTable = quoteIdentifier(sessionTableName);
  const pool = new Pool({ connectionString });
  try {
    let totalDeleted = 0;
    let deleted = 0;
    const batchSize = 1000;
    do {
      const { rowCount } = await pool.query(
        `DELETE FROM ${sessionTable}
         WHERE stream_id IN (
           SELECT stream_id FROM ${sessionTable}
           WHERE expires_at IS NOT NULL AND expires_at < NOW()
           LIMIT ${batchSize}
         )`
      );
      deleted = rowCount ?? 0;
      totalDeleted += deleted;
      if (deleted > 0) {
        console.log(`Deleted batch of ${deleted} sessions...`);
      }
    } while (deleted > 0);

    console.log(`Deleted total ${totalDeleted} expired resumable-stream sessions (chunks cascade).`);
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("Failed to clean up expired sessions", error);
  process.exitCode = 1;
});
