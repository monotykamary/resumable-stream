#!/usr/bin/env node
import { Pool } from "pg";

import { DEFAULT_SCHEMA } from "../postgres/schema";

async function main() {
  const connectionString = process.env.POSTGRES_URL;
  if (!connectionString) {
    console.error("POSTGRES_URL is required to run postgres:setup");
    process.exitCode = 1;
    return;
  }
  const pool = new Pool({ connectionString });
  try {
    await pool.query(DEFAULT_SCHEMA);
    console.log("Applied resumable-stream Postgres schema");
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("Failed to apply Postgres schema", error);
  process.exitCode = 1;
});
