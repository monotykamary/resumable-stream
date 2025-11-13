import { Pool } from "pg";

import { DEFAULT_SESSION_TABLE } from "../src/postgres/schema";
import { quoteIdentifier } from "../src/postgres/utils";

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
    const { rowCount } = await pool.query(
      `DELETE FROM ${sessionTable} WHERE expires_at IS NOT NULL AND expires_at < NOW()`
    );
    console.log(`Deleted ${rowCount ?? 0} expired resumable-stream sessions (chunks cascade).`);
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("Failed to clean up expired sessions", error);
  process.exitCode = 1;
});
