export const DEFAULT_SESSION_TABLE = "rs_stream_sessions";
export const DEFAULT_CHUNK_TABLE = "rs_stream_chunks";

export const DEFAULT_SCHEMA = `
CREATE TABLE IF NOT EXISTS ${DEFAULT_SESSION_TABLE} (
  stream_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ,
  last_offset BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ${DEFAULT_CHUNK_TABLE} (
  stream_id TEXT NOT NULL REFERENCES ${DEFAULT_SESSION_TABLE}(stream_id) ON DELETE CASCADE,
  seq BIGSERIAL,
  start_offset BIGINT NOT NULL,
  end_offset BIGINT NOT NULL,
  chunk TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (stream_id, seq)
);

CREATE INDEX IF NOT EXISTS ${DEFAULT_CHUNK_TABLE}_stream_seq_idx
  ON ${DEFAULT_CHUNK_TABLE} (stream_id, seq);
`;

export const LISTEN_CHANNEL_SUFFIX = "_notify";
