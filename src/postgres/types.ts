import { CreateResumableStreamContextOptions } from "../types";

export interface PostgresQueryResult<T = unknown> {
  rows: T[];
}

export interface PostgresQueryable {
  query<T = unknown>(sql: string, params?: unknown[]): Promise<PostgresQueryResult<T>>;
}

export interface PostgresClientLike extends PostgresQueryable {
  release(): void | Promise<void>;
  on?(event: "notification", listener: (payload: PostgresNotification) => void): void;
  off?(event: "notification", listener: (payload: PostgresNotification) => void): void;
}

export interface PostgresPoolLike extends PostgresQueryable {
  connect(): Promise<PostgresClientLike>;
}

export interface PostgresNotification {
  processId?: number;
  channel: string;
  payload: string | null;
}

export interface CreatePostgresResumableStreamContextOptions
  extends Omit<CreateResumableStreamContextOptions, "subscriber" | "publisher"> {
  /**
   * Pool or client used for issuing SQL commands. Typically an instance of `pg.Pool`.
   */
  pool: PostgresPoolLike;
  /**
   * Optional pool dedicated to LISTEN/NOTIFY so the main pool is not blocked by listeners.
   */
  listenerPool?: PostgresPoolLike;
  /**
   * How long chunks should be retained before cleanup jobs purge them, in seconds.
   * Defaults to 24 hours.
   */
  retentionSeconds?: number;
  /**
   * Name of the table storing stream sessions.
   */
  sessionTableName?: string;
  /**
   * Name of the table storing chunk rows.
   */
  chunkTableName?: string;
  /**
   * How long followers should wait for LISTEN notifications before falling back to polling.
   * Defaults to 500ms.
   */
  listenTimeoutMs?: number;
  /**
   * Interval between poll attempts when no new notifications are available.
   * Defaults to 50ms.
   */
  pollIntervalMs?: number;
  /**
   * Number of chunks to accumulate before persisting them in a single INSERT.
   * Defaults to 0 (interval-only). Set to a positive number to flush immediately once that many chunks are buffered.
   */
  chunkBatchSize?: number;
  /**
   * How long to wait (in milliseconds) before flushing a partial batch.
   * Defaults to 5ms.
   */
  chunkBatchIntervalMs?: number;
  /**
   * Maximum number of chunks allowed in memory before producer writes backpressure the flush.
   * Defaults to 1024 (or 4x the batch size, whichever is larger).
   */
  maxBufferedChunks?: number;
}

export type PostgresStreamStatus = "pending" | "streaming" | "done" | "failed";
