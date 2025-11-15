import { PostgresPoolLike } from "./types";
import { PostgresNotifier } from "./notifier";
import { toNumber } from "./utils";

type PendingChunk = {
  chunk: string;
  startOffset: number;
  endOffset: number;
};

type ChunkBatchWriterOptions = {
  pool: PostgresPoolLike;
  notifier: PostgresNotifier;
  chunkTable: string;
  sessionTable: string;
  retentionIntervalLiteral: string;
  streamId: string;
  batchSize: number;
  batchIntervalMs: number;
  maxBufferedChunks: number;
  initialOffset: number;
};

export class ChunkBatchWriter {
  private readonly batchSize: number;
  private readonly batchIntervalMs: number;
  private readonly maxBufferedChunks: number;
  private buffer: PendingChunk[] = [];
  private flushPromise: Promise<void> | null = null;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private flushError: unknown = null;
  private nextOffset: number;
  private closed = false;

  constructor(private readonly options: ChunkBatchWriterOptions) {
    const configuredBatchSize = options.batchSize;
    this.batchSize = Number.isFinite(configuredBatchSize)
      ? Math.max(1, Math.floor(configuredBatchSize))
      : Number.POSITIVE_INFINITY;
    this.batchIntervalMs = Math.max(1, options.batchIntervalMs);
    this.maxBufferedChunks = Math.max(1, Math.floor(options.maxBufferedChunks));
    this.nextOffset = options.initialOffset;
  }

  async append(chunk: string): Promise<void> {
    if (this.buffer.length >= this.maxBufferedChunks) {
      this.startFlush();
      if (this.flushPromise) {
        await this.flushPromise;
      }
    }

    this.throwIfFailed();
    if (this.closed) {
      throw new Error("Cannot append chunk after writer is closed.");
    }
    const startOffset = this.nextOffset;
    const endOffset = startOffset + chunk.length;
    this.nextOffset = endOffset;
    this.buffer.push({ chunk, startOffset, endOffset });

    const reachedSizeThreshold =
      Number.isFinite(this.batchSize) && this.buffer.length >= this.batchSize;
    if (reachedSizeThreshold) {
      this.startFlush();
    } else if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => this.startFlush(), this.batchIntervalMs);
    }

    this.throwIfFailed();
  }

  async close(options?: { suppressErrors?: boolean }): Promise<void> {
    if (this.closed) {
      if (!options?.suppressErrors) {
        this.throwIfFailed();
      }
      return;
    }
    this.closed = true;
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    this.startFlush();
    try {
      if (this.flushPromise) {
        await this.flushPromise;
      }
      if (!options?.suppressErrors) {
        this.throwIfFailed();
      }
    } catch (error) {
      if (!options?.suppressErrors) {
        throw error;
      }
    }
  }

  private startFlush(): void {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    if (this.flushPromise || this.buffer.length === 0) {
      return;
    }
    const flushTask = this.flushPendingBatches();
    this.flushPromise = flushTask;
    flushTask
      .catch((error) => {
        this.flushError = error;
      })
      .finally(() => {
        if (this.flushPromise === flushTask) {
          this.flushPromise = null;
        }
      });
  }

  private async flushPendingBatches(): Promise<void> {
    while (this.buffer.length) {
      const batch = this.buffer.splice(0, this.batchSize);
      await this.persistBatch(batch);
    }
  }

  private async persistBatch(batch: PendingChunk[]): Promise<void> {
    if (!batch.length) {
      return;
    }
    const valueFragments: string[] = [];
    const params: Array<string | number> = [];
    let paramIndex = 1;
    for (const item of batch) {
      valueFragments.push(`($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++})`);
      params.push(this.options.streamId, item.startOffset, item.endOffset, item.chunk);
    }
    const insertSql = `INSERT INTO ${this.options.chunkTable} (stream_id, start_offset, end_offset, chunk)
                       VALUES ${valueFragments.join(", ")}
                       RETURNING seq`;
    const insertResult = await this.options.pool.query<{ seq: number | string }>(insertSql, params);
    let latestSeq: number | null = null;
    if (insertResult.rows.length) {
      latestSeq = toNumber(insertResult.rows[insertResult.rows.length - 1].seq);
    }

    const lastOffset = batch[batch.length - 1].endOffset;
    await this.options.pool.query(
      `UPDATE ${this.options.sessionTable}
       SET last_offset = $2, updated_at = NOW(), expires_at = NOW() + INTERVAL '${this.options.retentionIntervalLiteral}'
       WHERE stream_id = $1`,
      [this.options.streamId, lastOffset]
    );

    if (latestSeq !== null) {
      try {
        await this.options.notifier.notify({ streamId: this.options.streamId, event: "chunk", seq: latestSeq });
      } catch {
        // Notification failures are tolerated; polling handles the fallback.
      }
    }
  }

  private throwIfFailed(): void {
    if (this.flushError) {
      throw this.flushError;
    }
  }
}
