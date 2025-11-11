import { ResumableStreamContext } from "./types";
import {
  CreatePostgresResumableStreamContextOptions,
  PostgresPoolLike,
  PostgresStreamStatus,
} from "./postgres/types";
import {
  DEFAULT_CHUNK_TABLE,
  DEFAULT_SESSION_TABLE,
  LISTEN_CHANNEL_SUFFIX,
} from "./postgres/schema";
import { PostgresNotifier } from "./postgres/notifier";
import { delay, quoteIdentifier, sanitizeChannelName, toNumber } from "./postgres/utils";

const DEFAULT_RETENTION_SECONDS = 24 * 60 * 60;
const DEFAULT_POLL_INTERVAL_MS = 50;
const DEFAULT_LISTEN_TIMEOUT_MS = 500;

type InternalPostgresOptions = {
  pool: PostgresPoolLike;
  listenerPool?: PostgresPoolLike;
  waitUntil: (promise: Promise<unknown>) => void;
  sessionTableName: string;
  chunkTableName: string;
  retentionSeconds: number;
  keyPrefix: string;
  pollIntervalMs: number;
  listenTimeoutMs: number;
};

type ChunkRow = {
  seq: number;
  chunk: string;
  startOffset: number;
  endOffset: number;
};

export function createPostgresResumableStreamContext(
  options: CreatePostgresResumableStreamContextOptions
): ResumableStreamContext & { close: () => Promise<void> } {
  const waitUntil = options.waitUntil || (async (p) => await p);
  const keyPrefix = `${options.keyPrefix || "resumable-stream"}:rs`;
  const internalOptions: InternalPostgresOptions = {
    pool: options.pool,
    listenerPool: options.listenerPool,
    waitUntil,
    chunkTableName: options.chunkTableName || DEFAULT_CHUNK_TABLE,
    sessionTableName: options.sessionTableName || DEFAULT_SESSION_TABLE,
    retentionSeconds: options.retentionSeconds || DEFAULT_RETENTION_SECONDS,
    keyPrefix,
    pollIntervalMs: Math.max(5, options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS),
    listenTimeoutMs: Math.max(50, options.listenTimeoutMs ?? DEFAULT_LISTEN_TIMEOUT_MS),
  };

  return new PostgresResumableStreamContext(internalOptions);
}

class PostgresResumableStreamContext implements ResumableStreamContext {
  private readonly sessionTable: string;
  private readonly chunkTable: string;
  private readonly retentionIntervalLiteral: string;
  private readonly notifier: PostgresNotifier;

  constructor(private readonly options: InternalPostgresOptions) {
    this.sessionTable = quoteIdentifier(options.sessionTableName);
    this.chunkTable = quoteIdentifier(options.chunkTableName);
    this.retentionIntervalLiteral = `${options.retentionSeconds} seconds`;
    const channelName = sanitizeChannelName(`${options.chunkTableName}${LISTEN_CHANNEL_SUFFIX}`);
    this.notifier = new PostgresNotifier(
      options.listenerPool ?? options.pool,
      channelName,
      options.listenTimeoutMs
    );
  }

  async resumableStream(
    streamId: string,
    makeStream: () => ReadableStream<string>,
    skipCharacters?: number
  ): Promise<ReadableStream<string> | null> {
    const namespacedId = this.namespacedId(streamId);
    const role = await this.determineStreamRole(namespacedId);
    if (role === "producer") {
      return this.createProducerStream(namespacedId, makeStream);
    }
    if (role === "done") {
      return null;
    }
    return this.createFollowerStream(namespacedId, skipCharacters ?? 0);
  }

  async resumeExistingStream(
    streamId: string,
    skipCharacters?: number
  ): Promise<ReadableStream<string> | null | undefined> {
    const namespacedId = this.namespacedId(streamId);
    const status = await this.getStreamStatus(namespacedId);
    if (!status) {
      return undefined;
    }
    if (status === "done") {
      return null;
    }
    return this.createFollowerStream(namespacedId, skipCharacters ?? 0);
  }

  async createNewResumableStream(
    streamId: string,
    makeStream: () => ReadableStream<string>,
    _skipCharacters?: number
  ): Promise<ReadableStream<string> | null> {
    const namespacedId = this.namespacedId(streamId);
    await this.resetStreamState(namespacedId);
    return this.createProducerStream(namespacedId, makeStream);
  }

  async hasExistingStream(streamId: string): Promise<null | true | "DONE"> {
    const status = await this.getStreamStatus(this.namespacedId(streamId));
    if (!status) {
      return null;
    }
    return status === "done" ? "DONE" : true;
  }

  private namespacedId(streamId: string): string {
    return `${this.options.keyPrefix}:${streamId}`;
  }

  private async determineStreamRole(streamId: string): Promise<"producer" | "consumer" | "done"> {
    if (await this.tryReserveStream(streamId)) {
      return "producer";
    }
    const status = await this.getStreamStatus(streamId);
    if (!status) {
      if (await this.tryReserveStream(streamId)) {
        return "producer";
      }
      return "consumer";
    }
    return status === "done" ? "done" : "consumer";
  }

  private async tryReserveStream(streamId: string): Promise<boolean> {
    const { rows } = await this.options.pool.query<{ stream_id: string }>(
      `INSERT INTO ${this.sessionTable} (stream_id, status, last_offset, created_at, updated_at, expires_at)
       VALUES ($1, 'streaming', 0, NOW(), NOW(), NOW() + INTERVAL '${this.retentionIntervalLiteral}')
       ON CONFLICT DO NOTHING
       RETURNING stream_id`,
      [streamId]
    );
    if (rows.length) {
      await this.options.pool.query(`DELETE FROM ${this.chunkTable} WHERE stream_id = $1`, [streamId]);
      return true;
    }
    return false;
  }

  private async resetStreamState(streamId: string): Promise<void> {
    await this.options.pool.query(`DELETE FROM ${this.chunkTable} WHERE stream_id = $1`, [streamId]);
    await this.options.pool.query(
      `INSERT INTO ${this.sessionTable} (stream_id, status, last_offset, created_at, updated_at, expires_at)
       VALUES ($1, 'streaming', 0, NOW(), NOW(), NOW() + INTERVAL '${this.retentionIntervalLiteral}')
       ON CONFLICT (stream_id)
       DO UPDATE SET status = 'streaming', last_offset = 0, updated_at = NOW(), expires_at = NOW() + INTERVAL '${this.retentionIntervalLiteral}'`,
      [streamId]
    );
  }

  private async getStreamStatus(streamId: string): Promise<PostgresStreamStatus | null> {
    const { rows } = await this.options.pool.query<{ status: PostgresStreamStatus }>(
      `SELECT status FROM ${this.sessionTable} WHERE stream_id = $1 LIMIT 1`,
      [streamId]
    );
    return rows[0]?.status ?? null;
  }

  private async markStreamDone(streamId: string): Promise<void> {
    await this.options.pool.query(
      `UPDATE ${this.sessionTable}
       SET status = 'done', updated_at = NOW(), expires_at = NOW() + INTERVAL '${this.retentionIntervalLiteral}'
       WHERE stream_id = $1`,
      [streamId]
    );
  }

  private async persistChunk(
    streamId: string,
    chunk: string,
    startOffset: number
  ): Promise<{ seq: number; endOffset: number }> {
    const endOffset = startOffset + chunk.length;
    const insertResult = await this.options.pool.query<{ seq: number | string }>(
      `INSERT INTO ${this.chunkTable} (stream_id, start_offset, end_offset, chunk)
       VALUES ($1, $2, $3, $4)
       RETURNING seq`,
      [streamId, startOffset, endOffset, chunk]
    );
    await this.options.pool.query(
      `UPDATE ${this.sessionTable}
       SET last_offset = $2, updated_at = NOW(), expires_at = NOW() + INTERVAL '${this.retentionIntervalLiteral}'
       WHERE stream_id = $1`,
      [streamId, endOffset]
    );
    return { seq: toNumber(insertResult.rows[0].seq), endOffset };
  }

  private createProducerStream(
    streamId: string,
    makeStream: () => ReadableStream<string>
  ): ReadableStream<string> {
    let cancelled = false;
    let doneResolver: (() => void) | undefined;
    let reader: ReadableStreamDefaultReader<string> | null = null;
    const donePromise = new Promise<void>((resolve) => {
      doneResolver = resolve;
    });
    this.options.waitUntil(donePromise);

    return new ReadableStream<string>({
      start: async (controller) => {
        reader = makeStream().getReader();
        let lastOffset = await this.getLastOffset(streamId);
        try {
          while (!cancelled) {
            const { done, value } = await reader.read();
            if (done) {
              await this.markStreamDone(streamId);
              try {
                await this.notifier.notify({ streamId, event: "done" });
              } catch {
                // notification failures are tolerated; followers will poll the DB
              }
              controller.close();
              doneResolver?.();
              return;
            }
            if (typeof value !== "string") {
              continue;
            }
            controller.enqueue(value);
            const { seq, endOffset } = await this.persistChunk(streamId, value, lastOffset);
            lastOffset = endOffset;
            try {
              await this.notifier.notify({ streamId, event: "chunk", seq });
            } catch {
              // swallow notifier errors; polling path will pick up the chunk
            }
          }
        } catch (error) {
          controller.error(error);
          doneResolver?.();
        }
      },
      cancel: async () => {
        cancelled = true;
        await reader?.cancel();
      },
    });
  }

  private createFollowerStream(streamId: string, skipCharacters: number): ReadableStream<string> {
    let cancelled = false;

    const consume = async (controller: ReadableStreamDefaultController<string>) => {
      let lastSeq = 0;
      let remainingSkip = skipCharacters;
      try {
        while (!cancelled) {
          const chunks = await this.fetchChunks(streamId, lastSeq);
          let emitted = false;
          for (const chunk of chunks) {
            emitted = true;
            lastSeq = chunk.seq;
            const { text, nextSkip } = sliceChunk(chunk, remainingSkip);
            remainingSkip = nextSkip;
            if (text) {
              controller.enqueue(text);
            }
          }

          const status = await this.getStreamStatus(streamId);
          if (status === "done" && !chunks.length) {
            controller.close();
            return;
          }

          if (!emitted) {
            await this.waitForMore(streamId);
          }
        }
      } catch (error) {
        controller.error(error);
      }
    };

    return new ReadableStream<string>({
      start: (controller) => {
        void consume(controller);
      },
      cancel: async () => {
        cancelled = true;
      },
    });
  }

  private async fetchChunks(streamId: string, afterSeq: number): Promise<ChunkRow[]> {
    const { rows } = await this.options.pool.query<{
      seq: number | string;
      chunk: string;
      start_offset: number | string;
      end_offset: number | string;
    }>(
      `SELECT seq, chunk, start_offset, end_offset
       FROM ${this.chunkTable}
       WHERE stream_id = $1 AND seq > $2
       ORDER BY seq ASC`,
      [streamId, afterSeq]
    );
    return rows.map((row) => ({
      seq: toNumber(row.seq),
      chunk: row.chunk,
      startOffset: toNumber(row.start_offset),
      endOffset: toNumber(row.end_offset),
    }));
  }

  private async waitForMore(streamId: string): Promise<void> {
    const payload = await this.notifier.waitFor(streamId);
    if (!payload) {
      await delay(this.options.pollIntervalMs);
    }
  }

  private async getLastOffset(streamId: string): Promise<number> {
    const { rows } = await this.options.pool.query<{ last_offset: number | string }>(
      `SELECT last_offset FROM ${this.sessionTable} WHERE stream_id = $1 LIMIT 1`,
      [streamId]
    );
    if (!rows.length) {
      return 0;
    }
    return toNumber(rows[0].last_offset);
  }

  async close(): Promise<void> {
    await this.notifier.close();
  }
}

function sliceChunk(
  chunk: ChunkRow,
  remainingSkip: number
): { text: string | null; nextSkip: number } {
  const length = chunk.endOffset - chunk.startOffset;
  if (remainingSkip <= 0) {
    return { text: chunk.chunk, nextSkip: 0 };
  }
  if (remainingSkip >= length) {
    return { text: null, nextSkip: remainingSkip - length };
  }
  return { text: chunk.chunk.slice(remainingSkip), nextSkip: 0 };
}
