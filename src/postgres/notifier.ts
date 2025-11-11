import { delay, quoteIdentifier, sanitizeChannelName } from "./utils";
import {
  PostgresClientLike,
  PostgresNotification,
  PostgresPoolLike,
} from "./types";

export type NotifierPayload = {
  streamId: string;
  seq?: number;
  event: "chunk" | "done";
};

export class PostgresNotifier {
  private clientPromise?: Promise<PostgresClientLike | null>;
  private listeners = new Map<string, Set<(payload: NotifierPayload) => void>>();
  private readonly channelName?: string;
  private readonly quotedChannel?: string;
  private notificationHandler?: (payload: PostgresNotification) => void;

  constructor(
    private readonly pool: PostgresPoolLike | undefined,
    channel: string | undefined,
    private readonly listenTimeoutMs: number
  ) {
    if (pool && channel) {
      const sanitized = sanitizeChannelName(channel);
      this.channelName = sanitized;
      this.quotedChannel = quoteIdentifier(sanitized);
    }
  }

  async notify(payload: NotifierPayload): Promise<void> {
    if (!this.pool || !this.quotedChannel) {
      return;
    }
    await this.pool.query("SELECT pg_notify($1, $2)", [this.channelName, JSON.stringify(payload)]);
  }

  async waitFor(streamId: string): Promise<NotifierPayload | null> {
    const client = await this.ensureClient();
    if (!client) {
      await delay(this.listenTimeoutMs);
      return null;
    }
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        cleanup();
        resolve(null);
      }, this.listenTimeoutMs);
      const listener = (payload: NotifierPayload) => {
        if (payload.streamId !== streamId) {
          return;
        }
        cleanup();
        resolve(payload);
      };
      const cleanup = () => {
        clearTimeout(timer);
        const listeners = this.listeners.get(streamId);
        if (listeners) {
          listeners.delete(listener);
          if (listeners.size === 0) {
            this.listeners.delete(streamId);
          }
        }
      };
      const listeners = this.listeners.get(streamId) || new Set();
      listeners.add(listener);
      this.listeners.set(streamId, listeners);
    });
  }

  private async ensureClient(): Promise<PostgresClientLike | null> {
    if (!this.pool || !this.channelName || !this.quotedChannel) {
      return null;
    }
    if (!this.clientPromise) {
      this.clientPromise = (async () => {
        const client = await this.pool.connect();
        if (!client.on) {
          await client.release();
          return null;
        }
        this.notificationHandler = (notification: PostgresNotification) => {
          if (notification.channel !== this.channelName || !notification.payload) {
            return;
          }
          try {
            const payload = JSON.parse(notification.payload) as NotifierPayload;
            const listeners = this.listeners.get(payload.streamId);
            if (!listeners) {
              return;
            }
            for (const listener of listeners) {
              listener(payload);
            }
          } catch {
            // ignore malformed payloads
          }
        };
        client.on("notification", this.notificationHandler);
        await client.query(`LISTEN ${this.quotedChannel}`);
        return client;
      })();
    }
    return this.clientPromise;
  }

  async close(): Promise<void> {
    if (!this.clientPromise) {
      return;
    }
    const client = await this.clientPromise;
    if (!client) {
      return;
    }
    if (this.notificationHandler && client.off) {
      client.off("notification", this.notificationHandler);
    }
    try {
      await client.query(`UNLISTEN ${this.quotedChannel}`);
    } catch {
      // ignore errors while shutting down
    }
    await client.release();
    this.listeners.clear();
    this.clientPromise = undefined;
  }
}
