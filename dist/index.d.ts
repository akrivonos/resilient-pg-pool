import pg from 'pg';
export interface ResilientPoolOptions extends pg.PoolConfig {
    /** Pause (ms) before each query attempt. Array length = number of attempts. Default [0, 250, 1000]. */
    retryDelaysMs?: number[];
    /** Called when an idle pooled client errors (server dropped the socket). Default: console.error. */
    onIdleError?: (err: Error) => void;
}
export interface ResilientPool {
    /** The underlying pg.Pool, for anything the wrapper doesn't cover (LISTEN/NOTIFY, etc.). */
    readonly pool: pg.Pool;
    query<T extends Record<string, any> = any>(sql: string, params?: any[]): Promise<T[]>;
    queryOne<T extends Record<string, any> = any>(sql: string, params?: any[]): Promise<T | null>;
    transaction<T>(fn: (client: pg.PoolClient) => Promise<T>): Promise<T>;
    end(): Promise<void>;
}
/**
 * True for transient socket-level errors where a fresh-connection retry is likely to succeed.
 * Does NOT match SQL errors. Exported so callers can reuse the same classification.
 */
export declare function isTransientSocketError(e: unknown): boolean;
/**
 * Create a resilient pool. Accepts everything `pg.PoolConfig` does, plus retry tuning.
 * Restart-survival defaults (keepAlive, 10s connect timeout, 30s idle) are applied unless
 * you override them.
 */
export declare function createResilientPool(options?: ResilientPoolOptions): ResilientPool;
