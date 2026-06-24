// resilient-pg-pool — a node-postgres Pool that survives Postgres restarts.
//
// Wraps `pg.Pool` with the things you otherwise rediscover the hard way in production:
//   - TCP keepalive so the driver notices dead sockets before the next query lands on one,
//   - a bounded connection checkout (fail fast instead of hanging on a server's auth timeout),
//   - a `pool.on('error')` handler so an idle-client error (a server restart killing pooled
//     sockets while nothing is checked out) does NOT crash the process as an unhandled event,
//   - automatic backoff-retry of queries on TRANSIENT socket errors (ECONNRESET / EPIPE /
//     ETIMEDOUT / "socket hang up" / connect timeout) — the failure mode where the WHOLE pool
//     goes stale at once after a DB bounce and an instant retry just grabs the next dead socket.
//
// SQL errors (bad query, constraint violations) are NOT retried — they re-fire deterministically.
// Out of scope on purpose: schema routing, migrations, ORM. This is just the resilience layer.
//
// Extracted from the Trimmer toolkit's pg module (the parts that aren't app-specific).
import pg from 'pg';
const DEFAULT_RETRY_DELAYS_MS = [0, 250, 1000];
const sleep = (ms) => (ms > 0 ? new Promise((r) => setTimeout(r, ms)) : Promise.resolve());
/**
 * True for transient socket-level errors where a fresh-connection retry is likely to succeed.
 * Does NOT match SQL errors. Exported so callers can reuse the same classification.
 */
export function isTransientSocketError(e) {
    const err = e;
    const code = err?.code;
    if (code === 'ECONNRESET' || code === 'EPIPE' || code === 'ETIMEDOUT')
        return true;
    return (!!err?.message &&
        /ECONNRESET|EPIPE|ETIMEDOUT|socket hang up|Connection terminated|timeout exceeded when trying to connect/i.test(err.message));
}
/**
 * Create a resilient pool. Accepts everything `pg.PoolConfig` does, plus retry tuning.
 * Restart-survival defaults (keepAlive, 10s connect timeout, 30s idle) are applied unless
 * you override them.
 */
export function createResilientPool(options = {}) {
    const { retryDelaysMs = DEFAULT_RETRY_DELAYS_MS, onIdleError, ...poolConfig } = options;
    const pool = new pg.Pool({
        keepAlive: true,
        keepAliveInitialDelayMillis: 30_000,
        idleTimeoutMillis: 30_000,
        connectionTimeoutMillis: 10_000,
        ...poolConfig,
    });
    // Without this listener, an idle-client error is an unhandled 'error' event → process crash.
    pool.on('error', (e) => {
        if (onIdleError)
            onIdleError(e);
        else
            console.error('[resilient-pg-pool] idle client error (connection dropped):', e.message);
    });
    async function query(sql, params) {
        const attempts = retryDelaysMs.length;
        for (let attempt = 0; attempt < attempts; attempt++) {
            await sleep(retryDelaysMs[attempt]);
            let client;
            try {
                client = await pool.connect();
            }
            catch (e) {
                // Checkout itself can fail transiently (connect timeout, dial refused mid-restart).
                if (attempt < attempts - 1 && isTransientSocketError(e))
                    continue;
                throw e;
            }
            let errored = false;
            try {
                const result = await client.query(sql, params);
                return result.rows;
            }
            catch (e) {
                errored = true;
                client.release(true); // drop the bad connection from the pool
                if (attempt < attempts - 1 && isTransientSocketError(e))
                    continue;
                throw e;
            }
            finally {
                if (!errored)
                    client.release();
            }
        }
        throw new Error('[resilient-pg-pool] query retry logic fell through unexpectedly');
    }
    async function queryOne(sql, params) {
        const rows = await query(sql, params);
        return rows[0] ?? null;
    }
    async function transaction(fn) {
        // Retry only the acquire + BEGIN. Once `fn` runs we do NOT retry — it may have side effects,
        // so a silent re-run risks double execution. Mid-fn failures roll back and rethrow.
        const attempts = retryDelaysMs.length;
        const acquireAndBegin = async () => {
            for (let attempt = 0; attempt < attempts; attempt++) {
                await sleep(retryDelaysMs[attempt]);
                let client;
                try {
                    client = await pool.connect();
                }
                catch (e) {
                    if (attempt < attempts - 1 && isTransientSocketError(e))
                        continue;
                    throw e;
                }
                try {
                    await client.query('BEGIN');
                    return client;
                }
                catch (e) {
                    client.release(true);
                    if (attempt < attempts - 1 && isTransientSocketError(e))
                        continue;
                    throw e;
                }
            }
            throw new Error('[resilient-pg-pool] transaction setup retry fell through unexpectedly');
        };
        const client = await acquireAndBegin();
        let errored = false;
        try {
            const result = await fn(client);
            await client.query('COMMIT');
            return result;
        }
        catch (e) {
            errored = true;
            try {
                await client.query('ROLLBACK');
            }
            catch {
                /* connection already dead */
            }
            client.release(true);
            throw e;
        }
        finally {
            if (!errored)
                client.release();
        }
    }
    async function end() {
        await pool.end();
    }
    return { pool, query, queryOne, transaction, end };
}
