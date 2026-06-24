# resilient-pg-pool

A thin wrapper over [`pg.Pool`](https://node-postgres.com/) that **survives a Postgres
restart** instead of crashing or hanging. It's the production-hardening you usually bolt on
after your first 3am `ECONNRESET` storm:

- **TCP keepalive** so the driver notices dead sockets before a query lands on one.
- **Bounded checkout** (`connectionTimeoutMillis`) — fail fast with a retryable error instead of
  dangling on the server's auth timeout when it's mid-restart.
- **`pool.on('error')` handled** — an idle pooled client erroring (server dropped the socket
  while nothing was checked out) would otherwise be an *unhandled* `'error'` event and crash
  the process. Here it's logged and the dead client is discarded.
- **Backoff retry on transient socket errors** (`ECONNRESET` / `EPIPE` / `ETIMEDOUT` / "socket
  hang up" / connect timeout). When a DB bounce makes the *whole pool* stale at once, an
  instant retry just grabs the next corpse — so retries are spaced `[0, 250, 1000]ms` by default
  to let keepalive/idle-timeout purge the dead sockets and the server settle.

SQL errors (bad query, constraint violations) are **not** retried — they re-fire
deterministically. Schema routing, migrations, and ORM concerns are intentionally **out of
scope**; this is only the resilience layer.

## Install

```bash
npm install resilient-pg-pool pg
```

`pg` is a peer dependency — you bring your own version.

## Usage

```ts
import { createResilientPool } from 'resilient-pg-pool';

const db = createResilientPool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  // any pg.PoolConfig field works; resilience defaults are applied unless overridden
});

const rows = await db.query('SELECT * FROM users WHERE id = $1', [id]);
const one = await db.queryOne('SELECT now() AS t');

await db.transaction(async (client) => {
  await client.query('INSERT INTO ledger(amount) VALUES ($1)', [100]);
  await client.query('UPDATE balances SET total = total + $1', [100]);
});

await db.end();
```

## API

### `createResilientPool(options?): ResilientPool`

`options` extends [`pg.PoolConfig`](https://node-postgres.com/apis/pool) and adds:

| option | default | meaning |
|---|---|---|
| `retryDelaysMs` | `[0, 250, 1000]` | pause before each attempt; length = number of attempts |
| `onIdleError` | `console.error` | handler for idle-client pool errors |

Returns `{ pool, query, queryOne, transaction, end }`. `pool` is the raw `pg.Pool` for anything
the wrapper doesn't cover (e.g. `LISTEN`/`NOTIFY`).

- `query(sql, params?)` → rows; retries transient socket errors.
- `queryOne(sql, params?)` → first row or `null`.
- `transaction(fn)` → runs `fn(client)` in `BEGIN`/`COMMIT`; retries only the acquire+`BEGIN`,
  never after `fn` starts (to avoid double side effects).

### `isTransientSocketError(e): boolean`

Exported classifier, in case you want the same retry decision elsewhere.

## License

MIT
