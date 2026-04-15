# Smart Pool [![CI](https://github.com/Marcus-Johnson/smart-pool/actions/workflows/ci.yml/badge.svg)](https://github.com/Marcus-Johnson/smart-pool/actions)

A high-performance, feature-rich task queue and concurrency management library for Node.js. Built for production workloads requiring advanced scheduling, priority management, batching, rate limiting, circuit breaking, worker thread support, and adaptive concurrency.

---

## What's new in 1.2.0

| Feature | Summary |
|---|---|
| **`onEmpty()`** | Resolves when the queue (and batch buffers) drains to zero; tasks already executing may still be running |
| **`onSizeLessThan(limit)`** | Resolves as soon as `pendingCount` drops strictly below `limit`; resolves immediately if already satisfied |
| **`onError()`** | Each call returns a distinct Promise that rejects with the **next** task error — ideal for racing against `onIdle()` |
| **`setPriority(id, priority)`** | Dynamically re-order a task still waiting in the queue; heap rebuilt immediately |
| **`isSaturated`** | `true` when every concurrency slot is occupied **and** tasks are waiting |
| **`runningTasks`** | Live array of `{ id, priority, startTime, timeout }` for every in-flight task |
| **`sizeBy(options)`** | Count queued tasks matching a `priority`, `type`, and/or `tag` filter |

## What's new in 1.1.0

| Feature | Summary |
|---|---|
| **Delayed jobs** | `{ delay: ms }` option defers task enqueue; `onIdle()` and `clear()` are fully aware |
| **Dead-letter queue** | Tasks that exhaust all retries land in `pool.dlq`; configurable `onDlq` callback + `task:dlq` event |
| **`exportMetrics(format)`** | Export a JSON snapshot or Prometheus text-format scrape endpoint |
| **`resetMetrics()`** | Zero all counters and latency history at any time |
| **`clearDlq()`** | Empty the dead-letter queue |
| **Multi-caller `onIdle()`** | Multiple simultaneous `onIdle()` callers now all resolve correctly |
| **`drain()` reset** | `isDraining` resets to `false` after drain completes; pool accepts tasks again |

---

## Features

- **Priority Queue** — Binary max-heap with dynamic priority adjustments
- **Concurrency Control** — Fixed or adaptive concurrency limits
- **Rate Limiting** — Per-type rate limits with token bucket algorithm
- **Circuit Breakers** — Automatic failure detection and recovery
- **Task Batching** — Group similar tasks for efficient processing
- **Worker Threads** — Offload CPU-intensive tasks to worker threads
- **Task Dependencies** — Execute tasks only after dependencies complete
- **Caching** — Deduplicate identical pending tasks
- **Retry Logic** — Exponential backoff with configurable limits
- **Delayed Jobs** — Schedule tasks to run after a configurable delay *(new in 1.1)*
- **Dead-letter Queue** — Capture exhausted-retry failures for inspection *(new in 1.1)*
- **Abort Support** — Cancel tasks via `AbortSignal`
- **Priority Aging** — Prevent starvation with automatic priority boosts
- **Priority Decay** — Reduce priority of stale high-priority tasks
- **Dynamic Priority** — Change a queued task's priority at runtime via `setPriority()` *(new in 1.2)*
- **Metrics** — Real-time performance tracking with percentiles
- **Prometheus Export** — One-call Prometheus text-format metrics *(new in 1.1)*
- **`onEmpty()`** — Await the moment the queue goes empty *(new in 1.2)*
- **`onSizeLessThan()`** — Back-pressure primitive; await a queue depth threshold *(new in 1.2)*
- **`onError()`** — Per-subscription error observation *(new in 1.2)*
- **`isSaturated`** — Instant saturation check *(new in 1.2)*
- **`runningTasks`** — Live snapshot of in-flight tasks *(new in 1.2)*
- **`sizeBy()`** — Filtered queue depth queries *(new in 1.2)*
- **Lifecycle Hooks** — Execute code at key points in task execution
- **Sub-queues** — Isolated queues with independent concurrency limits
- **Weight-based Load** — Track and limit load by task weight

---

## Installation

```bash
npm install smart-pool
```

---

## Quick Start

```javascript
import smartPool from 'smart-pool';

const pool = smartPool(5);

const result = await pool(async () => {
  return 'Task completed';
});

console.log(result);
```

---

## API Reference

### `smartPool(concurrency, options)`

Creates a new task pool instance.

**Parameters:**
- `concurrency` (number): Maximum number of concurrent tasks (minimum 1)
- `options` (object, optional): Global configuration options

**Returns:** `PoolInstance`

---

### Pool Instance

#### `pool(task, options)`

Enqueue and execute a task.

**Task Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `priority` | number | `0` | Higher values execute sooner |
| `weight` | number | `1` | Task weight for load tracking |
| `type` | string | — | Groups tasks for rate limiting and circuit breaking |
| `cacheKey` | string | — | Deduplicate identical pending tasks |
| `batchKey` | string | — | Group tasks for batch processing |
| `id` | string \| number | — | Unique task identifier |
| `tags` | string[] | — | Tags for filtering/cancellation |
| `metadata` | object | — | Custom metadata |
| `dependsOn` | array | — | Task IDs that must complete first |
| `deadline` | number | — | Unix timestamp when task expires |
| `signal` | AbortSignal | — | Abort signal for cancellation |
| `timeout` | number | — | Per-task timeout in milliseconds |
| `retryCount` | number | — | Maximum retry attempts (overrides global) |
| `retryDelay` | number | — | Initial retry delay in ms (overrides global) |
| `delay` | number | — | **[1.1]** Milliseconds to wait before enqueuing the task |
| `worker` | object | — | Worker thread config (`path`, `data`) |

**Returns:** `Promise` resolving to task result

---

#### `pool.map(items, fn, options)`

Map a function over array items using the pool.

```javascript
const results = await pool.map(
  [1, 2, 3, 4, 5],
  async (n) => n * 2,
  { priority: 5 }
);
```

---

#### `pool.pause()` / `pool.resume()`

Pause and resume task execution. Queued tasks remain in queue.

---

#### `pool.cancel(query)`

Cancel pending tasks matching an ID/tag query or predicate.

```javascript
pool.cancel({ id: 'task-1' });
pool.cancel({ tag: 'batch-1' });
pool.cancel((task) => task.priority < 5);
```

**Returns:** Number of cancelled tasks.

---

#### `pool.onIdle()`

Wait for all tasks to complete — including batched, blocked, and delayed tasks.
Multiple concurrent callers are all resolved when the pool goes idle.

**Returns:** `Promise<{ errors, failed, metrics }>`

---

#### `pool.drain()`

Stop accepting new tasks and wait for all in-flight work to finish.
`isDraining` is reset to `false` after completion so the pool can be reused.

**Returns:** `Promise<{ errors, failed, metrics }>`

---

#### `pool.onEmpty()`

Wait for the priority queue (and all batch buffers) to reach zero.  Tasks that are already executing may still be running when this resolves — use `onIdle()` if you need everything to be finished.

Multiple concurrent callers all resolve at the same moment.

**Returns:** `Promise<void>`

---

#### `pool.onSizeLessThan(limit)`

Resolves as soon as `pendingCount` drops strictly below `limit`.  Useful as a back-pressure primitive when producing tasks faster than they are consumed.

**Parameters:**
- `limit` (number): Threshold; resolves immediately if already satisfied.

**Returns:** `Promise<void>`

---

#### `pool.onError()`

Returns a `Promise` that **rejects** with the error from the next failing task.  Every call creates an independent subscription, so multiple callers each receive the rejection independently.  Typically used in a `Promise.race()` alongside `onIdle()`.

```javascript
const winner = await Promise.race([
  pool.onIdle().then(() => 'idle'),
  pool.onError().catch(() => 'error'),
]);
```

**Returns:** `Promise<never>`

---

#### `pool.clear()`

Cancel all pending tasks (including delayed ones), reject any waiting `onIdle()` callers, and terminate all worker threads.

**Returns:** `Promise<void>`

---

#### `pool.setConcurrency(limit)`

Dynamically adjust the concurrency limit.

---

#### `pool.peek()`

View the next task without removing it.

---

#### `pool.remove(predicate)`

Remove tasks from queue matching a predicate. Returns `boolean`.

---

#### `pool.setPriority(id, priority)` *(new in 1.2)*

Dynamically change the priority of a task that is still waiting in the queue.  The heap is rebuilt immediately so the task's execution order reflects the new value on the very next scheduling tick.

**Parameters:**
- `id` (string | number): The task's `id` as set at enqueue time.
- `priority` (number): New priority value.

**Returns:** `true` if the task was found and updated, `false` if no queued task has that ID.

```javascript
pool.setPriority('slow-report', 100); // jump it to the front
```

---

#### `pool.sizeBy(options)` *(new in 1.2)*

Returns the count of queued tasks that match all of the supplied filter criteria.  All fields are optional; omitted fields are ignored.

| Field | Type | Description |
|---|---|---|
| `priority` | number | Exact priority value to match |
| `type` | string | Task type to match |
| `tag` | string | Tag that the task must include |

```javascript
pool.sizeBy({ type: 'api' })       // how many "api" tasks are queued?
pool.sizeBy({ tag: 'critical' })   // tasks tagged "critical"
pool.sizeBy({ priority: 10 })      // tasks at exactly priority 10
```

**Returns:** `number`

---

#### `pool.useQueue(name, concurrency)`

Create or get an isolated sub-queue with independent concurrency control.

```javascript
const apiQueue = pool.useQueue('api', 3);
const dbQueue  = pool.useQueue('database', 5);
```

---

#### `pool.getWorkerHealth()`

Returns `Array<{ path, busy, active }>` for all worker threads.

---

#### `pool.exportMetrics(format?)` *(new in 1.1)*

Export a metrics snapshot in the requested format.

```javascript
// Structured JSON snapshot
const snap = pool.exportMetrics();        // or 'json'
console.log(snap.throughput, snap.p99);

// Prometheus text format — drop behind /metrics
const text = pool.exportMetrics('prometheus');
res.setHeader('Content-Type', 'text/plain; version=0.0.4');
res.end(text);
```

**JSON snapshot fields:** `totalTasks`, `successfulTasks`, `failedTasks`, `dlqCount`, `activeCount`, `pendingCount`, `concurrency`, `currentLoad`, `throughput`, `errorRate`, `percentiles` (p50/p90/p99), `dlqSize`, `uptime`.

**Prometheus gauges/counters exported:**
- `smart_pool_total_tasks_total`
- `smart_pool_successful_tasks_total`
- `smart_pool_failed_tasks_total`
- `smart_pool_dlq_tasks_total`
- `smart_pool_active_tasks`
- `smart_pool_pending_tasks`
- `smart_pool_concurrency_limit`
- `smart_pool_current_load`
- `smart_pool_dlq_size`
- `smart_pool_latency_p50_milliseconds`
- `smart_pool_latency_p90_milliseconds`
- `smart_pool_latency_p99_milliseconds`

---

#### `pool.resetMetrics()` *(new in 1.1)*

Zero all counters, latency history, and DLQ count. Useful for per-window benchmarks.

```javascript
pool.resetMetrics();
```

---

#### `pool.clearDlq()` *(new in 1.1)*

Empty the in-memory dead-letter queue.

---

#### Properties

| Property | Type | Description |
|---|---|---|
| `activeCount` | number | Currently executing tasks |
| `pendingCount` | number | Tasks in queue, batch, blocked, or delayed |
| `currentLoad` | number | Aggregate weight of active tasks |
| `concurrency` | number | Current concurrency limit |
| `isDraining` | boolean | Whether pool is in drain mode |
| `isPaused` | boolean | Whether pool is paused |
| `metrics` | Metrics | Live performance metrics |
| `dlq` | DLQEntry[] | **[1.1]** Snapshot of the dead-letter queue |
| `isSaturated` | boolean | **[1.2]** `true` when all slots are occupied and at least one task is waiting |
| `runningTasks` | RunningTask[] | **[1.2]** Live array of `{ id, priority, startTime, timeout }` for every in-flight task |

---

### Global Options

```javascript
const pool = smartPool(5, {
  // Queue Management
  maxQueueSize: 10000,

  // Adaptive Concurrency
  adaptive: true,
  minConcurrency: 2,
  maxConcurrency: 20,
  adaptiveLatencyLow: 50,
  adaptiveLatencyHigh: 200,

  // Rate Limiting
  rateLimits: {
    api:      { interval: 1000, tasksPerInterval: 10 },
    database: { interval: 100,  tasksPerInterval: 5  }
  },

  // Circuit Breaker
  circuitThreshold: 5,
  circuitResetTimeout: 30000,

  // Batching
  batchSize: 10,
  batchTimeout: 100,

  // Retry
  retryCount: 3,
  initialRetryDelay: 100,
  retryFactor: 2,
  maxRetryDelay: 10000,

  // Dead-letter Queue (1.1)
  maxDlqSize: 1000,
  onDlq: (entry) => console.error('DLQ:', entry.errorMessage),

  // Priority Management
  agingThreshold: 5,
  agingBoost: 1,
  decayThreshold: 10,
  decayAmount: 1,

  // Worker Threads
  workerPoolSize: 4,
  workerPathWhitelist: ['/app/workers/'],

  // Maintenance
  interval: 1000,
  completedTaskCleanupMs: 60000,
  maxLatencyHistory: 10000,
  maxErrorHistory: 1000,

  // Events
  emitter: eventEmitter,

  // Lifecycle Hooks
  onEnqueue:     (task)          => console.log('Enqueued:', task.id),
  onDequeue:     (task)          => console.log('Dequeued:', task.id),
  beforeExecute: (task)          => console.log('Executing:', task.id),
  afterExecute:  (task, profile) => console.log('Completed:', task.id, profile.duration + 'ms'),
});
```

---

## Tutorials

### 1. Basic Task Queue

```javascript
import smartPool from 'smart-pool';

const pool = smartPool(3);

await pool(async () => console.log('Low priority'),  1);
await pool(async () => console.log('High priority'), 10);

await pool.onIdle();
```

### 2. API Rate Limiting

```javascript
const pool = smartPool(10, {
  rateLimits: {
    github:  { interval: 3600000, tasksPerInterval: 5000 },
    twitter: { interval: 900000,  tasksPerInterval: 300  }
  }
});

const user = await pool(
  async () => fetch('https://api.github.com/users/alice').then(r => r.json()),
  { type: 'github', priority: 5 }
);
```

### 3. Delayed Jobs *(new in 1.1)*

Schedule tasks to run after a delay — useful for retry cool-downs, rate limiting, or cron-like scheduling.

```javascript
const pool = smartPool(5);

// Runs immediately
pool(async () => sendWelcomeEmail(userId));

// Runs after 60 seconds
pool(async () => sendFollowUpEmail(userId), { delay: 60_000 });

// Runs after 1 hour
pool(async () => sendSummaryEmail(userId), { delay: 3_600_000 });

await pool.onIdle();
```

Delayed tasks are included in `pool.pendingCount`. `pool.clear()` cancels them and rejects their Promises.

### 4. Dead-letter Queue *(new in 1.1)*

Inspect failures that exhausted all retries.

```javascript
const pool = smartPool(5, {
  retryCount: 3,
  initialRetryDelay: 500,
  retryFactor: 2,
  onDlq: (entry) => {
    // e.g. persist to database, alert, etc.
    console.error(`[DLQ] Task ${entry.id} failed after ${entry.attempts} attempts:`, entry.errorMessage);
  }
});

try {
  await pool(async () => callUnstableAPI(), { id: 'sync-job', type: 'api' });
} catch (err) {
  // Inspect the DLQ after the fact
  const dlq = pool.dlq;
  console.log(`DLQ depth: ${dlq.length}`);
  for (const entry of dlq) {
    console.log(entry.id, entry.attempts, entry.errorMessage);
  }
  pool.clearDlq();
}
```

> **Note:** Tasks with no `retryCount` (single-attempt) are intentionally excluded from the DLQ to avoid noise.

### 5. Prometheus Metrics Endpoint *(new in 1.1)*

```javascript
import http from 'node:http';
import smartPool from 'smart-pool';

const pool = smartPool(10);

http.createServer((req, res) => {
  if (req.url === '/metrics') {
    const body = pool.exportMetrics('prometheus');
    res.writeHead(200, { 'Content-Type': 'text/plain; version=0.0.4; charset=utf-8' });
    return res.end(body);
  }
  res.end('ok');
}).listen(9090);
```

### 6. JSON Metrics Snapshot *(new in 1.1)*

```javascript
const snap = pool.exportMetrics('json');

console.log({
  throughput:  snap.throughput,
  errorRate:   snap.errorRate,
  p50:         snap.percentiles.p50,
  p99:         snap.percentiles.p99,
  dlq:         snap.dlqSize,
  uptime:      snap.uptime,
});
```

### 7. Metrics Window with `resetMetrics` *(new in 1.1)*

Measure throughput per time window rather than since startup.

```javascript
const pool = smartPool(10);

setInterval(async () => {
  const snap = pool.exportMetrics();
  console.log(`[1s window] throughput=${snap.throughput} p99=${snap.percentiles.p99}ms errors=${snap.failedTasks}`);
  pool.resetMetrics();
}, 1000);
```

### 8. Circuit Breaker

```javascript
const pool = smartPool(5, {
  circuitThreshold: 3,
  circuitResetTimeout: 30000,
  retryCount: 2,
  initialRetryDelay: 1000,
  onDlq: (e) => alerting.send(`Circuit tripped for ${e.type}: ${e.errorMessage}`)
});

async function callUnstableAPI(endpoint) {
  return pool(
    async () => {
      const res = await fetch(endpoint);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res.json();
    },
    { type: 'unstable-api' }
  );
}
```

### 9. Task Dependencies (DAG)

```javascript
const pool = smartPool(10);

await pool(() => db.users.create({ name: 'Alice' }),              { id: 'create-user' });
await pool(() => db.profiles.create({ bio: 'Developer' }),        { id: 'create-profile', dependsOn: ['create-user'] });
await pool(() => sendWelcomeEmail(userId),                        { dependsOn: ['create-user', 'create-profile'] });
```

### 10. Worker Threads

**worker.js:**
```javascript
import { parentPort } from 'node:worker_threads';

parentPort.on('message', ({ data }) => {
  let sum = 0;
  for (let i = 0; i < data; i++) sum += Math.sqrt(i);
  parentPort.postMessage({ type: 'result', data: sum });
});
```

**main.js:**
```javascript
const pool = smartPool(5, {
  workerPoolSize: 4,
  workerPathWhitelist: ['/app/workers/']
});

const results = await pool.map(
  [1_000_000, 2_000_000, 3_000_000],
  (n) => pool(() => {}, { worker: { path: '/app/workers/worker.js', data: n } })
);
```

### 11. Adaptive Concurrency

```javascript
const pool = smartPool(5, {
  adaptive: true,
  minConcurrency: 2,
  maxConcurrency: 20,
  adaptiveLatencyLow: 50,
  adaptiveLatencyHigh: 200,
});

setInterval(() => {
  console.log(`concurrency=${pool.concurrency} active=${pool.activeCount} pending=${pool.pendingCount}`);
}, 1000);
```

### 12. Sub-queues

```javascript
const pool = smartPool(10);
const criticalQueue    = pool.useQueue('critical',   5);
const backgroundQueue  = pool.useQueue('background', 2);

await criticalQueue(async () => processPayment());
backgroundQueue(async () => generateReport());
```

### 13. Timeout and Abort

```javascript
const pool = smartPool(5);
const controller = new AbortController();

const t1 = pool(async () => longRunningOperation(), { timeout: 5000 });
const t2 = pool(async () => anotherOperation(),      { signal: controller.signal });

setTimeout(() => controller.abort(), 2000);

try {
  await Promise.all([t1, t2]);
} catch (err) {
  console.error('Cancelled:', err.message);
}
```

### 14. Task Caching

```javascript
const pool = smartPool(5);

async function fetchUser(userId) {
  return pool(
    async () => fetch(`https://api.example.com/users/${userId}`).then(r => r.json()),
    { cacheKey: `user-${userId}` }
  );
}

// Only one HTTP request is made despite three calls
const [a, b, c] = await Promise.all([
  fetchUser(123), fetchUser(123), fetchUser(123)
]);
```

### 15. Task Batching

```javascript
const pool = smartPool(5, { batchSize: 50, batchTimeout: 100 });

async function insertUser(user) {
  return pool(
    async (batch) => {
      const ids = await db.users.insertMany(batch.map(t => t.data));
      return ids[batch.indexOf(user)];
    },
    { batchKey: 'user-insert', data: user }
  );
}

const ids = await Promise.all(users.map(insertUser));
```

### 16. Lifecycle Hooks

```javascript
const pool = smartPool(5, {
  onEnqueue:     (task)          => console.log(`[ENQUEUE]  ${task.id} priority=${task.priority}`),
  onDequeue:     (task)          => console.log(`[DEQUEUE]  ${task.id}`),
  beforeExecute: (task)          => console.log(`[EXECUTE]  ${task.id}`),
  afterExecute:  (task, profile) => {
    console.log(`[COMPLETE] ${task.id} ${profile.duration}ms status=${profile.status}`);
    if (profile.error) console.error(`[ERROR]    ${profile.error}`);
  },
});
```

---

## Events

When an `emitter` is provided, the pool emits these events:

| Event | Payload | Description |
|---|---|---|
| `circuit:open` | `{ type }` | Circuit breaker opened |
| `circuit:closed` | `{ type }` | Circuit breaker closed |
| `concurrency:adjust` | `{ concurrency, reason }` | Adaptive concurrency changed |
| `task:retry` | `{ id, attempt, delay, error }` | Task retry attempt |
| `task:timeout` | `{ id }` | Task timed out |
| `task:dlq` | `DLQEntry` | **[1.1]** Task sent to dead-letter queue |
| `batch:flush` | `{ key }` | Batch flushed |

---

## Best Practices

1. **Choose appropriate concurrency** — Start conservative; use `adaptive: true` to tune automatically.
2. **Type-based rate limiting** — Respect external API quotas with `rateLimits`.
3. **Circuit breakers** — Protect against cascading failures with `circuitThreshold`.
4. **Batch similar operations** — Use `batchKey` to reduce overhead for bulk work.
5. **Monitor the DLQ** — Wire `onDlq` to your alerting system so persistent failures are visible.
6. **Expose `/metrics`** — Use `exportMetrics('prometheus')` to integrate with Grafana/Prometheus.
7. **Use sub-queues for isolation** — Separate critical and background work with `useQueue`.
8. **Handle errors gracefully** — Always `.catch()` individual task promises.
9. **Clean up on shutdown** — Call `pool.drain()` or `pool.clear()` in `SIGTERM` handlers.

```javascript
process.on('SIGTERM', async () => {
  await pool.drain();
  process.exit(0);
});
```

---

## Performance Tips

- **Batch when possible** — `batchKey` dramatically reduces per-item overhead for DB/API writes.
- **Enable adaptive mode** — Let the pool optimize concurrency automatically under load.
- **Use worker threads** — Offload CPU-intensive tasks to avoid blocking the event loop.
- **Cache duplicate requests** — `cacheKey` deduplicates in-flight identical tasks.
- **Use `resetMetrics()` in benchmarks** — Isolate measurement windows from warmup.
- **Monitor p99** — Use latency percentiles to identify tail-latency bottlenecks before they affect SLOs.

### 17. Dynamic Priority (`setPriority`) *(new in 1.2)*

Promote or demote a queued task without cancelling and re-enqueuing it.

```javascript
const pool = smartPool(1);

pool(() => delay(50)); // keep slot busy
pool(() => processReport(), { id: 'report', priority: 0 });
pool(() => processPayment(), { id: 'payment', priority: 0 });

// Payment just became urgent — jump it ahead
pool.setPriority('payment', 100);

await pool.onIdle(); // payment runs before report
```

`setPriority` returns `false` if no queued task has that ID (e.g. it is already executing or has finished).

---

### 18. `onEmpty` — react when the queue drains *(new in 1.2)*

`onEmpty()` resolves the moment the queue (and batch buffers) reach zero.  Unlike `onIdle()`, tasks that are already executing may still be running.

```javascript
const pool = smartPool(2);

for (let i = 0; i < 10; i++) {
  pool(() => processItem(i));
}

// Fires as soon as nothing is left waiting — concurrency slots may still be busy
pool.onEmpty().then(() => console.log('Queue drained, workers finishing up'));

await pool.onIdle(); // wait for the last task to complete
```

---

### 19. `onSizeLessThan` — back-pressure *(new in 1.2)*

Pause a producer until the consumer catches up.

```javascript
const pool = smartPool(5);

for (const batch of paginatedSource()) {
  // Don't let the queue grow past 50 items
  await pool.onSizeLessThan(50);
  for (const item of batch) {
    pool(() => processItem(item));
  }
}

await pool.onIdle();
```

---

### 20. `onError` — observe failures without catching every task *(new in 1.2)*

Each `onError()` call is an independent one-shot subscription.  Race it against `onIdle()` to abort early on the first failure.

```javascript
const pool = smartPool(5);

for (const job of jobs) pool(() => runJob(job));

const outcome = await Promise.race([
  pool.onIdle().then(() => ({ ok: true })),
  pool.onError().catch((err) => ({ ok: false, err })),
]);

if (!outcome.ok) {
  console.error('First failure:', outcome.err.message);
  await pool.clear(); // cancel everything else
}
```

---

### 21. `isSaturated` and `runningTasks` *(new in 1.2)*

`isSaturated` is a fast boolean — no need to compare `activeCount` and `concurrency` manually.
`runningTasks` gives you a live snapshot of what is actually executing right now.

```javascript
const pool = smartPool(4);

setInterval(() => {
  if (pool.isSaturated) {
    console.log('Pool is saturated — consider increasing concurrency');
  }
  for (const t of pool.runningTasks) {
    const elapsed = Date.now() - t.startTime;
    if (elapsed > 5000) {
      console.warn(`Task ${t.id} has been running for ${elapsed}ms`);
    }
  }
}, 1000);
```

---

### 22. `sizeBy` — filtered queue introspection *(new in 1.2)*

Query how many tasks of a particular shape are currently waiting.

```javascript
const pool = smartPool(5);

// ... many tasks enqueued with types and tags ...

console.log('API tasks waiting:',      pool.sizeBy({ type: 'api' }));
console.log('Critical tasks waiting:', pool.sizeBy({ tag: 'critical' }));
console.log('Priority-10 tasks:',      pool.sizeBy({ priority: 10 }));
```

---

## Changelog

### 1.2.0

- **[feat]** `pool.onEmpty()` — resolves when queue and batch buffers reach zero; multiple concurrent callers all resolve
- **[feat]** `pool.onSizeLessThan(limit)` — back-pressure helper; resolves immediately if already below limit
- **[feat]** `pool.onError()` — independent per-call subscriptions that reject with the next task error
- **[feat]** `pool.setPriority(id, priority)` — dynamically reorder a queued task; returns `false` if not found
- **[feat]** `pool.isSaturated` — boolean property; `true` when all slots are occupied and tasks are waiting
- **[feat]** `pool.runningTasks` — live `RunningTask[]` snapshot of every in-flight task with `id`, `priority`, `startTime`, `timeout`
- **[feat]** `pool.sizeBy(options)` — count queued tasks by `priority`, `type`, and/or `tag`

### 1.1.0

- **[feat]** `delay` task option — defer task enqueue by N milliseconds; `onIdle()` and `clear()` are fully aware
- **[feat]** Dead-letter queue — `pool.dlq`, `pool.clearDlq()`, `onDlq` callback, `task:dlq` event, `maxDlqSize` option
- **[feat]** `pool.exportMetrics('json' | 'prometheus')` — structured snapshot or Prometheus text format
- **[feat]** `pool.resetMetrics()` — zero all counters and latency history on demand
- **[fix]** `onIdle()` now supports multiple concurrent callers; all resolve correctly
- **[fix]** `drain()` resets `isDraining = false` after completion so the pool is reusable
- **[fix]** `clear()` now rejects delayed tasks and resolves pending `onIdle()` callers

### 1.0.0

- Initial release

---

## License

MIT