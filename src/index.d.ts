export interface RateLimit {
  interval: number;
  tasksPerInterval: number;
}

export interface TaskOptions {
  /** Task priority — higher values execute sooner. Default: 0 */
  priority?: number;
  /** Weight for load-tracking. Default: 1 */
  weight?: number;
  /** Task type used for rate limiting and circuit-breaker grouping */
  type?: string;
  /** Deduplication key — identical pending tasks share one execution */
  cacheKey?: string;
  /** Batching key — tasks with the same key are grouped into a batch */
  batchKey?: string;
  /** Unique task identifier (used for dependencies and cancellation) */
  id?: string | number;
  /** Tags for bulk cancellation */
  tags?: string[];
  /** Arbitrary metadata attached to the task */
  metadata?: Record<string, unknown>;
  /** IDs of tasks that must complete before this one starts */
  dependsOn?: (string | number)[];
  /** Unix timestamp (ms) after which the task is dropped as expired */
  deadline?: number;
  /** AbortSignal for external cancellation */
  signal?: AbortSignal;
  /** Per-task timeout in milliseconds */
  timeout?: number;
  /** Maximum retry attempts for this task (overrides global retryCount) */
  retryCount?: number;
  /** Initial retry delay in milliseconds (overrides global initialRetryDelay) */
  retryDelay?: number;
  /**
   * Delay in milliseconds before this task is enqueued.
   * The returned Promise resolves after the task eventually executes.
   */
  delay?: number;
  /** Worker-thread configuration */
  worker?: {
    path: string;
    data?: unknown;
  };
}

export interface Metrics {
  totalTasks: number;
  successfulTasks: number;
  failedTasks: number;
  /** Total tasks sent to the dead-letter queue */
  dlqCount: number;
  throughput: string;
  errorRate: string;
  percentiles: {
    p50: string;
    p90: string;
    p99: string;
  };
}

/** A structured metrics snapshot returned by exportMetrics('json') */
export interface MetricsSnapshot extends Omit<Metrics, "throughput" | "errorRate"> {
  activeCount: number;
  pendingCount: number;
  concurrency: number;
  currentLoad: number;
  throughput: string;
  errorRate: string;
  dlqSize: number;
  uptime: number;
}

export interface WorkerHealth {
  path: string;
  busy: boolean;
  active: boolean;
}

/** A dead-letter queue entry created after all retries are exhausted */
export interface DLQEntry {
  id: string | number | null;
  type: string | null;
  priority: number;
  error: Error;
  errorMessage: string;
  attempts: number;
  failedAt: number;
  metadata: Record<string, unknown> | null;
  tags: string[];
}

export interface GlobalOptions {
  // Queue management
  maxQueueSize?: number;

  // Worker threads
  workerPoolSize?: number;
  workerPathWhitelist?: string[];

  // Batching
  batchSize?: number;
  batchTimeout?: number;

  // Rate limiting
  rateLimits?: Record<string, RateLimit>;

  // Circuit breaker
  circuitThreshold?: number;
  circuitResetTimeout?: number;

  // Retry / backoff
  retryCount?: number;
  initialRetryDelay?: number;
  retryFactor?: number;
  maxRetryDelay?: number;

  // Adaptive concurrency
  adaptive?: boolean;
  minConcurrency?: number;
  maxConcurrency?: number;
  adaptiveLatencyLow?: number;
  adaptiveLatencyHigh?: number;

  // Maintenance
  interval?: number;
  tasksPerInterval?: number;
  completedTaskCleanupMs?: number;
  maxLatencyHistory?: number;
  maxErrorHistory?: number;

  // Priority management
  agingThreshold?: number;
  agingBoost?: number;
  decayThreshold?: number;
  decayAmount?: number;

  // Dead-letter queue
  /** Maximum entries retained in the dead-letter queue. Default: 1000 */
  maxDlqSize?: number;
  /** Called whenever a task is sent to the dead-letter queue */
  onDlq?: (entry: DLQEntry) => void;

  // Events
  emitter?: { emit: (event: string, data: unknown) => void };

  // Lifecycle hooks
  onEnqueue?: (task: unknown) => void;
  onDequeue?: (task: unknown) => void;
  beforeExecute?: (task: unknown) => void;
  afterExecute?: (
    task: unknown,
    profile: {
      duration: number;
      memoryDelta: number;
      status: "success" | "failure";
      error?: string;
    }
  ) => void;
}

/** Metadata returned for each entry in `pool.runningTasks` */
export interface RunningTask {
  id: string | number | null;
  priority: number;
  startTime: number;
  timeout?: number;
}

/** Options accepted by `pool.sizeBy()` */
export interface SizeByOptions {
  /** Count only tasks with this exact priority */
  priority?: number;
  /** Count only tasks with this type */
  type?: string;
  /** Count only tasks that include this tag */
  tag?: string;
}

export interface PoolInstance {
  /** Enqueue a task and return a Promise resolving to its result */
  <T>(task: () => Promise<T> | T, options?: number | TaskOptions): Promise<T>;

  // --- Read-only state ---
  readonly activeCount: number;
  readonly pendingCount: number;
  readonly currentLoad: number;
  readonly concurrency: number;
  readonly isDraining: boolean;
  readonly isPaused: boolean;
  readonly metrics: Metrics;
  /** Snapshot of the dead-letter queue (failed tasks after all retries) */
  readonly dlq: DLQEntry[];
  /**
   * `true` when all concurrency slots are occupied **and** at least one task
   * is waiting in the queue or a batch buffer.
   */
  readonly isSaturated: boolean;
  /** Live snapshot of every task currently executing, with start-time metadata */
  readonly runningTasks: RunningTask[];

  // --- Flow control ---
  pause(): void;
  resume(): void;

  /** Cancel pending tasks matching an ID/tag query or a predicate function */
  cancel(
    query: { id?: string | number; tag?: string } | ((task: unknown) => boolean)
  ): number;

  /** Wait for all queued, batched, blocked, and delayed tasks to finish */
  onIdle(): Promise<{ errors: Error[]; failed: boolean; metrics: Metrics }>;

  /**
   * Resolves when the priority queue (and any batch buffers) becomes empty.
   * Tasks that are already executing may still be running when this resolves.
   * Multiple concurrent callers all resolve at the same time.
   */
  onEmpty(): Promise<void>;

  /**
   * Resolves as soon as `pendingCount` drops strictly below `limit`.
   * Resolves immediately if the condition is already met.
   */
  onSizeLessThan(limit: number): Promise<void>;

  /**
   * Returns a Promise that **rejects** with the error thrown by the next
   * failing task.  Each call creates an independent subscription — useful
   * for racing against `onIdle()` to detect the first failure.
   *
   * @example
   * const winner = await Promise.race([
   *   pool.onIdle().then(() => 'idle'),
   *   pool.onError().catch(() => 'error'),
   * ]);
   */
  onError(): Promise<never>;

  /** Stop accepting new tasks then wait for all in-flight work to finish */
  drain(): Promise<{ errors: Error[]; failed: boolean; metrics: Metrics }>;

  /** Cancel all pending tasks, clear timers, and terminate worker threads */
  clear(): Promise<void>;

  // --- Configuration ---
  setConcurrency(limit: number): void;

  /**
   * Dynamically change the priority of a task that is still waiting in the
   * queue.  The heap is rebuilt immediately so execution order reflects the
   * new value on the very next `next()` tick.
   *
   * @returns `true` if the task was found and updated, `false` otherwise.
   */
  setPriority(id: string | number, priority: number): boolean;

  // --- Inspection ---
  peek(): unknown;
  remove(predicate: (item: unknown) => boolean): boolean;

  /**
   * Count queued tasks that match the given filter criteria.
   * All fields are optional and combined with AND logic.
   *
   * @example
   * pool.sizeBy({ type: 'api' })        // tasks of type "api"
   * pool.sizeBy({ tag: 'critical' })    // tasks tagged "critical"
   * pool.sizeBy({ priority: 10 })       // tasks at exactly priority 10
   */
  sizeBy(options?: SizeByOptions): number;

  // --- Batch helpers ---
  map<T, R>(
    items: T[],
    fn: (item: T) => Promise<R> | R,
    opts?: number | TaskOptions
  ): Promise<R[]>;

  // --- Sub-queues ---
  useQueue(name: string, concurrency?: number): PoolInstance;

  // --- Workers ---
  getWorkerHealth(): WorkerHealth[];

  // --- Observability ---
  /**
   * Export a metrics snapshot.
   * @param format `'prometheus'` returns a Prometheus text-format string;
   *               `'json'` (default) returns a structured {@link MetricsSnapshot}.
   */
  exportMetrics(format: "prometheus"): string;
  exportMetrics(format?: "json"): MetricsSnapshot;
  exportMetrics(format?: "prometheus" | "json"): string | MetricsSnapshot;

  /** Reset all metric counters and latency history */
  resetMetrics(): void;

  /** Clear the dead-letter queue */
  clearDlq(): void;
}

/**
 * Create a new smart-pool instance.
 *
 * @param initialConcurrency Maximum number of concurrent tasks (≥ 1)
 * @param globalOptions      Pool-wide configuration
 */
export default function leap(
  initialConcurrency: number,
  globalOptions?: GlobalOptions
): PoolInstance;