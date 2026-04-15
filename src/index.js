import { Worker } from "node:worker_threads";
import { PriorityHeap } from "./heap.js";

export default function leap(initialConcurrency, globalOptions = {}) {
  if (typeof initialConcurrency !== "number" || initialConcurrency < 1) {
    throw new Error("initialConcurrency must be a number >= 1");
  }

  if (
    globalOptions.minConcurrency !== undefined &&
    globalOptions.minConcurrency < 1
  ) {
    throw new Error("minConcurrency must be >= 1");
  }

  if (
    globalOptions.maxConcurrency !== undefined &&
    globalOptions.minConcurrency !== undefined &&
    globalOptions.maxConcurrency < globalOptions.minConcurrency
  ) {
    throw new Error("maxConcurrency must be >= minConcurrency");
  }

  if (
    globalOptions.circuitThreshold !== undefined &&
    globalOptions.circuitThreshold < 1
  ) {
    throw new Error("circuitThreshold must be >= 1");
  }

  if (globalOptions.batchSize !== undefined && globalOptions.batchSize < 1) {
    throw new Error("batchSize must be >= 1");
  }

  if (globalOptions.interval !== undefined && globalOptions.interval < 1) {
    throw new Error("interval must be >= 1");
  }

  const subQueues = new Map();
  const emitter = globalOptions.emitter ?? null;
  const emit = (event, data) => emitter?.emit?.(event, data);

  const { onEnqueue, onDequeue, beforeExecute, afterExecute } = globalOptions;

  const validateWorkerPath = (path) => {
    if (typeof path !== "string" || path.length === 0) {
      throw new Error("Worker path must be a non-empty string");
    }
    if (path.includes("..") || path.includes("~")) {
      throw new Error("Worker path cannot contain '..' or '~'");
    }
    if (globalOptions.workerPathWhitelist) {
      const isWhitelisted = globalOptions.workerPathWhitelist.some((allowed) =>
        path.startsWith(allowed)
      );
      if (!isWhitelisted) {
        throw new Error(`Worker path '${path}' is not in whitelist`);
      }
    }
    return true;
  };

  const createPoolInstance = (concurrency, options) => {
    const queue = new PriorityHeap();
    const blockedTasks = new Map();
    const completedTasks = new Map();
    const pendingCache = new Map();
    const typeRateLimitState = new Map();
    const circuitBreakers = new Map();
    const batchBuffers = new Map();
    const batchTimers = new Map();
    const workerPool = [];
    const activeWorkers = new Set();
    const rateLimitTimers = new Set();
    const abortListeners = new Map();
    const workerWaitingQueue = [];
    const scheduledRateLimitChecks = new Set();
    const delayTimers = new Set();
    const delayedTaskDatas = new Set();
    const deadLetterQueue = [];
    const runningTasksMap = new Map();

    let currentLoad = 0;
    let activeCount = 0;
    let delayedCount = 0;
    let currentConcurrency = concurrency;
    let isDraining = false;
    let isPaused = false;
    let seqCounter = 0;
    let idleResolvers = [];
    let emptyResolvers = [];
    let sizeLimitResolvers = [];
    let errorResolvers = [];
    let errorsInCycle = [];
    let latencies = [];
    let priorityTracker = { min: Infinity, max: -Infinity, count: 0 };

    const config = {
      maxWorkerPoolSize: options.workerPoolSize ?? 0,
      circuitThreshold: options.circuitThreshold ?? 5,
      circuitResetTimeout: options.circuitResetTimeout ?? 30000,
      adaptive: options.adaptive ?? false,
      minC: options.minConcurrency ?? 1,
      maxC: options.maxConcurrency ?? concurrency * 2,
      completedTaskCleanupMs: options.completedTaskCleanupMs ?? 60000,
      batchSize: options.batchSize ?? 10,
      batchTimeout: options.batchTimeout ?? 100,
      initialRetryDelay: options.initialRetryDelay ?? 100,
      retryFactor: options.retryFactor ?? 2,
      maxRetryDelay: options.maxRetryDelay ?? 10000,
      maintenanceInterval: options.interval ?? 1000,
      maxLatencyHistory: options.maxLatencyHistory ?? 10000,
      maxErrorHistory: options.maxErrorHistory ?? 1000,
      maxQueueSize: options.maxQueueSize ?? 10000,
      adaptiveLatencyLow: options.adaptiveLatencyLow ?? 50,
      adaptiveLatencyHigh: options.adaptiveLatencyHigh ?? 200,
      maxDlqSize: options.maxDlqSize ?? 1000,
    };

    const metrics = {
      totalTasks: 0,
      successfulTasks: 0,
      failedTasks: 0,
      dlqCount: 0,
      startTime: Date.now(),
      allLatencies: [],
      latencyLock: false,
      get throughput() {
        const elapsedSec = (Date.now() - this.startTime) / 1000;
        return elapsedSec > 0
          ? (this.successfulTasks / elapsedSec).toFixed(2)
          : "0.00";
      },
      get errorRate() {
        return this.totalTasks > 0
          ? (this.failedTasks / this.totalTasks).toFixed(4)
          : "0.0000";
      },
      get percentiles() {
        this.latencyLock = true;
        const snapshot = [...this.allLatencies];
        this.latencyLock = false;

        if (snapshot.length === 0)
          return { p50: "0.00", p90: "0.00", p99: "0.00" };
        const sorted = snapshot.sort((a, b) => a - b);
        const getP = (p) => {
          const idx = Math.max(0, Math.ceil((p / 100) * sorted.length) - 1);
          return sorted[idx].toFixed(2);
        };
        return { p50: getP(50), p90: getP(90), p99: getP(99) };
      },
    };

    const getCircuitBreaker = (type) => {
      const key = type || "default";
      if (!circuitBreakers.has(key)) {
        circuitBreakers.set(key, {
          openUntil: 0,
          consecutiveFailures: 0,
          lock: false,
        });
      }
      return circuitBreakers.get(key);
    };

    const pushToDlq = (taskData, error, attempts) => {
      const entry = {
        id: taskData.id ?? null,
        type: taskData.type ?? null,
        priority: taskData.priority,
        error,
        errorMessage: error?.message ?? String(error),
        attempts,
        failedAt: Date.now(),
        metadata: taskData.metadata ?? null,
        tags: taskData.tags ?? [],
      };
      deadLetterQueue.push(entry);
      if (deadLetterQueue.length > config.maxDlqSize) {
        deadLetterQueue.shift();
      }
      metrics.dlqCount++;
      emit("task:dlq", entry);
      options.onDlq?.(entry);
    };

    const maintenanceInterval = setInterval(() => {
      if (options.agingThreshold && queue.size() > 0) {
        queue.adjustPriorities(
          options.agingThreshold,
          options.agingBoost || 1,
          false
        );
        rebuildPriorityTracker();
      }
      if (options.decayThreshold && queue.size() > 0) {
        queue.adjustPriorities(
          options.decayThreshold,
          options.decayAmount || 1,
          true
        );
        rebuildPriorityTracker();
      }

      const cutoff = Date.now() - config.completedTaskCleanupMs;
      const toDelete = [];
      for (const [id, time] of completedTasks.entries()) {
        if (time < cutoff) {
          const hasBlockedDeps = Array.from(blockedTasks.values()).some(
            (tasks) => tasks.some((t) => t.dependsOn?.includes(id))
          );
          if (!hasBlockedDeps) toDelete.push(id);
        }
      }
      toDelete.forEach((id) => completedTasks.delete(id));

      for (const [type, breaker] of circuitBreakers.entries()) {
        const now = Date.now();
        if (
          now >= breaker.openUntil &&
          breaker.openUntil > 0 &&
          !breaker.lock
        ) {
          breaker.lock = true;
          breaker.consecutiveFailures = 0;
          breaker.openUntil = 0;
          breaker.lock = false;
          emit("circuit:closed", { type });
        }
      }

      next();
    }, config.maintenanceInterval);

    const rebuildPriorityTracker = () => {
      if (queue.size() === 0) {
        priorityTracker = { min: Infinity, max: -Infinity, count: 0 };
        return;
      }
      let min = Infinity;
      let max = -Infinity;
      for (let i = 0; i < queue.heap.length; i++) {
        const p = queue.heap[i].priority;
        if (p < min) min = p;
        if (p > max) max = p;
      }
      priorityTracker = { min, max, count: queue.size() };
    };

    const updatePriorityOnPush = (priority) => {
      priorityTracker.count++;
      if (priority < priorityTracker.min) priorityTracker.min = priority;
      if (priority > priorityTracker.max) priorityTracker.max = priority;
    };

    const updatePriorityOnPop = () => {
      priorityTracker.count--;
      if (priorityTracker.count === 0) {
        priorityTracker = { min: Infinity, max: -Infinity, count: 0 };
      } else if (priorityTracker.count < 100) {
        rebuildPriorityTracker();
      }
    };

    const adjustConcurrency = () => {
      if (!config.adaptive || latencies.length < 10) return;
      const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length;
      latencies = [];

      if (
        avg < config.adaptiveLatencyLow &&
        currentConcurrency < config.maxC
      ) {
        currentConcurrency++;
        emit("concurrency:adjust", {
          concurrency: currentConcurrency,
          reason: "low_latency",
        });
      } else if (
        avg > config.adaptiveLatencyHigh &&
        currentConcurrency > config.minC
      ) {
        currentConcurrency--;
        emit("concurrency:adjust", {
          concurrency: currentConcurrency,
          reason: "high_latency",
        });
      }
    };

    const checkRateLimit = (type) => {
      const taskType = type || "default";
      const limit =
        options.rateLimits?.[taskType] ||
        (options.tasksPerInterval
          ? {
              interval: options.interval || 1000,
              tasksPerInterval: options.tasksPerInterval,
            }
          : null);

      if (!limit) return false;

      if (!typeRateLimitState.has(taskType)) {
        typeRateLimitState.set(taskType, {
          count: 0,
          windowStart: Date.now(),
        });
      }

      const state = typeRateLimitState.get(taskType);
      const now = Date.now();
      const elapsed = now - state.windowStart;

      if (elapsed >= limit.interval) {
        state.count = 0;
        state.windowStart = now;
      }

      if (state.count >= limit.tasksPerInterval) {
        const remainingTime = limit.interval - elapsed;
        if (!scheduledRateLimitChecks.has(taskType)) {
          scheduledRateLimitChecks.add(taskType);
          const timerId = setTimeout(() => {
            scheduledRateLimitChecks.delete(taskType);
            next();
          }, remainingTime);
          rateLimitTimers.add(timerId);
        }
        return true;
      }

      state.count++;
      return false;
    };

    const getWorker = async (path) => {
      let available = workerPool.find(
        (w) => w.path === path && !w.busy && activeWorkers.has(w.worker)
      );
      if (available) {
        available.busy = true;
        return available;
      }

      if (workerPool.length < config.maxWorkerPoolSize) {
        const worker = new Worker(path);
        const wrapper = { worker, path, busy: true };
        workerPool.push(wrapper);
        activeWorkers.add(worker);
        return wrapper;
      }

      return new Promise((resolve) => {
        workerWaitingQueue.push({ path, resolve });
      });
    };

    const releaseWorker = (wrapper) => {
      wrapper.busy = false;
      const waiting = workerWaitingQueue.findIndex(
        (w) => w.path === wrapper.path
      );
      if (waiting !== -1) {
        const { resolve } = workerWaitingQueue.splice(waiting, 1)[0];
        wrapper.busy = true;
        resolve(wrapper);
      }
    };

    const terminateWorker = async (wrapper) => {
      try {
        activeWorkers.delete(wrapper.worker);
        await wrapper.worker.terminate();
      } catch (err) {
        emit("worker:terminate:error", { path: wrapper.path, error: err });
      }
    };

    const isQueueEmpty = () =>
      queue.size() === 0 && batchBuffers.size === 0 && delayedCount === 0;

    const resolveEmpty = () => {
      if (emptyResolvers.length === 0) return;
      const resolvers = emptyResolvers;
      emptyResolvers = [];
      for (const r of resolvers) r();
    };

    const checkSizeLimitResolvers = () => {
      if (sizeLimitResolvers.length === 0) return;
      const pending =
        queue.size() +
        Array.from(blockedTasks.values()).reduce((a, t) => a + t.length, 0) +
        Array.from(batchBuffers.values()).reduce((a, t) => a + t.length, 0) +
        delayedCount;
      sizeLimitResolvers = sizeLimitResolvers.filter(({ limit, resolve }) => {
        if (pending < limit) {
          resolve();
          return false;
        }
        return true;
      });
    };

    const notifyError = (err) => {
      if (errorResolvers.length === 0) return;
      const resolvers = errorResolvers;
      errorResolvers = [];
      for (const reject of resolvers) reject(err);
    };

    const executeTask = async (taskData) => {
      const {
        task,
        resolve,
        reject,
        type,
        retryCount,
        timeout,
        signal,
        worker: workerOptions,
      } = taskData;
      const breaker = getCircuitBreaker(type);
      const now = Date.now();

      if (now < breaker.openUntil) {
        reject(
          new Error(`Circuit breaker open for type: ${type || "default"}`)
        );
        return;
      }

      currentLoad += taskData.weight || 1;
      taskData.isActive = true;
      activeCount++;
      runningTasksMap.set(taskData, {
        id: taskData.id ?? null,
        priority: taskData.priority,
        startTime: Date.now(),
        timeout: taskData.timeout ?? undefined,
      });
      beforeExecute?.(taskData);

      const startTime = Date.now();
      const startMem = process.memoryUsage().heapUsed;
      let retries = 0;
      let lastError = null;
      const maxRetries = retryCount ?? options.retryCount ?? 0;

      while (retries <= maxRetries) {
        try {
          let result;
          let timeoutId;
          let abortHandler;

          const taskTimeout = timeout ?? 0;

          const executePromise = workerOptions
            ? (async () => {
                const wrapper = await getWorker(workerOptions.path);
                return new Promise((res, rej) => {
                  const handleMessage = (msg) => {
                    if (msg.type === "result") {
                      wrapper.worker.off("message", handleMessage);
                      wrapper.worker.off("error", handleError);
                      releaseWorker(wrapper);
                      res(msg.data);
                    }
                  };
                  const handleError = (err) => {
                    wrapper.worker.off("message", handleMessage);
                    wrapper.worker.off("error", handleError);
                    releaseWorker(wrapper);
                    breaker.consecutiveFailures++;
                    if (
                      breaker.consecutiveFailures >= config.circuitThreshold
                    ) {
                      breaker.openUntil =
                        Date.now() + config.circuitResetTimeout;
                      emit("circuit:open", { type: type || "default" });
                    }
                    rej(err);
                  };
                  wrapper.worker.on("message", handleMessage);
                  wrapper.worker.on("error", handleError);
                  wrapper.worker.postMessage(workerOptions.data);
                });
              })()
            : task();

          const promises = [executePromise];

          if (taskTimeout > 0) {
            const timeoutPromise = new Promise((_, rej) => {
              timeoutId = setTimeout(
                () => rej(new Error("Task timeout")),
                taskTimeout
              );
            });
            promises.push(timeoutPromise);
          }

          if (signal) {
            const abortPromise = new Promise((_, rej) => {
              abortHandler = () => rej(new Error("Task aborted"));
              signal.addEventListener("abort", abortHandler);
            });
            promises.push(abortPromise);
          }

          result = await Promise.race(promises);

          if (timeoutId) clearTimeout(timeoutId);
          if (abortHandler && signal) {
            signal.removeEventListener("abort", abortHandler);
            abortListeners.delete(taskData);
          }

          const duration = Date.now() - startTime;
          const memDelta = process.memoryUsage().heapUsed - startMem;

          if (!metrics.latencyLock) {
            metrics.allLatencies.push(duration);
            if (metrics.allLatencies.length > config.maxLatencyHistory) {
              metrics.allLatencies.shift();
            }
          }

          latencies.push(duration);
          if (latencies.length > 100) latencies.shift();

          breaker.consecutiveFailures = 0;
          metrics.successfulTasks++;
          afterExecute?.(taskData, {
            duration,
            memoryDelta: memDelta,
            status: "success",
          });
          runningTasksMap.delete(taskData);
          resolve(result);
          return;
        } catch (err) {
          lastError = err;
          retries++;

          if (retries > maxRetries) {
            const duration = Date.now() - startTime;
            const memDelta = process.memoryUsage().heapUsed - startMem;

            if (
              err.message !== "Task aborted" &&
              err.message !== "Task timeout"
            ) {
              breaker.consecutiveFailures++;
              if (breaker.consecutiveFailures >= config.circuitThreshold) {
                breaker.openUntil = Date.now() + config.circuitResetTimeout;
                emit("circuit:open", { type: type || "default" });
              }

              if (maxRetries > 0) {
                pushToDlq(taskData, err, retries);
              }
            }

            metrics.failedTasks++;
            errorsInCycle.push(err);
            if (errorsInCycle.length > config.maxErrorHistory) {
              errorsInCycle.shift();
            }
            afterExecute?.(taskData, {
              duration,
              memoryDelta: memDelta,
              status: "failure",
              error: err.message,
            });
            notifyError(err);
            runningTasksMap.delete(taskData);
            reject(err);
            return;
          }

          const delay = Math.min(
            config.initialRetryDelay *
              Math.pow(config.retryFactor, retries - 1),
            config.maxRetryDelay
          );
          emit("task:retry", {
            id: taskData.id,
            attempt: retries,
            delay,
            error: lastError?.message,
          });
          await new Promise((r) => setTimeout(r, delay));
        }
      }
    };

    const flushBatch = (batchKey) => {
      const buffer = batchBuffers.get(batchKey);
      if (!buffer || buffer.length === 0) return;

      batchBuffers.delete(batchKey);
      const timerId = batchTimers.get(batchKey);
      if (timerId) {
        clearTimeout(timerId);
        batchTimers.delete(batchKey);
      }

      for (const taskData of buffer) {
        if (taskData.dependsOn && taskData.dependsOn.length > 0) {
          checkDependencies(taskData);
        } else {
          queue.push(taskData);
          updatePriorityOnPush(taskData.priority);
        }
      }
      next();
    };

    const checkDependencies = (taskData) => {
      const unresolved = taskData.dependsOn.filter(
        (id) => !completedTasks.has(id)
      );
      if (unresolved.length === 0) {
        queue.push(taskData);
        updatePriorityOnPush(taskData.priority);
        next();
      } else {
        for (const depId of unresolved) {
          if (!blockedTasks.has(depId)) blockedTasks.set(depId, []);
          const blocked = blockedTasks.get(depId);
          if (!blocked.includes(taskData)) {
            blocked.push(taskData);
          }
        }
      }
    };

    const resolveIdle = () => {
      const resolvers = idleResolvers;
      const errors = errorsInCycle;
      idleResolvers = [];
      errorsInCycle = [];
      for (const r of resolvers) {
        r({ errors, failed: errors.length > 0, metrics });
      }
    };

    const isIdle = () =>
      activeCount === 0 &&
      queue.size() === 0 &&
      blockedTasks.size === 0 &&
      batchBuffers.size === 0 &&
      delayedCount === 0;

    const next = () => {
      if (isPaused || isDraining) {
        if (isDraining && isIdle() && idleResolvers.length > 0) resolveIdle();
        return;
      }

      if (queue.size() === 0) {
        if (isQueueEmpty()) resolveEmpty();
        checkSizeLimitResolvers();
        if (isIdle() && idleResolvers.length > 0) resolveIdle();
        return;
      }

      while (activeCount < currentConcurrency && queue.size() > 0) {
        const taskData = queue.peek();
        if (!taskData) break;

        const weight = taskData.weight || 1;
        if (activeCount > 0 && currentLoad + weight > currentConcurrency) {
          break;
        }

        if (checkRateLimit(taskData.type)) break;

        queue.pop();
        updatePriorityOnPop();
        onDequeue?.(taskData);

        if (taskData.deadline && Date.now() > taskData.deadline) {
          taskData.reject(new Error("Task deadline exceeded"));
          metrics.failedTasks++;
          continue;
        }

        metrics.totalTasks++;

        executeTask(taskData).finally(() => {
          currentLoad -= taskData.weight || 1;
          taskData.isActive = false;
          activeCount--;
          runningTasksMap.delete(taskData);

          if (taskData.id) {
            completedTasks.set(taskData.id, Date.now());
            const blocked = blockedTasks.get(taskData.id);
            if (blocked) {
              blockedTasks.delete(taskData.id);
              for (const waiting of blocked) {
                checkDependencies(waiting);
              }
            }
          }

          if (taskData.cacheKey) {
            pendingCache.delete(taskData.cacheKey);
          }

          checkSizeLimitResolvers();
          adjustConcurrency();
          next();
        });
      }

      if (isQueueEmpty()) resolveEmpty();
      checkSizeLimitResolvers();
    };

    const enqueueTaskData = (taskData, opts) => {
      if (opts.batchKey) {
        if (!batchBuffers.has(opts.batchKey))
          batchBuffers.set(opts.batchKey, []);
        const buffer = batchBuffers.get(opts.batchKey);
        buffer.push(taskData);

        if (buffer.length >= config.batchSize) {
          flushBatch(opts.batchKey);
        } else if (!batchTimers.has(opts.batchKey)) {
          const timerId = setTimeout(() => {
            flushBatch(opts.batchKey);
          }, config.batchTimeout);
          batchTimers.set(opts.batchKey, timerId);
        }
      } else if (taskData.dependsOn.length > 0) {
        checkDependencies(taskData);
      } else {
        queue.push(taskData);
        updatePriorityOnPush(taskData.priority);
        next();
      }
    };

    const poolInstance = (task, options) => {
      if (typeof task !== "function") {
        throw new Error("Task must be a function");
      }

      const opts =
        typeof options === "number" ? { priority: options } : options || {};

      if (opts.worker) validateWorkerPath(opts.worker.path);

      if (opts.cacheKey) {
        const cached = pendingCache.get(opts.cacheKey);
        if (cached && cached.task === task) return cached.promise;
      }

      if (isDraining) {
        return Promise.reject(new Error("Pool is draining"));
      }

      if (queue.size() >= config.maxQueueSize) {
        return Promise.reject(new Error("Queue is full"));
      }

      let taskData;
      const taskPromise = new Promise((resolve, reject) => {
        taskData = {
          task,
          resolve,
          reject,
          seq: seqCounter++,
          priority: opts.priority ?? 0,
          weight: opts.weight ?? 1,
          dependsOn: opts.dependsOn || [],
          cycles: 0,
          isActive: false,
          ...opts,
        };
        onEnqueue?.(taskData);

        if (opts.signal) {
          const abortHandler = () => {
            reject(new Error("Task aborted"));
            poolInstance.cancel((t) => t === taskData);
          };
          opts.signal.addEventListener("abort", abortHandler);
          abortListeners.set(taskData, abortHandler);
        }

        if (opts.delay && opts.delay > 0) {
          delayedCount++;
          delayedTaskDatas.add(taskData);
          const timerId = setTimeout(() => {
            delayTimers.delete(timerId);
            delayedTaskDatas.delete(taskData);
            delayedCount--;
            enqueueTaskData(taskData, opts);
          }, opts.delay);
          delayTimers.add(timerId);
        } else {
          enqueueTaskData(taskData, opts);
        }
      });

      taskPromise.catch(() => {
        const handler = abortListeners.get(taskData);
        if (handler && taskData?.signal) {
          taskData.signal.removeEventListener("abort", handler);
          abortListeners.delete(taskData);
        }

        if (opts.batchKey) {
          const buffer = batchBuffers.get(opts.batchKey);
          if (buffer) {
            const filtered = buffer.filter((t) => t !== taskData);
            if (filtered.length === 0) {
              batchBuffers.delete(opts.batchKey);
              const timer = batchTimers.get(opts.batchKey);
              if (timer) {
                clearTimeout(timer);
                batchTimers.delete(opts.batchKey);
              }
            } else {
              batchBuffers.set(opts.batchKey, filtered);
            }
          }
        }
      });

      if (opts.cacheKey) {
        pendingCache.set(opts.cacheKey, { task, promise: taskPromise });
      }

      return taskPromise;
    };

    poolInstance.pause = () => {
      isPaused = true;
    };
    poolInstance.resume = () => {
      isPaused = false;
      next();
    };
    poolInstance.onIdle = () => {
      for (const key of batchBuffers.keys()) flushBatch(key);
      if (isIdle()) {
        return Promise.resolve({ errors: [], failed: false, metrics });
      }
      return new Promise((r) => {
        idleResolvers.push(r);
      });
    };
    poolInstance.drain = () => {
      isDraining = true;
      return poolInstance.onIdle().then((result) => {
        isDraining = false;
        return result;
      });
    };

    poolInstance.onEmpty = () => {
      for (const key of batchBuffers.keys()) flushBatch(key);
      if (isQueueEmpty()) return Promise.resolve();
      return new Promise((r) => emptyResolvers.push(r));
    };

    poolInstance.onSizeLessThan = (limit) => {
      if (poolInstance.pendingCount < limit) return Promise.resolve();
      return new Promise((r) =>
        sizeLimitResolvers.push({ limit, resolve: r })
      );
    };

    poolInstance.onError = () => {
      return new Promise((_, reject) => errorResolvers.push(reject));
    };

    poolInstance.cancel = (query) => {
      const match =
        typeof query === "function"
          ? query
          : (t) =>
              (query.id && t.id === query.id) ||
              (query.tag && t.tags?.includes(query.tag));
      let count = 0;
      let minRemoved = Infinity;
      let maxRemoved = -Infinity;

      queue.remove((t) => {
        if (match(t)) {
          if (t.cacheKey) pendingCache.delete(t.cacheKey);

          const handler = abortListeners.get(t);
          if (handler && t.signal) {
            t.signal.removeEventListener("abort", handler);
          }
          abortListeners.delete(t);

          if (t.priority < minRemoved) minRemoved = t.priority;
          if (t.priority > maxRemoved) maxRemoved = t.priority;

          t.reject(new Error("Task cancelled via API"));
          count++;
          return true;
        }
        return false;
      });

      for (const [batchKey, buffer] of batchBuffers.entries()) {
        const initialLen = buffer.length;
        const filtered = buffer.filter((task) => {
          if (match(task)) {
            if (task.cacheKey) pendingCache.delete(task.cacheKey);

            const handler = abortListeners.get(task);
            if (handler && task.signal) {
              task.signal.removeEventListener("abort", handler);
            }
            abortListeners.delete(task);

            task.reject(new Error("Task cancelled via API"));
            count++;
            return false;
          }
          return true;
        });

        if (filtered.length === 0) {
          batchBuffers.delete(batchKey);
          if (batchTimers.has(batchKey)) {
            clearTimeout(batchTimers.get(batchKey));
            batchTimers.delete(batchKey);
          }
        } else if (filtered.length !== initialLen) {
          batchBuffers.set(batchKey, filtered);
        }
      }

      const cancelledInBlocked = new Set();
      for (const [depId, tasks] of blockedTasks.entries()) {
        const initialLen = tasks.length;
        const filtered = tasks.filter((task) => {
          if (match(task)) {
            if (!cancelledInBlocked.has(task)) {
              if (task.cacheKey) pendingCache.delete(task.cacheKey);

              const handler = abortListeners.get(task);
              if (handler && task.signal) {
                task.signal.removeEventListener("abort", handler);
              }
              abortListeners.delete(task);

              task.reject(new Error("Task cancelled via API"));
              count++;
              cancelledInBlocked.add(task);
            }
            return false;
          }
          return true;
        });

        if (filtered.length === 0) {
          blockedTasks.delete(depId);
        } else if (filtered.length !== initialLen) {
          blockedTasks.set(depId, filtered);
        }
      }

      if (
        count > 0 &&
        (minRemoved <= priorityTracker.min || maxRemoved >= priorityTracker.max)
      ) {
        rebuildPriorityTracker();
      }

      return count;
    };

    poolInstance.setConcurrency = (limit) => {
      currentConcurrency = limit;
      next();
    };

    poolInstance.peek = () => {
      return queue.peek();
    };

    poolInstance.remove = (predicate) => {
      const result = queue.remove(predicate);
      if (result) rebuildPriorityTracker();
      return result;
    };

    poolInstance.setPriority = (id, priority) => {
      const task = queue.heap.find((t) => t.id === id);
      if (!task) return false;
      task.priority = priority;
      queue._rebuild();
      rebuildPriorityTracker();
      return true;
    };

    poolInstance.sizeBy = (options = {}) => {
      return queue.heap.filter((task) => {
        if (
          options.priority !== undefined &&
          task.priority !== options.priority
        )
          return false;
        if (options.type !== undefined && task.type !== options.type)
          return false;
        if (options.tag !== undefined && !task.tags?.includes(options.tag))
          return false;
        return true;
      }).length;
    };

    poolInstance.clear = async () => {
      clearInterval(maintenanceInterval);
      rateLimitTimers.forEach(clearTimeout);
      rateLimitTimers.clear();

      delayTimers.forEach(clearTimeout);
      delayTimers.clear();
      for (const taskData of delayedTaskDatas) {
        taskData.reject(new Error("Pool cleared"));
      }
      delayedTaskDatas.clear();
      delayedCount = 0;

      const queuedTasks = [...queue.heap];
      queue.clear();

      activeCount = 0;
      currentLoad = 0;

      for (const task of queuedTasks) {
        task.reject(new Error("Pool cleared"));
      }

      for (const [, tasks] of blockedTasks) {
        for (const task of tasks) {
          task.reject(new Error("Pool cleared"));
        }
      }
      blockedTasks.clear();

      for (const [, buffer] of batchBuffers) {
        for (const task of buffer) {
          task.reject(new Error("Pool cleared"));
        }
      }
      batchBuffers.clear();

      batchTimers.forEach(clearTimeout);
      batchTimers.clear();

      completedTasks.clear();
      pendingCache.clear();
      circuitBreakers.clear();
      runningTasksMap.clear();

      for (const [task, handler] of abortListeners.entries()) {
        if (task.signal) {
          task.signal.removeEventListener("abort", handler);
        }
      }
      abortListeners.clear();

      emptyResolvers = [];
      sizeLimitResolvers = [];
      errorResolvers = [];

      const resolvers = idleResolvers;
      idleResolvers = [];
      for (const r of resolvers) {
        r({ errors: errorsInCycle, failed: errorsInCycle.length > 0, metrics });
      }
      errorsInCycle = [];

      const terminationPromises = workerPool.map((w) => terminateWorker(w));
      await Promise.allSettled(terminationPromises);

      workerPool.length = 0;
      activeWorkers.clear();
      workerWaitingQueue.length = 0;

      if (poolInstance.useQueue) {
        for (const sub of subQueues.values()) {
          await sub.clear();
        }
        subQueues.clear();
      }
    };

    poolInstance.map = async (items, fn, opts) => {
      const optsObj =
        typeof opts === "number" ? { priority: opts } : opts || {};
      const throwOnError = optsObj.throwOnError ?? true;

      const results = [];
      for (const item of items) {
        results.push(poolInstance(() => fn(item), optsObj));
      }

      if (throwOnError) {
        return Promise.all(results);
      } else {
        const settled = await Promise.allSettled(results);
        return settled.map((result) =>
          result.status === "fulfilled" ? result.value : result.reason
        );
      }
    };

    poolInstance.getWorkerHealth = () => {
      return workerPool.map((w) => ({
        path: w.path,
        busy: w.busy,
        active: activeWorkers.has(w.worker),
      }));
    };

    poolInstance.exportMetrics = (format = "json") => {
      const m = metrics;
      const p = m.percentiles;
      const pending = poolInstance.pendingCount;

      if (format === "prometheus") {
        const lines = [
          `# HELP smart_pool_total_tasks_total Total number of tasks processed`,
          `# TYPE smart_pool_total_tasks_total counter`,
          `smart_pool_total_tasks_total ${m.totalTasks}`,
          `# HELP smart_pool_successful_tasks_total Total successful tasks`,
          `# TYPE smart_pool_successful_tasks_total counter`,
          `smart_pool_successful_tasks_total ${m.successfulTasks}`,
          `# HELP smart_pool_failed_tasks_total Total failed tasks`,
          `# TYPE smart_pool_failed_tasks_total counter`,
          `smart_pool_failed_tasks_total ${m.failedTasks}`,
          `# HELP smart_pool_dlq_tasks_total Total tasks sent to dead-letter queue`,
          `# TYPE smart_pool_dlq_tasks_total counter`,
          `smart_pool_dlq_tasks_total ${m.dlqCount}`,
          `# HELP smart_pool_active_tasks Currently executing tasks`,
          `# TYPE smart_pool_active_tasks gauge`,
          `smart_pool_active_tasks ${activeCount}`,
          `# HELP smart_pool_pending_tasks Tasks waiting in queue (includes delayed and blocked)`,
          `# TYPE smart_pool_pending_tasks gauge`,
          `smart_pool_pending_tasks ${pending}`,
          `# HELP smart_pool_concurrency_limit Current maximum concurrency`,
          `# TYPE smart_pool_concurrency_limit gauge`,
          `smart_pool_concurrency_limit ${currentConcurrency}`,
          `# HELP smart_pool_current_load Aggregate weight of active tasks`,
          `# TYPE smart_pool_current_load gauge`,
          `smart_pool_current_load ${currentLoad}`,
          `# HELP smart_pool_dlq_size Current dead-letter queue depth`,
          `# TYPE smart_pool_dlq_size gauge`,
          `smart_pool_dlq_size ${deadLetterQueue.length}`,
          `# HELP smart_pool_latency_p50_milliseconds 50th percentile task latency`,
          `# TYPE smart_pool_latency_p50_milliseconds gauge`,
          `smart_pool_latency_p50_milliseconds ${p.p50}`,
          `# HELP smart_pool_latency_p90_milliseconds 90th percentile task latency`,
          `# TYPE smart_pool_latency_p90_milliseconds gauge`,
          `smart_pool_latency_p90_milliseconds ${p.p90}`,
          `# HELP smart_pool_latency_p99_milliseconds 99th percentile task latency`,
          `# TYPE smart_pool_latency_p99_milliseconds gauge`,
          `smart_pool_latency_p99_milliseconds ${p.p99}`,
        ];
        return lines.join("\n");
      }

      return {
        totalTasks: m.totalTasks,
        successfulTasks: m.successfulTasks,
        failedTasks: m.failedTasks,
        dlqCount: m.dlqCount,
        activeCount,
        pendingCount: pending,
        concurrency: currentConcurrency,
        currentLoad,
        throughput: m.throughput,
        errorRate: m.errorRate,
        percentiles: p,
        dlqSize: deadLetterQueue.length,
        uptime: Date.now() - m.startTime,
      };
    };

    poolInstance.clearDlq = () => {
      deadLetterQueue.length = 0;
    };

    poolInstance.resetMetrics = () => {
      metrics.totalTasks = 0;
      metrics.successfulTasks = 0;
      metrics.failedTasks = 0;
      metrics.dlqCount = 0;
      metrics.startTime = Date.now();
      metrics.allLatencies = [];
      errorsInCycle = [];
    };

    Object.defineProperties(poolInstance, {
      activeCount: { get: () => activeCount },
      pendingCount: {
        get: () => {
          const blockedCount = Array.from(blockedTasks.values()).reduce(
            (acc, tasks) => acc + tasks.length,
            0
          );
          const batchCount = Array.from(batchBuffers.values()).reduce(
            (acc, tasks) => acc + tasks.length,
            0
          );
          return queue.size() + blockedCount + batchCount + delayedCount;
        },
      },
      currentLoad: { get: () => currentLoad },
      concurrency: { get: () => currentConcurrency },
      isDraining: { get: () => isDraining },
      isPaused: { get: () => isPaused },
      metrics: { get: () => metrics },
      dlq: { get: () => [...deadLetterQueue] },
      isSaturated: {
        get: () =>
          activeCount >= currentConcurrency &&
          (queue.size() > 0 || batchBuffers.size > 0),
      },
      runningTasks: {
        get: () => Array.from(runningTasksMap.values()),
      },
    });

    return poolInstance;
  };

  const mainPool = createPoolInstance(initialConcurrency, globalOptions);
  mainPool.useQueue = (name, concurrency = initialConcurrency) => {
    if (!subQueues.has(name))
      subQueues.set(name, createPoolInstance(concurrency, globalOptions));
    return subQueues.get(name);
  };

  return mainPool;
}