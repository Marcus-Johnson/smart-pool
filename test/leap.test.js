import test from "node:test";
import assert from "node:assert";
import leap from "../src/index.js";

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

// ─────────────────────────────────────────────
//  EXISTING TESTS (1.0)
// ─────────────────────────────────────────────

test("Core: should respect strict concurrency limits", async () => {
  const pool = leap(3);
  try {
    let active = 0;
    let maxActive = 0;
    const task = async () => {
      active++;
      maxActive = Math.max(maxActive, active);
      await delay(20);
      active--;
    };
    await Promise.all(Array.from({ length: 10 }, () => pool(task)));
    assert.strictEqual(maxActive, 3, "Should never exceed concurrency of 3");
  } finally {
    await pool.clear();
  }
});

test("Core: should manage load via task weights", async () => {
  const pool = leap(10);
  try {
    let ran = false;
    pool(() => delay(50), { weight: 10 });
    const p2 = pool(() => { ran = true; }, { weight: 1 });
    await delay(20);
    assert.strictEqual(ran, false, "Weighted task should be blocked");
    await p2;
    assert.strictEqual(ran, true);
  } finally {
    await pool.clear();
  }
});

test("Core: should resolve onIdle when all work is finished", async () => {
  const pool = leap(2);
  try {
    let count = 0;
    for (let i = 0; i < 5; i++) {
      pool(async () => { await delay(10); count++; });
    }
    const result = await pool.onIdle();
    assert.strictEqual(count, 5);
    assert.strictEqual(result.failed, false);
  } finally {
    await pool.clear();
  }
});

test("Priority: should execute tasks in priority order (Max-Heap)", async () => {
  const pool = leap(1);
  try {
    const order = [];
    pool(() => delay(30));
    pool(() => order.push("low"), { priority: 0 });
    pool(() => order.push("high"), { priority: 100 });
    pool(() => order.push("mid"), { priority: 50 });
    await pool.onIdle();
    assert.deepStrictEqual(order, ["high", "mid", "low"]);
  } finally {
    await pool.clear();
  }
});

test("Priority: should prevent starvation via aging boost", async () => {
  const pool = leap(1, { agingThreshold: 1, agingBoost: 20, interval: 20 });
  try {
    const order = [];
    pool(() => delay(60));
    pool(() => order.push("starved"), { priority: 0 });
    await delay(10);
    pool(() => order.push("new-high"), { priority: 10 });
    await pool.onIdle();
    assert.strictEqual(order[0], "starved");
  } finally {
    await pool.clear();
  }
});

test("Resilience: should retry failed tasks with exponential backoff", async () => {
  const pool = leap(1, { retryCount: 3, initialRetryDelay: 10, retryFactor: 2 });
  try {
    let attempts = 0;
    const result = await pool(() => {
      attempts++;
      if (attempts < 3) throw new Error("Temp Fail");
      return "OK";
    });
    assert.strictEqual(result, "OK");
    assert.strictEqual(attempts, 3);
  } finally {
    await pool.clear();
  }
});

test("Resilience: should trip circuit breaker on repeated failures", async () => {
  const pool = leap(1, { circuitThreshold: 2, circuitResetTimeout: 50 });
  try {
    const fail = () => Promise.reject(new Error("Fail"));
    await Promise.allSettled([pool(fail), pool(fail)]);
    await assert.rejects(pool(() => "should not run"), /Circuit breaker open/);
    await delay(60);
    const success = await pool(() => "recovered");
    assert.strictEqual(success, "recovered");
  } finally {
    await pool.clear();
  }
});

test("Advanced: should respect task dependencies (DAG)", async () => {
  const pool = leap(2);
  try {
    const log = [];
    const t1 = pool(() => log.push("A"), { id: "A" });
    const t3 = pool(() => log.push("C"), { id: "C", dependsOn: ["B"] });
    const t2 = pool(() => log.push("B"), { id: "B", dependsOn: ["A"] });
    await Promise.all([t1, t2, t3]);
    assert.deepStrictEqual(log, ["A", "B", "C"]);
  } finally {
    await pool.clear();
  }
});

test("Advanced: should batch tasks by key", async () => {
  const pool = leap(5, { batchSize: 3, batchTimeout: 1000 });
  try {
    let execCount = 0;
    const task = () => { execCount++; return Promise.resolve(); };
    const p1 = pool(task, { batchKey: "b1" });
    const p2 = pool(task, { batchKey: "b1" });
    const p3 = pool(task, { batchKey: "b1" });
    await Promise.all([p1, p2, p3]);
    assert.strictEqual(execCount, 3);
  } finally {
    await pool.clear();
  }
});

test("Advanced: should enforce rate limits per type", async () => {
  const pool = leap(5, {
    rateLimits: { api: { interval: 100, tasksPerInterval: 2 } },
  });
  try {
    const start = Date.now();
    const tasks = Array.from({ length: 3 }, () =>
      pool(() => Promise.resolve(), { type: "api" })
    );
    await Promise.all(tasks);
    const duration = Date.now() - start;
    assert.ok(duration >= 100, `Rate limit ignored: took ${duration}ms`);
  } finally {
    await pool.clear();
  }
});

test("Lifecycle: should deduplicate work via cacheKey", async () => {
  const pool = leap(1);
  try {
    let calls = 0;
    const sharedTask = async () => { calls++; await delay(20); return "data"; };
    const [r1, r2] = await Promise.all([
      pool(sharedTask, { cacheKey: "fetch-1" }),
      pool(sharedTask, { cacheKey: "fetch-1" }),
    ]);
    assert.strictEqual(calls, 1, "Task should only execute once");
    assert.strictEqual(r1, r2);
  } finally {
    await pool.clear();
  }
});

test("Lifecycle: should cancel tasks by ID or Tag", async () => {
  const pool = leap(1);
  try {
    pool(() => delay(50));
    const p1 = pool(() => "never", { id: "cancel-me" });
    const p2 = pool(() => "never", { tags: ["cleanup"] });
    const cancelled = pool.cancel({ id: "cancel-me" });
    const cancelledTag = pool.cancel({ tag: "cleanup" });
    assert.strictEqual(cancelled, 1);
    assert.strictEqual(cancelledTag, 1);
    await assert.rejects(p1, /Task cancelled/);
    await assert.rejects(p2, /Task cancelled/);
  } finally {
    await pool.clear();
  }
});

test("Lifecycle: should clear all resources and terminate workers", async () => {
  const pool = leap(1);
  pool(() => delay(100));
  pool(() => "pending");
  assert.strictEqual(pool.pendingCount, 1);
  await pool.clear();
  assert.strictEqual(pool.pendingCount, 0);
  assert.strictEqual(pool.activeCount, 0);
});

test("Observability: should trigger afterExecute profile hooks", async () => {
  let profileResult = null;
  const pool = leap(1, {
    afterExecute: (task, profile) => { profileResult = profile; },
  });
  try {
    await pool(() => delay(20));
    assert.ok(profileResult.duration >= 20);
    assert.strictEqual(profileResult.status, "success");
    assert.ok(profileResult.memoryDelta !== undefined);
  } finally {
    await pool.clear();
  }
});

test("Observability: should calculate correct latency percentiles", async () => {
  const pool = leap(1);
  try {
    await pool(() => delay(10));
    await pool(() => delay(30));
    const { p50, p99 } = pool.metrics.percentiles;
    assert.ok(parseFloat(p50) >= 8, `Expected p50 >= 8, got ${p50}`);
    assert.ok(parseFloat(p99) >= 25, `Expected p99 >= 25, got ${p99}`);
  } finally {
    await pool.clear();
  }
});

// ─────────────────────────────────────────────
//  NEW TESTS (1.1.0)
// ─────────────────────────────────────────────

test("1.1 delay: should defer task enqueue by the specified ms", async () => {
  const pool = leap(2);
  try {
    const log = [];
    const start = Date.now();
    pool(() => log.push("immediate"));
    pool(() => log.push("delayed"), { delay: 80 });
    await pool.onIdle();
    const elapsed = Date.now() - start;
    assert.ok(elapsed >= 80, `Expected at least 80ms, got ${elapsed}ms`);
    assert.strictEqual(log[0], "immediate");
    assert.strictEqual(log[1], "delayed");
  } finally {
    await pool.clear();
  }
});

test("1.1 delay: delayed tasks should appear in pendingCount", async () => {
  const pool = leap(2);
  try {
    pool(() => Promise.resolve(), { delay: 300 });
    await Promise.resolve(); // flush microtask queue
    assert.strictEqual(pool.pendingCount, 1, "Delayed task should count as pending");
  } finally {
    await pool.clear();
  }
});

test("1.1 delay: pool.clear() should cancel pending delayed tasks", async () => {
  const pool = leap(2);
  let executed = false;
  const p = pool(() => { executed = true; }, { delay: 500 });
  await pool.clear();
  await assert.rejects(p, /Pool cleared/);
  assert.strictEqual(executed, false, "Delayed task must not have run");
});

test("1.1 delay: onIdle should wait for delayed tasks to complete", async () => {
  const pool = leap(2);
  try {
    let done = false;
    pool(async () => { done = true; }, { delay: 50 });
    await pool.onIdle();
    assert.strictEqual(done, true, "onIdle must wait for delayed tasks");
  } finally {
    await pool.clear();
  }
});

test("1.1 DLQ: exhausted retries should land in dead-letter queue", async () => {
  const pool = leap(1, { retryCount: 2, initialRetryDelay: 5, retryFactor: 1 });
  try {
    await assert.rejects(
      pool(() => { throw new Error("always fails"); }),
      /always fails/
    );
    const dlq = pool.dlq;
    assert.strictEqual(dlq.length, 1, "Failed task should be in DLQ");
    assert.ok(dlq[0].errorMessage.includes("always fails"));
    assert.strictEqual(dlq[0].attempts, 3); // 1 initial + 2 retries
  } finally {
    await pool.clear();
  }
});

test("1.1 DLQ: tasks without retries should NOT land in DLQ", async () => {
  const pool = leap(1); // no retryCount configured
  try {
    await assert.rejects(
      pool(() => { throw new Error("one-shot fail"); }),
      /one-shot fail/
    );
    assert.strictEqual(pool.dlq.length, 0, "Single-attempt failures should not pollute DLQ");
  } finally {
    await pool.clear();
  }
});

test("1.1 DLQ: clearDlq should empty the dead-letter queue", async () => {
  const pool = leap(1, { retryCount: 1, initialRetryDelay: 5 });
  try {
    await assert.rejects(pool(() => { throw new Error("fail"); }));
    assert.strictEqual(pool.dlq.length, 1);
    pool.clearDlq();
    assert.strictEqual(pool.dlq.length, 0);
  } finally {
    await pool.clear();
  }
});

test("1.1 DLQ: onDlq callback should fire when task enters DLQ", async () => {
  let dlqEntry = null;
  const pool = leap(1, {
    retryCount: 1,
    initialRetryDelay: 5,
    onDlq: (entry) => { dlqEntry = entry; },
  });
  try {
    await assert.rejects(pool(() => { throw new Error("callback test"); }));
    assert.ok(dlqEntry !== null, "onDlq callback should have fired");
    assert.ok(dlqEntry.errorMessage.includes("callback test"));
    assert.ok(typeof dlqEntry.failedAt === "number");
  } finally {
    await pool.clear();
  }
});

test("1.1 exportMetrics: JSON format contains all expected fields", async () => {
  const pool = leap(3);
  try {
    await pool(() => delay(10));
    const snapshot = pool.exportMetrics("json");
    const numFields = ["totalTasks","successfulTasks","failedTasks","dlqCount",
      "activeCount","pendingCount","concurrency","currentLoad","dlqSize","uptime"];
    for (const f of numFields) {
      assert.strictEqual(typeof snapshot[f], "number", `${f} should be a number`);
    }
    assert.strictEqual(typeof snapshot.throughput, "string");
    assert.strictEqual(typeof snapshot.errorRate, "string");
    assert.ok(snapshot.percentiles?.p50 !== undefined);
    assert.strictEqual(snapshot.concurrency, 3);
    assert.strictEqual(snapshot.totalTasks, 1);
    assert.strictEqual(snapshot.successfulTasks, 1);
  } finally {
    await pool.clear();
  }
});

test("1.1 exportMetrics: Prometheus format is valid text-format", async () => {
  const pool = leap(2);
  try {
    await pool(() => delay(5));
    const text = pool.exportMetrics("prometheus");
    assert.strictEqual(typeof text, "string");
    assert.ok(text.includes("# HELP"));
    assert.ok(text.includes("# TYPE"));
    assert.ok(text.includes("smart_pool_total_tasks_total"));
    assert.ok(text.includes("smart_pool_latency_p99_milliseconds"));
    assert.ok(text.includes("smart_pool_dlq_size"));
    // Every non-comment line must be "<name> <number>"
    const metricLines = text.split("\n").filter((l) => l.length > 0 && !l.startsWith("#"));
    for (const line of metricLines) {
      const parts = line.split(" ");
      assert.strictEqual(parts.length, 2, `Bad metric line: "${line}"`);
      assert.ok(!isNaN(Number(parts[1])), `Non-numeric value: "${line}"`);
    }
  } finally {
    await pool.clear();
  }
});

test("1.1 resetMetrics: should zero all counters", async () => {
  const pool = leap(2);
  try {
    await pool(() => delay(5));
    await pool(() => delay(5));
    assert.strictEqual(pool.metrics.totalTasks, 2);
    pool.resetMetrics();
    assert.strictEqual(pool.metrics.totalTasks, 0);
    assert.strictEqual(pool.metrics.successfulTasks, 0);
    assert.strictEqual(pool.metrics.failedTasks, 0);
    assert.strictEqual(pool.metrics.dlqCount, 0);
    assert.strictEqual(pool.metrics.allLatencies.length, 0);
  } finally {
    await pool.clear();
  }
});

test("1.1 onIdle: multiple concurrent callers should all resolve", async () => {
  const pool = leap(2);
  try {
    let count = 0;
    for (let i = 0; i < 4; i++) {
      pool(async () => { await delay(20); count++; });
    }
    const [r1, r2] = await Promise.all([pool.onIdle(), pool.onIdle()]);
    assert.strictEqual(count, 4);
    assert.strictEqual(r1.failed, false);
    assert.strictEqual(r2.failed, false);
  } finally {
    await pool.clear();
  }
});

test("1.1 drain: pool should accept tasks again after drain completes", async () => {
  const pool = leap(2);
  try {
    pool(() => delay(10));
    await pool.drain();
    assert.strictEqual(pool.isDraining, false, "isDraining should reset after drain");
    const result = await pool(() => "post-drain");
    assert.strictEqual(result, "post-drain");
  } finally {
    await pool.clear();
  }
});

test("1.1 drain: should not hang when tasks are active at call time", async () => {
  const pool = leap(2);
  try {
    pool(() => new Promise((r) => setTimeout(r, 10)));
    await pool.drain();
    assert.strictEqual(pool.isDraining, false);
    const result = await pool(() => "post-drain");
    assert.strictEqual(result, "post-drain");
  } finally {
    await pool.clear();
  }
});

test("1.1 onEmpty: resolves when queue becomes empty (tasks may still run)", async () => {
  const pool = leap(1);
  try {
    let queueEmptyFired = false;
    let allDone = false;

    for (let i = 0; i < 4; i++) {
      pool(async () => { await new Promise((r) => setTimeout(r, 20)); });
    }

    pool.onEmpty().then(() => { queueEmptyFired = true; });
    await pool.onIdle();
    allDone = true;

    assert.strictEqual(queueEmptyFired, true, "onEmpty must have fired before onIdle");
    assert.strictEqual(allDone, true);
  } finally {
    await pool.clear();
  }
});

test("1.1 onEmpty: resolves immediately when pool has no queued tasks", async () => {
  const pool = leap(2);
  try {
    await pool.onEmpty();
  } finally {
    await pool.clear();
  }
});

test("1.1 onEmpty: multiple concurrent callers all resolve", async () => {
  const pool = leap(1);
  try {
    for (let i = 0; i < 3; i++) pool(() => new Promise((r) => setTimeout(r, 20)));
    const [r1, r2] = await Promise.all([pool.onEmpty(), pool.onEmpty()]);
    assert.strictEqual(r1, undefined);
    assert.strictEqual(r2, undefined);
  } finally {
    await pool.clear();
  }
});

test("1.1 onSizeLessThan: resolves immediately when already below limit", async () => {
  const pool = leap(5);
  try {
    await pool.onSizeLessThan(10);
  } finally {
    await pool.clear();
  }
});

test("1.1 onSizeLessThan: blocks until pending count drops below limit", async () => {
  const pool = leap(1);
  try {
    let resolved = false;
    for (let i = 0; i < 5; i++) {
      pool(() => new Promise((r) => setTimeout(r, 10)));
    }
    pool.onSizeLessThan(3).then(() => { resolved = true; });
    assert.strictEqual(resolved, false, "should not resolve immediately");
    await pool.onIdle();
    assert.strictEqual(resolved, true, "should resolve after pending drops");
  } finally {
    await pool.clear();
  }
});

test("1.1 onError: rejects when a task fails", async () => {
  const pool = leap(1);
  try {
    let caughtError = null;
    pool.onError().catch((err) => { caughtError = err; });
    await assert.rejects(pool(() => { throw new Error("boom"); }), /boom/);
    await new Promise((r) => setTimeout(r, 10));
    assert.ok(caughtError !== null, "onError should have been called");
    assert.ok(caughtError.message.includes("boom"));
  } finally {
    await pool.clear();
  }
});

test("1.1 onError: resolves normally when racing with onIdle and no errors occur", async () => {
  const pool = leap(2);
  try {
    pool(() => new Promise((r) => setTimeout(r, 20)));
    pool(() => new Promise((r) => setTimeout(r, 20)));

    const winner = await Promise.race([
      pool.onIdle().then(() => "idle"),
      pool.onError().catch(() => "error"),
    ]);
    assert.strictEqual(winner, "idle");
  } finally {
    await pool.clear();
  }
});

test("1.1 onError: each call returns a distinct subscription", async () => {
  const pool = leap(1);
  try {
    let e1 = null;
    let e2 = null;
    pool.onError().catch((err) => { e1 = err; });
    pool.onError().catch((err) => { e2 = err; });
    await assert.rejects(pool(() => { throw new Error("multi"); }));
    await new Promise((r) => setTimeout(r, 10));
    assert.ok(e1 !== null);
    assert.ok(e2 !== null);
  } finally {
    await pool.clear();
  }
});

test("1.1 setPriority: dynamically boosts a queued task's priority", async () => {
  const pool = leap(1);
  try {
    const order = [];
    pool(() => new Promise((r) => setTimeout(r, 30)));
    pool(() => order.push("A"), { id: "A", priority: 0 });
    pool(() => order.push("B"), { id: "B", priority: 0 });
    pool(() => order.push("C"), { id: "C", priority: 0 });

    pool.setPriority("C", 100);

    await pool.onIdle();
    assert.strictEqual(order[0], "C", "C should execute first after priority boost");
  } finally {
    await pool.clear();
  }
});

test("1.1 setPriority: returns false when task id is not in queue", async () => {
  const pool = leap(2);
  try {
    const result = pool.setPriority("nonexistent", 99);
    assert.strictEqual(result, false);
  } finally {
    await pool.clear();
  }
});

test("1.1 isSaturated: true when all slots occupied and tasks waiting", async () => {
  const pool = leap(2);
  try {
    pool(() => new Promise((r) => setTimeout(r, 50)));
    pool(() => new Promise((r) => setTimeout(r, 50)));
    pool(() => new Promise((r) => setTimeout(r, 50)));
    await new Promise((r) => setTimeout(r, 5));
    assert.strictEqual(pool.isSaturated, true);
  } finally {
    await pool.clear();
  }
});

test("1.1 isSaturated: false when queue is empty", async () => {
  const pool = leap(2);
  try {
    assert.strictEqual(pool.isSaturated, false);
  } finally {
    await pool.clear();
  }
});

test("1.1 runningTasks: lists tasks currently executing with metadata", async () => {
  const pool = leap(2);
  try {
    let snapshot = null;
    pool(async () => {
      snapshot = pool.runningTasks;
      await new Promise((r) => setTimeout(r, 10));
    }, { id: "task-rt" });

    await pool.onIdle();
    assert.ok(snapshot !== null);
    assert.strictEqual(snapshot.length, 1);
    assert.strictEqual(snapshot[0].id, "task-rt");
    assert.ok(typeof snapshot[0].startTime === "number");
  } finally {
    await pool.clear();
  }
});

test("1.1 runningTasks: empty array when no tasks are executing", async () => {
  const pool = leap(2);
  try {
    assert.deepStrictEqual(pool.runningTasks, []);
    await pool(() => "quick");
    assert.deepStrictEqual(pool.runningTasks, []);
  } finally {
    await pool.clear();
  }
});

test("1.1 sizeBy: filters queue by priority", async () => {
  const pool = leap(1);
  try {
    pool(() => new Promise((r) => setTimeout(r, 50)));
    pool(() => {}, { priority: 5 });
    pool(() => {}, { priority: 5 });
    pool(() => {}, { priority: 10 });
    await new Promise((r) => setTimeout(r, 5));
    assert.strictEqual(pool.sizeBy({ priority: 5 }), 2);
    assert.strictEqual(pool.sizeBy({ priority: 10 }), 1);
    assert.strictEqual(pool.sizeBy({ priority: 0 }), 0);
  } finally {
    await pool.clear();
  }
});

test("1.1 sizeBy: filters queue by type", async () => {
  const pool = leap(1);
  try {
    pool(() => new Promise((r) => setTimeout(r, 50)));
    pool(() => {}, { type: "api" });
    pool(() => {}, { type: "api" });
    pool(() => {}, { type: "db" });
    await new Promise((r) => setTimeout(r, 5));
    assert.strictEqual(pool.sizeBy({ type: "api" }), 2);
    assert.strictEqual(pool.sizeBy({ type: "db" }), 1);
  } finally {
    await pool.clear();
  }
});

test("1.1 sizeBy: filters queue by tag", async () => {
  const pool = leap(1);
  try {
    pool(() => new Promise((r) => setTimeout(r, 50)));
    pool(() => {}, { tags: ["critical"] });
    pool(() => {}, { tags: ["critical", "billing"] });
    pool(() => {}, { tags: ["background"] });
    await new Promise((r) => setTimeout(r, 5));
    assert.strictEqual(pool.sizeBy({ tag: "critical" }), 2);
    assert.strictEqual(pool.sizeBy({ tag: "billing" }), 1);
    assert.strictEqual(pool.sizeBy({ tag: "background" }), 1);
  } finally {
    await pool.clear();
  }
});