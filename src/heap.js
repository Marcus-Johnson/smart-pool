/**
 * A Binary Max-Heap
 */
export class PriorityHeap {
  constructor() {
    this.heap = [];
  }

  _compare(a, b) {
    if (a.priority !== b.priority) {
      return a.priority - b.priority;
    }
    return b.seq - a.seq;
  }

  push(item) {
    this.heap.push(item);
    this._bubbleUp();
  }

  pop() {
    const size = this.heap.length;
    if (size === 0) return null;
    if (size === 1) return this.heap.pop();
    const top = this.heap[0];
    this.heap[0] = this.heap.pop();
    this._bubbleDown(0);
    return top;
  }

  peek() {
    return this.heap[0] || null;
  }

  clear() {
    this.heap = [];
  }

  remove(predicate) {
    const initialSize = this.heap.length;
    this.heap = this.heap.filter((item) => !predicate(item));
    if (this.heap.length !== initialSize) {
      this._rebuild();
      return true;
    }
    return false;
  }

  adjustPriorities(threshold, amount, isDecay = false) {
    const size = this.heap.length;
    if (size <= 1) return;

    let changed = false;
    const targetPriority = isDecay
      ? this.heap.reduce((min, item) => Math.min(min, item.priority), Infinity)
      : this.heap.reduce(
          (max, item) => Math.max(max, item.priority),
          -Infinity
        );

    for (let i = 0; i < size; i++) {
      const item = this.heap[i];
      const isEligible = isDecay
        ? item.priority > targetPriority
        : item.priority < targetPriority;

      if (isEligible) {
        item.cycles = (item.cycles || 0) + 1;
        if (item.cycles >= threshold) {
          item.priority = targetPriority;
          item.cycles = 0;
          changed = true;
        }
      } else {
        item.cycles = 0;
      }
    }

    if (changed) this._rebuild();
  }

  size() {
    return this.heap.length;
  }

  _rebuild() {
    const length = this.heap.length;
    for (let i = (length >> 1) - 1; i >= 0; i--) {
      this._bubbleDown(i);
    }
  }

  _bubbleUp() {
    let index = this.heap.length - 1;
    const element = this.heap[index];
    while (index > 0) {
      let parentIdx = (index - 1) >> 1;
      if (this._compare(element, this.heap[parentIdx]) <= 0) break;
      this.heap[index] = this.heap[parentIdx];
      index = parentIdx;
    }
    this.heap[index] = element;
  }

  _bubbleDown(index = 0) {
    const length = this.heap.length;
    const element = this.heap[index];
    while (true) {
      let leftChildIdx = (index << 1) + 1;
      let rightChildIdx = (index << 1) + 2;
      let swap = null;

      if (leftChildIdx < length) {
        if (this._compare(this.heap[leftChildIdx], element) > 0)
          swap = leftChildIdx;
      }

      if (rightChildIdx < length) {
        const compareTo = swap === null ? element : this.heap[leftChildIdx];
        if (this._compare(this.heap[rightChildIdx], compareTo) > 0)
          swap = rightChildIdx;
      }

      if (swap === null) break;
      this.heap[index] = this.heap[swap];
      index = swap;
    }
    this.heap[index] = element;
  }
}