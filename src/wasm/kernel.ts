/**
 * WASM kernel loader and typed-array fast path for bun_panda.
 *
 * The Rust crate (`crates/core`) exposes a flat C ABI over linear
 * memory. This module owns all marshalling: it packs group-key bytes,
 * calls `bp_group_ids` + `bp_agg_f64`, and reads results back. Any
 * failure to load the module makes every `wasmAvailable()` check false
 * so callers transparently keep the pure-TS path.
 */
import { keyFragment } from "../internal/dataframe/keys";
import type { CellValue, Row } from "../types";

export const AGG_SUM = 0;
export const AGG_MEAN = 1;
export const AGG_MIN = 2;
export const AGG_MAX = 3;
export const AGG_COUNT = 4;

interface KernelExports {
  memory: WebAssembly.Memory;
  bp_alloc(len: number): number;
  bp_free(ptr: number, len: number): void;
  bp_free_all(): void;
  bp_group_ids(keys: number, offsets: number, n: number): number;
  bp_last_group_count(): number;
  bp_agg_f64(
    values: number,
    ids: number,
    n: number,
    code: number,
    out: number,
    counts: number,
    nGroups: number
  ): void;
}

let cached: KernelExports | null = null;
let failed = false;

/** Loads (once) and returns the wasm exports, or null when unavailable. */
export function wasmKernel(): KernelExports | null {
  if (cached) {
    return cached;
  }
  if (failed) {
    return null;
  }
  try {
    // Bun/Node resolve this relative to the compiled source file; bundlers
    // with asset support inline it as a URL.
    const url = new URL("./bun_panda_core.wasm", import.meta.url);
    const bytes = readWasmSync(url);
    if (!bytes || bytes.byteLength === 0) {
      failed = true;
      return null;
    }
    const module = new WebAssembly.Module(bytes);
    const instance = new WebAssembly.Instance(module, {});
    const exports = instance.exports as unknown as KernelExports;
    if (
      typeof exports.bp_alloc !== "function" ||
      typeof exports.bp_agg_f64 !== "function" ||
      !(exports.memory instanceof WebAssembly.Memory)
    ) {
      failed = true;
      return null;
    }
    cached = exports;
    return cached;
  } catch {
    failed = true;
    return null;
  }
}

function readWasmSync(url: URL): ArrayBuffer | null {
  if (typeof Bun !== "undefined") {
    const fs = require("node:fs") as typeof import("node:fs");
    try {
      const buf = fs.readFileSync(url);
      return buf.buffer.slice(
        buf.byteOffset,
        buf.byteOffset + buf.byteLength
      ) as ArrayBuffer;
    } catch {
      return null;
    }
  }
  return null;
}

/**
 * True when the current call shape can run on the wasm path: the kernel
 * is loaded and every referenced column is numeric-only in practice.
 * Non-finite numbers never occur because `isNumber` gates ingestion.
 */
export function wasmAvailable(): boolean {
  return wasmKernel() !== null;
}

export interface WasmGroupAggResult {
  /** Group id per row; -1 marks an excluded row (missing key). */
  ids: Int32Array;
  /** Distinct group count (`bp_last_group_count`). */
  groupCount: number;
}

interface KeyPlan {
  keysPtr: number;
  offsPtr: number;
  n: number;
}

const enc = new TextEncoder();

function packKeys(kernel: KernelExports, fragments: string[]): KeyPlan {
  const encoded: Uint8Array[] = [];
  let total = 0;
  const offsets = new Int32Array(fragments.length + 1);
  for (let i = 0; i < fragments.length; i += 1) {
    const bytes = enc.encode(fragments[i]);
    encoded.push(bytes);
    total += bytes.length;
    offsets[i + 1] = total;
  }

  const keysPtr = kernel.bp_alloc(total > 0 ? total : 1);
  if (!keysPtr) {
    throw new Error("wasm alloc failed");
  }
  if (total > 0) {
    const heap = new Uint8Array(kernel.memory.buffer);
    let cursor = keysPtr;
    for (const bytes of encoded) {
      heap.set(bytes, cursor);
      cursor += bytes.length;
    }
  }

  const offsPtr = kernel.bp_alloc(offsets.byteLength);
  if (!offsPtr) {
    throw new Error("wasm alloc failed");
  }
  new Int32Array(kernel.memory.buffer, offsPtr, offsets.length).set(offsets);

  return { keysPtr, offsPtr, n: fragments.length };
}

/**
 * Computes dense group ids for `rows` keyed by `by`. Rows whose key is
 * missing receive id `-1`, mirroring pandas `dropna=True`.
 */
export function wasmGroupIds(rows: Row[], by: string[]): WasmGroupAggResult | null {
  const kernel = wasmKernel();
  if (!kernel) {
    return null;
  }
  try {
    const fragments = new Array<string>(rows.length);
    for (let i = 0; i < rows.length; i += 1) {
      const row = rows[i]!;
      let fragment = "";
      let missing = false;
      for (let c = 0; c < by.length; c += 1) {
        const value = row[by[c]!];
        if (value === null || value === undefined) {
          missing = true;
          break;
        }
        fragment += keyFragment(value);
      }
      fragments[i] = missing ? "" : fragment;
    }

    const plan = packKeys(kernel, fragments);
    const idsPtr = kernel.bp_group_ids(plan.keysPtr, plan.offsPtr, plan.n);
    if (!idsPtr) {
      return null;
    }
    const ids = new Int32Array(plan.n);
    ids.set(new Int32Array(kernel.memory.buffer, idsPtr, plan.n));
    const groupCount = kernel.bp_last_group_count();
    kernel.bp_free_all();
    return { ids, groupCount };
  } catch {
    failed = true;
    return null;
  }
}

/**
 * Runs one aggregation code over a numeric column using precomputed
 * group ids. Returns a Float64Array of length `groupCount`; groups with
 * no contributing values are NaN.
 */
export function wasmAggregateColumn(
  rows: Row[],
  column: string,
  ids: Int32Array,
  groupCount: number,
  code: number
): Float64Array | null {
  const kernel = wasmKernel();
  if (!kernel || groupCount === 0 || rows.length === 0) {
    return null;
  }
  try {
    const n = rows.length;
    const values = new Float64Array(n);
    for (let i = 0; i < n; i += 1) {
      const value = rows[i]![column];
      values[i] = typeof value === "number" && Number.isFinite(value) ? value : NaN;
    }

    const valsPtr = kernel.bp_alloc(values.byteLength);
    const idsPtr = kernel.bp_alloc(ids.byteLength);
    const outPtr = kernel.bp_alloc(groupCount * 8);
    const cntPtr = kernel.bp_alloc(groupCount * 4);
    if (!valsPtr || !idsPtr || !outPtr || !cntPtr) {
      return null;
    }
    // The bump arena hands back dirty pages: `out` accumulates with `+=`,
    // so stale bytes (often the 0xFF NaN pattern) must be cleared first.
    new Float64Array(kernel.memory.buffer, outPtr, groupCount).fill(0);
    new Int32Array(kernel.memory.buffer, cntPtr, groupCount).fill(0);
    new Float64Array(kernel.memory.buffer, valsPtr, n).set(values);
    new Int32Array(kernel.memory.buffer, idsPtr, n).set(ids);

    kernel.bp_agg_f64(valsPtr, idsPtr, n, code, outPtr, cntPtr, groupCount);
    const out = new Float64Array(groupCount);
    out.set(new Float64Array(kernel.memory.buffer, outPtr, groupCount));
    kernel.bp_free_all();
    return out;
  } catch {
    failed = true;
    return null;
  }
}
