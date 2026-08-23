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
  bp_agg_multi_f64(
    values: number,
    ids: number,
    n: number,
    planCodes: number,
    nPlans: number,
    out: number,
    counts: number,
    nGroups: number
  ): void;
  bp_argsort_f64(values: number, n: number, ascending: number): number;
  bp_filter_indices(mask: number, n: number): number;
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
      typeof exports.bp_agg_multi_f64 !== "function" ||
      typeof exports.bp_argsort_f64 !== "function" ||
      typeof exports.bp_filter_indices !== "function" ||
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
    const n = rows.length;
    // Single-pass key packing: build one big string, then encode once.
    // Per-row TextEncoder.encode calls were the dominant JS-side cost.
    const parts: string[] = [];
    const lengths = new Int32Array(n);
    let totalChars = 0;
    for (let i = 0; i < n; i += 1) {
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
      if (missing) {
        fragment = "";
      }
      parts.push(fragment);
      const charLen = fragment.length;
      lengths[i] = charLen;
      totalChars += charLen;
    }

    const packed: string = parts.join("");
    const encoded = enc.encode(packed);
    const totalBytes = encoded.length;

    const keysPtr = kernel.bp_alloc(totalBytes > 0 ? totalBytes : 1);
    if (!keysPtr) {
      throw new Error("wasm alloc failed");
    }
    if (totalBytes > 0) {
      new Uint8Array(kernel.memory.buffer, keysPtr, totalBytes).set(encoded);
    }

    // Byte offsets from char counts: fragments are ASCII in the common
    // path, but compute real byte offsets by walking the encoding once.
    const offsets = new Int32Array(n + 1);
    if (totalBytes !== totalChars) {
      // Non-ASCII present: derive byte lengths via encode of each part.
      let cursor = 0;
      for (let i = 0; i < n; i += 1) {
        offsets[i] = cursor;
        cursor += enc.encode(parts[i]).length;
      }
      offsets[n] = cursor;
    } else {
      let cursor = 0;
      for (let i = 0; i < n; i += 1) {
        offsets[i] = cursor;
        cursor += lengths[i]!;
      }
      offsets[n] = cursor;
    }

    const offsPtr = kernel.bp_alloc(offsets.byteLength);
    if (!offsPtr) {
      throw new Error("wasm alloc failed");
    }
    new Int32Array(kernel.memory.buffer, offsPtr, n + 1).set(offsets);

    const idsPtr = kernel.bp_group_ids(keysPtr, offsPtr, n);
    if (!idsPtr) {
      return null;
    }
    const ids = new Int32Array(n);
    ids.set(new Int32Array(kernel.memory.buffer, idsPtr, n));
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

/**
 * Fused multi-plan aggregation over columnar Float64Arrays.
 *
 * `columns` are the plan columns (same length), `codes` one aggregation
 * code per column. Returns a plan-major array: plan p's group results
 * start at `p * groupCount`. Groups with no contributing values are
 * NaN (or 0 for count). Returns null when the kernel is unavailable or
 * the call shape is unsupported.
 */
export function wasmAggMultiF64(
  columns: Float64Array[],
  codes: number[],
  ids: Int32Array,
  groupCount: number
): { results: Float64Array; nPlans: number } | null {
  const kernel = wasmKernel();
  if (!kernel || groupCount === 0 || columns.length === 0) {
    return null;
  }
  const n = ids.length;
  try {
    const nPlans = columns.length;
    // Pack columns contiguously into one buffer.
    const packedPtr = kernel.bp_alloc(nPlans * n * 8);
    const idsPtr = kernel.bp_alloc(ids.byteLength);
    const codesPtr = kernel.bp_alloc(codes.length * 4);
    const outPtr = kernel.bp_alloc(nPlans * groupCount * 8);
    const cntPtr = kernel.bp_alloc(nPlans * groupCount * 4);
    if (!packedPtr || !idsPtr || !codesPtr || !outPtr || !cntPtr) {
      return null;
    }
    new Float64Array(kernel.memory.buffer, outPtr, nPlans * groupCount).fill(0);
    new Int32Array(kernel.memory.buffer, cntPtr, nPlans * groupCount).fill(0);

    for (let p = 0; p < nPlans; p += 1) {
      new Float64Array(kernel.memory.buffer, packedPtr + p * n * 8, n).set(
        columns[p]!
      );
    }
    new Int32Array(kernel.memory.buffer, idsPtr, n).set(ids);
    new Int32Array(kernel.memory.buffer, codesPtr, nPlans).set(codes);

    kernel.bp_agg_multi_f64(
      packedPtr,
      idsPtr,
      n,
      codesPtr,
      nPlans,
      outPtr,
      cntPtr,
      groupCount
    );

    const results = new Float64Array(nPlans * groupCount);
    results.set(
      new Float64Array(kernel.memory.buffer, outPtr, nPlans * groupCount)
    );
    kernel.bp_free_all();
    return { results, nPlans };
  } catch {
    failed = true;
    return null;
  }
}

/**
 * Stable argsort of an f64 column; NaN entries go last regardless of
 * direction. Returns source indices in sorted order, or null when the
 * kernel is unavailable.
 */
export function wasmArgsortF64(
  values: Float64Array,
  ascending: boolean
): Int32Array | null {
  const kernel = wasmKernel();
  if (!kernel || values.length === 0) {
    return null;
  }
  try {
    const n = values.length;
    const valsPtr = kernel.bp_alloc(n * 8);
    if (!valsPtr) {
      return null;
    }
    new Float64Array(kernel.memory.buffer, valsPtr, n).set(values);
    const idxPtr = kernel.bp_argsort_f64(valsPtr, n, ascending ? 1 : 0);
    if (!idxPtr) {
      return null;
    }
    const idx = new Int32Array(n);
    idx.set(new Int32Array(kernel.memory.buffer, idxPtr, n));
    kernel.bp_free_all();
    return idx;
  } catch {
    failed = true;
    return null;
  }
}

/**
 * Compacted row indices for a boolean mask. `kept` count is returned
 * via the result length; returns null when the kernel is unavailable.
 */
export function wasmFilterIndices(mask: Uint8Array): Int32Array | null {
  const kernel = wasmKernel();
  if (!kernel || mask.length === 0) {
    return null;
  }
  try {
    const n = mask.length;
    const maskPtr = kernel.bp_alloc(n);
    if (!maskPtr) {
      return null;
    }
    new Uint8Array(kernel.memory.buffer, maskPtr, n).set(mask);
    const idxPtr = kernel.bp_filter_indices(maskPtr, n);
    const kept = kernel.bp_last_group_count();
    if (!idxPtr) {
      return null;
    }
    const idx = new Int32Array(kept);
    idx.set(new Int32Array(kernel.memory.buffer, idxPtr, kept));
    kernel.bp_free_all();
    return idx;
  } catch {
    failed = true;
    return null;
  }
}
