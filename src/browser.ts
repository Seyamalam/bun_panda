/** Browser-safe asynchronous entry for bun_panda's numeric Wasm kernels. */

export const BROWSER_AGG_SUM = 0;
export const BROWSER_AGG_MEAN = 1;
export const BROWSER_AGG_MIN = 2;
export const BROWSER_AGG_MAX = 3;
export const BROWSER_AGG_COUNT = 4;

export type BrowserAggregateCode = 0 | 1 | 2 | 3 | 4;

interface BrowserKernelExports {
  memory: WebAssembly.Memory;
  bp_alloc(len: number): number;
  bp_free_all(): void;
  bp_last_group_count(): number;
  bp_agg_f64(values: number, ids: number, n: number, code: number, out: number, counts: number, groups: number): void;
  bp_argsort_f64(values: number, n: number, ascending: number): number;
  bp_filter_indices(mask: number, n: number): number;
}

export interface BrowserKernelOptions {
  /** Preloaded bytes, useful for bundlers, workers, tests, and offline apps. */
  bytes?: ArrayBuffer;
  /** Wasm asset URL. Defaults to the asset adjacent to this ESM module. */
  url?: string | URL;
  /** Injectable fetch implementation for service workers and test harnesses. */
  fetch?: typeof globalThis.fetch;
}

export interface BrowserKernel {
  readonly memory: WebAssembly.Memory;
  stableArgsort(values: Float64Array, ascending?: boolean): Int32Array;
  filterIndices(mask: Uint8Array | readonly boolean[]): Int32Array;
  aggregate(values: Float64Array, groupIds: Int32Array, groupCount: number, code: BrowserAggregateCode): Float64Array;
}

function validate(instance: WebAssembly.Instance): BrowserKernelExports {
  const exports = instance.exports as unknown as BrowserKernelExports;
  if (
    !(exports.memory instanceof WebAssembly.Memory) ||
    typeof exports.bp_alloc !== "function" ||
    typeof exports.bp_free_all !== "function" ||
    typeof exports.bp_agg_f64 !== "function" ||
    typeof exports.bp_argsort_f64 !== "function" ||
    typeof exports.bp_filter_indices !== "function"
  ) {
    throw new Error("The supplied module is not a bun_panda Wasm kernel.");
  }
  return exports;
}

async function instantiate(options: BrowserKernelOptions): Promise<WebAssembly.Instance> {
  if (options.bytes) {
    const result = await WebAssembly.instantiate(options.bytes, {});
    return result instanceof WebAssembly.Instance ? result : result.instance;
  }

  const fetcher = options.fetch ?? globalThis.fetch;
  if (typeof fetcher !== "function") {
    throw new Error("Browser Wasm initialization requires fetch or preloaded bytes.");
  }
  const url = options.url ?? new URL("./wasm/bun_panda_core.wasm", import.meta.url);
  const response = await fetcher(url);
  if (!response.ok) throw new Error(`Failed to fetch bun_panda Wasm (${response.status}).`);
  const fallback = response.clone();
  if (typeof WebAssembly.instantiateStreaming === "function") {
    try {
      const result = await WebAssembly.instantiateStreaming(response, {});
      return result.instance;
    } catch {
      // Static hosts sometimes omit application/wasm; retain an async byte fallback.
    }
  }
  const result = await WebAssembly.instantiate(await fallback.arrayBuffer(), {});
  return result instanceof WebAssembly.Instance ? result : result.instance;
}

function allocate(exports: BrowserKernelExports, bytes: number): number {
  const pointer = exports.bp_alloc(Math.max(bytes, 1));
  if (!pointer) throw new Error("bun_panda Wasm allocation failed.");
  return pointer;
}

/** Loads an isolated kernel; one instance can be owned by each Web Worker. */
export async function createBrowserKernel(options: BrowserKernelOptions = {}): Promise<BrowserKernel> {
  const exports = validate(await instantiate(options));
  return {
    memory: exports.memory,

    stableArgsort(values: Float64Array, ascending = true): Int32Array {
      if (values.length === 0) return new Int32Array();
      try {
        const pointer = allocate(exports, values.byteLength);
        new Float64Array(exports.memory.buffer, pointer, values.length).set(values);
        const resultPointer = exports.bp_argsort_f64(pointer, values.length, ascending ? 1 : 0);
        if (!resultPointer) throw new Error("bun_panda Wasm argsort failed.");
        return new Int32Array(new Int32Array(exports.memory.buffer, resultPointer, values.length));
      } finally {
        exports.bp_free_all();
      }
    },

    filterIndices(mask: Uint8Array | readonly boolean[]): Int32Array {
      if (mask.length === 0) return new Int32Array();
      const bytes = mask instanceof Uint8Array ? mask : Uint8Array.from(mask, (value) => value ? 1 : 0);
      try {
        const pointer = allocate(exports, bytes.byteLength);
        new Uint8Array(exports.memory.buffer, pointer, bytes.length).set(bytes);
        const resultPointer = exports.bp_filter_indices(pointer, bytes.length);
        const kept = exports.bp_last_group_count();
        if (!resultPointer) throw new Error("bun_panda Wasm filter failed.");
        return new Int32Array(new Int32Array(exports.memory.buffer, resultPointer, kept));
      } finally {
        exports.bp_free_all();
      }
    },

    aggregate(values: Float64Array, groupIds: Int32Array, groupCount: number, code: BrowserAggregateCode): Float64Array {
      if (values.length !== groupIds.length) throw new Error("values and groupIds must have equal length.");
      if (!Number.isInteger(groupCount) || groupCount < 0) throw new Error("groupCount must be a non-negative integer.");
      if (groupCount === 0) return new Float64Array();
      try {
        const valuesPointer = allocate(exports, values.byteLength);
        const idsPointer = allocate(exports, groupIds.byteLength);
        const outputPointer = allocate(exports, groupCount * 8);
        const countsPointer = allocate(exports, groupCount * 4);
        new Float64Array(exports.memory.buffer, valuesPointer, values.length).set(values);
        new Int32Array(exports.memory.buffer, idsPointer, groupIds.length).set(groupIds);
        new Float64Array(exports.memory.buffer, outputPointer, groupCount).fill(0);
        new Int32Array(exports.memory.buffer, countsPointer, groupCount).fill(0);
        exports.bp_agg_f64(valuesPointer, idsPointer, values.length, code, outputPointer, countsPointer, groupCount);
        return new Float64Array(new Float64Array(exports.memory.buffer, outputPointer, groupCount));
      } finally {
        exports.bp_free_all();
      }
    },
  };
}
