import { bulkMemory, exceptions, multiValue, referenceTypes, relaxedSimd, simd, tailCall, threads } from "wasm-feature-detect";
import { BROWSER_AGG_SUM, createBrowserKernel } from "../../../src/browser";

interface RequestMessage {
  rows: number;
  seed: number;
  warmups: number;
  iterations: number;
  wasmUrl: string;
}

function lcg(initialSeed: number): () => number {
  let state = initialSeed >>> 0;
  return () => {
    state = (Math.imul(1664525, state) + 1013904223) >>> 0;
    return state / 2 ** 32;
  };
}

function sha256(value: unknown): Promise<string> {
  const bytes = new TextEncoder().encode(JSON.stringify(value));
  return crypto.subtle.digest("SHA-256", bytes).then((digest) =>
    [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("")
  );
}

function timed(iterations: number, operation: () => unknown): { samplesMs: number[]; output: unknown } {
  const samplesMs = new Array<number>(iterations);
  let output: unknown;
  for (let index = 0; index < iterations; index += 1) {
    const started = performance.now();
    output = operation();
    samplesMs[index] = performance.now() - started;
  }
  return { samplesMs, output };
}

self.onmessage = async (event: MessageEvent<RequestMessage>) => {
  const request = event.data;
  const workerStarted = performance.now();
  const random = lcg(request.seed);
  const values = new Float64Array(request.rows);
  const mask = new Uint8Array(request.rows);
  const groupIds = new Int32Array(request.rows);
  for (let index = 0; index < request.rows; index += 1) {
    values[index] = Math.floor(random() * 1_000_000) + index / 1_000_000;
    mask[index] = values[index] >= 500_000 ? 1 : 0;
    groupIds[index] = index % 64;
  }

  const initStarted = performance.now();
  const kernel = await createBrowserKernel({ url: request.wasmUrl });
  const wasmInitMs = performance.now() - initStarted;

  const jsSort = () => Int32Array.from(
    Array.from({ length: values.length }, (_, index) => index)
      .sort((left, right) => values[left]! - values[right]! || left - right)
  );
  const jsFilter = () => {
    const kept: number[] = [];
    for (let index = 0; index < mask.length; index += 1) if (mask[index]) kept.push(index);
    return Int32Array.from(kept);
  };
  const jsAggregate = () => {
    const output = new Float64Array(64);
    for (let index = 0; index < values.length; index += 1) output[groupIds[index]!] += values[index]!;
    return output;
  };
  const wasmSort = () => kernel.stableArgsort(values, true);
  const wasmFilter = () => kernel.filterIndices(mask);
  const wasmAggregate = () => kernel.aggregate(values, groupIds, 64, BROWSER_AGG_SUM);

  for (let index = 0; index < request.warmups; index += 1) {
    jsSort();
    jsFilter();
    jsAggregate();
    wasmSort();
    wasmFilter();
    wasmAggregate();
  }

  const results = {
    stable_argsort: { js: timed(request.iterations, jsSort), wasm: timed(request.iterations, wasmSort) },
    filter_indices: { js: timed(request.iterations, jsFilter), wasm: timed(request.iterations, wasmFilter) },
    grouped_sum: { js: timed(request.iterations, jsAggregate), wasm: timed(request.iterations, wasmAggregate) },
  };

  const checks: Record<string, { js: string; wasm: string; equivalent: boolean }> = {};
  for (const [name, result] of Object.entries(results)) {
    const jsOutput = Array.from(result.js.output as ArrayLike<number>);
    const wasmOutput = Array.from(result.wasm.output as ArrayLike<number>);
    const jsDigest = await sha256(jsOutput);
    const wasmDigest = await sha256(wasmOutput);
    checks[name] = { js: jsDigest, wasm: wasmDigest, equivalent: jsDigest === wasmDigest };
    if (jsDigest !== wasmDigest) throw new Error(`${name} produced different JavaScript and Wasm outputs`);
  }

  const capabilities = {
    webAssembly: typeof WebAssembly === "object",
    instantiateStreaming: typeof WebAssembly.instantiateStreaming === "function",
    crossOriginIsolated: globalThis.crossOriginIsolated,
    sharedArrayBuffer: typeof SharedArrayBuffer === "function",
    hardwareConcurrency: navigator.hardwareConcurrency,
    features: {
      simd: await simd(),
      relaxedSimd: await relaxedSimd(),
      threads: await threads(),
      bulkMemory: await bulkMemory(),
      exceptions: await exceptions(),
      multiValue: await multiValue(),
      referenceTypes: await referenceTypes(),
      tailCall: await tailCall(),
    },
  };

  self.postMessage({
    schemaVersion: "1.0.0",
    rows: request.rows,
    seed: request.seed,
    warmups: request.warmups,
    iterations: request.iterations,
    wasmInitMs,
    workerElapsedMs: performance.now() - workerStarted,
    wasmMemoryBytes: kernel.memory.buffer.byteLength,
    capabilities,
    checks,
    results: Object.fromEntries(Object.entries(results).map(([name, result]) => [name, {
      jsSamplesMs: result.js.samplesMs,
      wasmSamplesMs: result.wasm.samplesMs,
    }])),
  });
};
