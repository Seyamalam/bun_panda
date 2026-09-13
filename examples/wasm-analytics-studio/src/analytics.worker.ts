/// <reference lib="webworker" />

import { bulkMemory, exceptions, multiValue, referenceTypes, relaxedSimd, simd, tailCall, threads } from "wasm-feature-detect";
import { BROWSER_AGG_SUM, createBrowserKernel, type BrowserKernel } from "../../../src/browser";
import { chooseExecutionPath, type DispatchRequest } from "../../../src/wasm/dispatch";
import {
  byteLength,
  createDataset,
  filterIndices,
  gather,
  groupedSum,
  stableArgsort,
  summarize,
  type Dataset,
} from "./benchmark-core";
import type {
  CapabilityReport,
  ExperimentConfig,
  ExperimentReport,
  WorkerRequest,
  WorkerResponse,
  WorkloadId,
  WorkloadResult,
} from "./types";

declare const self: DedicatedWorkerGlobalScope;

interface WorkloadDefinition {
  id: WorkloadId;
  label: string;
  description: string;
  typescript: () => ArrayLike<number>;
  wasm: () => ArrayLike<number>;
  dispatch: DispatchRequest | null;
}

function post(message: WorkerResponse): void {
  self.postMessage(message);
}

async function digest(values: ArrayLike<number>): Promise<string> {
  const source = ArrayBuffer.isView(values)
    ? new Uint8Array(values.buffer, values.byteOffset, values.byteLength)
    : new TextEncoder().encode(JSON.stringify(Array.from(values)));
  const bytes = new Uint8Array(source.byteLength);
  bytes.set(source);
  const hash = await crypto.subtle.digest("SHA-256", bytes.buffer);
  return [...new Uint8Array(hash)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function measurePair(
  definition: WorkloadDefinition,
  config: ExperimentConfig,
): { typescriptSamples: number[]; wasmSamples: number[]; typescriptOutput: ArrayLike<number>; wasmOutput: ArrayLike<number> } {
  let typescriptOutput: ArrayLike<number> = [];
  let wasmOutput: ArrayLike<number> = [];
  for (let index = 0; index < config.warmups; index += 1) {
    typescriptOutput = definition.typescript();
    wasmOutput = definition.wasm();
  }

  const typescriptSamples: number[] = [];
  const wasmSamples: number[] = [];
  const timed = (operation: () => ArrayLike<number>, samples: number[]): ArrayLike<number> => {
    const started = performance.now();
    const output = operation();
    samples.push(performance.now() - started);
    return output;
  };

  for (let index = 0; index < config.iterations; index += 1) {
    if ((index + config.seed) % 2 === 0) {
      typescriptOutput = timed(definition.typescript, typescriptSamples);
      wasmOutput = timed(definition.wasm, wasmSamples);
    } else {
      wasmOutput = timed(definition.wasm, wasmSamples);
      typescriptOutput = timed(definition.typescript, typescriptSamples);
    }
  }
  return { typescriptSamples, wasmSamples, typescriptOutput, wasmOutput };
}

function workloadDefinitions(dataset: Dataset, config: ExperimentConfig, kernel: BrowserKernel): WorkloadDefinition[] {
  const typescriptPipeline = () => {
    const selected = filterIndices(dataset.mask);
    return groupedSum(gather(dataset.values, selected), gather(dataset.groupIds, selected), config.groups);
  };
  const wasmPipeline = () => {
    const selected = kernel.filterIndices(dataset.mask);
    return kernel.aggregate(gather(dataset.values, selected), gather(dataset.groupIds, selected), config.groups, BROWSER_AGG_SUM);
  };

  return [
    {
      id: "stable-sort",
      label: "Stable numeric sort",
      description: "Returns row indices ordered by value while preserving tie order.",
      typescript: () => stableArgsort(dataset.values),
      wasm: () => kernel.stableArgsort(dataset.values),
      dispatch: { operation: "sort", rowCount: config.rows },
    },
    {
      id: "filter-indices",
      label: "Mask compaction",
      description: "Compacts a byte mask into the positions of selected rows.",
      typescript: () => filterIndices(dataset.mask),
      wasm: () => kernel.filterIndices(dataset.mask),
      dispatch: { operation: "filter-mask", rowCount: config.rows },
    },
    {
      id: "grouped-sum",
      label: "Grouped reduction",
      description: "Fuses group lookup and numeric summation over typed columns.",
      typescript: () => groupedSum(dataset.values, dataset.groupIds, config.groups),
      wasm: () => kernel.aggregate(dataset.values, dataset.groupIds, config.groups, BROWSER_AGG_SUM),
      dispatch: {
        operation: "groupby-fused",
        rowCount: config.rows,
        planCount: 1,
        dropna: true,
        typedColumnsReused: true,
      },
    },
    {
      id: "filter-group-pipeline",
      label: "Filter → group pipeline",
      description: "Composes mask compaction, typed gathers, and grouped aggregation.",
      typescript: typescriptPipeline,
      wasm: wasmPipeline,
      dispatch: null,
    },
  ];
}

async function capabilities(): Promise<CapabilityReport> {
  const entries = await Promise.all([
    simd(), relaxedSimd(), threads(), bulkMemory(), exceptions(), multiValue(), referenceTypes(), tailCall(),
  ]);
  return {
    webAssembly: typeof WebAssembly === "object",
    instantiateStreaming: typeof WebAssembly.instantiateStreaming === "function",
    crossOriginIsolated: globalThis.crossOriginIsolated,
    sharedArrayBuffer: typeof SharedArrayBuffer === "function",
    hardwareConcurrency: navigator.hardwareConcurrency,
    features: Object.fromEntries(
      ["SIMD", "Relaxed SIMD", "Threads", "Bulk memory", "Exceptions", "Multi-value", "Reference types", "Tail calls"]
        .map((name, index) => [name, entries[index] ?? false]),
    ),
  };
}

async function run(config: ExperimentConfig, wasmUrl: string): Promise<ExperimentReport> {
  const workerStarted = performance.now();
  post({ type: "progress", stage: "Generating deterministic columns", percent: 8 });
  const dataset = createDataset(config);
  const capabilityReport = await capabilities();

  post({ type: "progress", stage: "Compiling the Rust/Wasm kernel", percent: 17 });
  const wasmResponse = await fetch(wasmUrl);
  if (!wasmResponse.ok) throw new Error(`Could not load the Wasm asset (${wasmResponse.status}).`);
  const wasmBytes = await wasmResponse.arrayBuffer();
  const initStarted = performance.now();
  const kernel = await createBrowserKernel({ bytes: wasmBytes });
  const wasmInitMs = performance.now() - initStarted;
  const wasmMemoryBeforeBytes = kernel.memory.buffer.byteLength;

  const definitions = workloadDefinitions(dataset, config, kernel);
  const workloads: WorkloadResult[] = [];
  for (let index = 0; index < definitions.length; index += 1) {
    const definition = definitions[index]!;
    post({
      type: "progress",
      stage: `Measuring ${definition.label.toLowerCase()}`,
      percent: 25 + Math.round(index / definitions.length * 65),
    });
    const measured = measurePair(definition, config);
    const [typescriptDigest, wasmDigest] = await Promise.all([
      digest(measured.typescriptOutput),
      digest(measured.wasmOutput),
    ]);
    const typescript = summarize(measured.typescriptSamples);
    const wasm = summarize(measured.wasmSamples);
    const dispatch = definition.dispatch
      ? chooseExecutionPath(definition.dispatch)
      : {
          path: "hybrid" as const,
          reason: "compose-calibrated-kernels",
          calibrationVersion: chooseExecutionPath({ operation: "filter-mask", rowCount: config.rows }).calibrationVersion,
        };
    workloads.push({
      id: definition.id,
      label: definition.label,
      description: definition.description,
      typescript,
      wasm,
      speedup: typescript.medianMs / Math.max(wasm.medianMs, Number.EPSILON),
      winner: wasm.medianMs < typescript.medianMs ? "wasm" : "typescript",
      equivalent: typescriptDigest === wasmDigest,
      typescriptDigest,
      wasmDigest,
      outputLength: measured.wasmOutput.length,
      dispatch,
    });
  }

  post({ type: "progress", stage: "Sealing reproducibility report", percent: 96 });
  return {
    schemaVersion: "1.0.0",
    runId: crypto.randomUUID(),
    createdAt: new Date().toISOString(),
    config,
    capabilities: capabilityReport,
    workloads,
    wasmInitMs,
    workerElapsedMs: performance.now() - workerStarted,
    inputBytes: byteLength(dataset),
    wasmAssetBytes: wasmBytes.byteLength,
    wasmMemoryBeforeBytes,
    wasmMemoryAfterBytes: kernel.memory.buffer.byteLength,
    userAgent: navigator.userAgent,
  };
}

self.onmessage = async (event: MessageEvent<WorkerRequest>) => {
  if (event.data.type !== "run") return;
  try {
    const report = await run(event.data.config, event.data.wasmUrl);
    post({ type: "complete", report });
  } catch (error) {
    const failure = error instanceof Error ? error : new Error(String(error));
    post({ type: "error", message: failure.message, stack: failure.stack });
  }
};

post({ type: "ready" });
