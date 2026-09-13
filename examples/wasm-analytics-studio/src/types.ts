import type { DispatchDecision } from "../../../src/wasm/dispatch";

export type DataShape = "uniform" | "skewed" | "nearly-sorted";
export type WorkloadId = "stable-sort" | "filter-indices" | "grouped-sum" | "filter-group-pipeline";

export interface ExperimentConfig {
  rows: number;
  groups: number;
  selectivity: number;
  iterations: number;
  warmups: number;
  seed: number;
  shape: DataShape;
}

export interface CapabilityReport {
  webAssembly: boolean;
  instantiateStreaming: boolean;
  crossOriginIsolated: boolean;
  sharedArrayBuffer: boolean;
  hardwareConcurrency: number;
  features: Record<string, boolean>;
}

export interface RuntimeMeasurement {
  samplesMs: number[];
  medianMs: number;
  p95Ms: number;
}

export interface WorkloadResult {
  id: WorkloadId;
  label: string;
  description: string;
  typescript: RuntimeMeasurement;
  wasm: RuntimeMeasurement;
  speedup: number;
  winner: "typescript" | "wasm";
  equivalent: boolean;
  typescriptDigest: string;
  wasmDigest: string;
  outputLength: number;
  dispatch: DispatchDecision | { path: "hybrid"; reason: string; calibrationVersion: string };
}

export interface ExperimentReport {
  schemaVersion: "1.0.0";
  runId: string;
  createdAt: string;
  config: ExperimentConfig;
  capabilities: CapabilityReport;
  workloads: WorkloadResult[];
  wasmInitMs: number;
  workerElapsedMs: number;
  inputBytes: number;
  wasmAssetBytes: number;
  wasmMemoryBeforeBytes: number;
  wasmMemoryAfterBytes: number;
  userAgent: string;
}

export type WorkerRequest = {
  type: "run";
  config: ExperimentConfig;
  wasmUrl: string;
};

export type WorkerResponse =
  | { type: "ready" }
  | { type: "progress"; stage: string; percent: number }
  | { type: "complete"; report: ExperimentReport }
  | { type: "error"; message: string; stack?: string };

export interface BuildMetadata {
  builtAt: string;
  assets: {
    mainJavaScript: number;
    workerJavaScript: number;
    styles: number;
    wasm: number;
    total: number;
  };
}
