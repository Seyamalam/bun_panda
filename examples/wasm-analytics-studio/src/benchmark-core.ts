import type { DataShape, ExperimentConfig, RuntimeMeasurement } from "./types";

export interface Dataset {
  values: Float64Array;
  mask: Uint8Array;
  groupIds: Int32Array;
}

export function createRandom(initialSeed: number): () => number {
  let state = initialSeed >>> 0;
  return () => {
    state = (Math.imul(1664525, state) + 1013904223) >>> 0;
    return state / 2 ** 32;
  };
}

function valueForShape(random: () => number, index: number, rows: number, shape: DataShape): number {
  if (shape === "nearly-sorted") {
    const block = Math.floor(index / 32) * 32;
    return Math.min(rows, block + Math.floor(random() * 32));
  }
  if (shape === "skewed") return Math.floor(random() ** 4 * 10_000);
  return Math.floor(random() * 10_000);
}

export function createDataset(config: ExperimentConfig): Dataset {
  const random = createRandom(config.seed);
  const values = new Float64Array(config.rows);
  const mask = new Uint8Array(config.rows);
  const groupIds = new Int32Array(config.rows);
  for (let index = 0; index < config.rows; index += 1) {
    values[index] = valueForShape(random, index, config.rows, config.shape);
    mask[index] = random() < config.selectivity ? 1 : 0;
    const groupSample = config.shape === "skewed" ? random() ** 3 : random();
    groupIds[index] = Math.min(config.groups - 1, Math.floor(groupSample * config.groups));
  }
  return { values, mask, groupIds };
}

export function stableArgsort(values: Float64Array): Int32Array {
  return Int32Array.from(
    Array.from({ length: values.length }, (_, index) => index)
      .sort((left, right) => values[left]! - values[right]! || left - right),
  );
}

export function filterIndices(mask: Uint8Array): Int32Array {
  let count = 0;
  for (const value of mask) count += value === 0 ? 0 : 1;
  const output = new Int32Array(count);
  let cursor = 0;
  for (let index = 0; index < mask.length; index += 1) {
    if (mask[index] !== 0) output[cursor++] = index;
  }
  return output;
}

export function groupedSum(values: Float64Array, groupIds: Int32Array, groups: number): Float64Array {
  const output = new Float64Array(groups);
  for (let index = 0; index < values.length; index += 1) {
    output[groupIds[index]!] += values[index]!;
  }
  return output;
}

export function gather<T extends Float64Array | Int32Array>(source: T, indices: Int32Array): T {
  const Output = source.constructor as { new(length: number): T };
  const output = new Output(indices.length);
  for (let index = 0; index < indices.length; index += 1) output[index] = source[indices[index]!]!;
  return output;
}

export function quantile(samples: readonly number[], percentile: number): number {
  if (samples.length === 0) return 0;
  const sorted = [...samples].sort((left, right) => left - right);
  const position = (sorted.length - 1) * percentile;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  if (lower === upper) return sorted[lower]!;
  return sorted[lower]! + (sorted[upper]! - sorted[lower]!) * (position - lower);
}

export function summarize(samplesMs: number[]): RuntimeMeasurement {
  return {
    samplesMs,
    medianMs: quantile(samplesMs, 0.5),
    p95Ms: quantile(samplesMs, 0.95),
  };
}

export function byteLength(dataset: Dataset): number {
  return dataset.values.byteLength + dataset.mask.byteLength + dataset.groupIds.byteLength;
}
