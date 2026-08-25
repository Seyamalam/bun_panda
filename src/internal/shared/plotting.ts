// Dependency-free plotting: ASCII + SVG renderers for Series/DataFrame.
import type { CellValue } from "../../types";
import { isMissing, numericValues } from "../../utils";

// ---------------------------------------------------------------------------
// Histogram (text)
// ---------------------------------------------------------------------------

export function asciiHistogram(
  values: CellValue[],
  bins = 10,
  width = 40
): string {
  const nums = numericValues(values).slice().sort((a, b) => a - b);
  if (nums.length === 0) return "(no numeric data)";
  const min = nums[0]!;
  const max = nums[nums.length - 1]!;
  const span = max - min || 1;
  const counts = new Array<number>(Math.max(1, bins)).fill(0);
  for (const n of nums) {
    const idx = Math.min(bins - 1, Math.floor(((n - min) / span) * bins));
    counts[idx]! += 1;
  }
  const peak = Math.max(...counts);
  const lines: string[] = [];
  for (let i = 0; i < counts.length; i += 1) {
    const lo = min + (span * i) / counts.length;
    const hi = min + (span * (i + 1)) / counts.length;
    const bar = "█".repeat(Math.round((counts[i]! / peak) * width));
    lines.push(`[${lo.toFixed(2)}, ${hi.toFixed(2)}) ${bar} ${counts[i]}`);
  }
  return lines.join("\n");
}

export function svgHistogram(values: CellValue[], bins = 10, height = 120): string {
  const nums = numericValues(values).sort((a, b) => a - b);
  if (nums.length === 0) return "<svg></svg>";
  const min = nums[0]!;
  const max = nums[nums.length - 1]!;
  const span = max - min || 1;
  const counts = new Array<number>(bins).fill(0);
  for (const n of nums) {
    counts[Math.min(bins - 1, Math.floor(((n - min) / span) * bins))]! += 1;
  }
  const peak = Math.max(...counts);
  const barWidth = 100 / bins;
  const bars = counts
    .map((c, i) => {
      const h = (c / peak) * height;
      return `<rect x="${(i * barWidth).toFixed(2)}" y="${(height - h).toFixed(2)}" width="${barWidth.toFixed(2)}" height="${h.toFixed(2)}" fill="steelblue"/>`;
    })
    .join("");
  return `<svg viewBox="0 0 100 ${height}" xmlns="http://www.w3.org/2000/svg">${bars}</svg>`;
}

// ---------------------------------------------------------------------------
// Line plot (SVG)
// ---------------------------------------------------------------------------

export function svgLine(values: CellValue[], height = 120): string {
  const nums = values.map((v) => (typeof v === "number" ? v : Number.NaN));
  const valid = nums.filter((n) => Number.isFinite(n));
  if (valid.length < 2) return "<svg></svg>";
  const min = Math.min(...valid);
  const max = Math.max(...valid);
  const spanY = max - min || 1;
  const stepX = 100 / (nums.length - 1);
  const points: string[] = [];
  for (let i = 0; i < nums.length; i += 1) {
    if (!Number.isFinite(nums[i]!)) continue;
    const x = (i * stepX).toFixed(2);
    const y = (height - ((nums[i]! - min) / spanY) * height).toFixed(2);
    points.push(`${x},${y}`);
  }
  return `<svg viewBox="0 0 100 ${height}" xmlns="http://www.w3.org/2000/svg"><polyline points="${points.join(" ")}" fill="none" stroke="steelblue" stroke-width="1"/></svg>`;
}

// ---------------------------------------------------------------------------
// Boxplot (text)
// ---------------------------------------------------------------------------

function quantileSorted(sorted: number[], q: number): number {
  const pos = (sorted.length - 1) * q;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  return sorted[lo]! + (sorted[hi]! - sorted[lo]!) * (pos - lo);
}

export interface BoxStats {
  min: number;
  q1: number;
  median: number;
  q3: number;
  max: number;
}

export function boxStats(values: CellValue[]): BoxStats | null {
  const nums = numericValues(values).sort((a, b) => a - b);
  if (nums.length === 0) return null;
  return {
    min: nums[0]!,
    q1: quantileSorted(nums, 0.25),
    median: quantileSorted(nums, 0.5),
    q3: quantileSorted(nums, 0.75),
    max: nums[nums.length - 1]!,
  };
}

export function asciiBoxplot(values: CellValue[], width = 50): string {
  const stats = boxStats(values);
  if (!stats) return "(no numeric data)";
  const { min, q1, median, q3, max } = stats;
  const span = max - min || 1;
  const at = (v: number) => Math.round(((v - min) / span) * (width - 1));
  const line = new Array<string>(width).fill("─");
  line[at(q1)] = "┌";
  line[at(q3)] = "┐";
  for (let i = at(q1) + 1; i < at(q3); i += 1) line[i] = "─";
  line[at(median)] = "│";
  const whiskerLow = new Array<string>(width).fill(" ");
  whiskerLow[min === max ? 0 : at(min)] = "○";
  const whiskerHigh = new Array<string>(width).fill(" ");
  whiskerHigh[at(max)] = "○";
  return [
    `min=${min.toFixed(2)}  q1=${q1.toFixed(2)}  med=${median.toFixed(2)}  q3=${q3.toFixed(2)}  max=${max.toFixed(2)}`,
    whiskerLow.join("") + whiskerHigh.join(""),
    line.join(""),
  ].join("\n");
}

// ---------------------------------------------------------------------------
// Sparse accessor (pandas-style density reporting)
// ---------------------------------------------------------------------------

export interface SparseInfo {
  density: number;
  filled: number;
  missing: number;
}

export function sparseInfo(values: CellValue[]): SparseInfo {
  const total = values.length;
  let missing = 0;
  for (const v of values) if (isMissing(v)) missing += 1;
  const filled = total - missing;
  return { density: total === 0 ? 1 : filled / total, filled, missing };
}
