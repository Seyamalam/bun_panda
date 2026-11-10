// Shared time-of-day parsing for Series/DataFrame at_time/between_time.
import type { CellValue } from "../../types";

/** Parses "HH:MM[:SS]" into seconds-of-day; null when invalid. */
export function parseTimeOfDay(text: string): number | null {
  const match = /^(\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(text.trim());
  if (!match) return null;
  const h = Number(match[1]);
  const m = Number(match[2]);
  const s = match[3] !== undefined ? Number(match[3]) : 0;
  if (h > 23 || m > 59 || s > 59) return null;
  return h * 3600 + m * 60 + s;
}

/** Seconds-of-day for a Date-like cell; null when the value is not datetime-like. */
export function secondsOfDay(value: CellValue): number | null {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.getHours() * 3600 + value.getMinutes() * 60 + value.getSeconds();
  }
  if (typeof value === "string") {
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return null;
    return parsed.getHours() * 3600 + parsed.getMinutes() * 60 + parsed.getSeconds();
  }
  return null;
}

/**
 * IEEE-754 fused multiply-add: RN(a*b + c) with a single rounding.
 * pandas' Cython ewm kernels compile expressions like `old_wt * avg + cur`
 * into FMA on common platforms, which differs from separate multiply+add by
 * one ULP. Dekker's TwoProduct + 2Sum emulates it exactly (barring overflow).
 */
const FMA_SPLITTER = 134217729; // 2**27 + 1

function fmaSplit(a: number): [number, number] {
  const c = FMA_SPLITTER * a;
  const hi = c - (c - a);
  return [hi, a - hi];
}

export function fma(a: number, b: number, c: number): number {
  // TwoProdFMA (Dekker): a*b = p + err, exactly.
  const p = a * b;
  const [aHi, aLo] = fmaSplit(a);
  const [bHi, bLo] = fmaSplit(b);
  const err = aHi * bHi - p + aHi * bLo + aLo * bHi + aLo * bLo;
  // 2Sum(p, c): p + c = s + err2, exactly.
  const s = p + c;
  const bp = s - p;
  const err2 = p - (s - bp) + (c - bp);
  // a*b + c = s + err + err2; a single final rounding emulates the fuse.
  return s + (err + err2);
}

/**
 * Numericizes a cell for rolling/expanding/ewm scans the way pandas does:
 * booleans become 1/0, bigints become numbers, and infinities count as
 * observations. Strings/objects/missing values return null.
 */
export function windowNumber(value: CellValue): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isNaN(value) ? null : value;
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "boolean") return value ? 1 : 0;
  return null;
}

/**
 * Exponentially weighted scan matching pandas ewm(adjust=True,
 * ignore_na=False) — a direct port of pandas' `_libs.window.aggregations`
 * `ewm`/`ewmcov` so results stay bitwise aligned. Infinity is treated as a
 * non-observation (pandas' float64 prep casts it to NaN). Decay weights
 * advance at every position including missing ones; the running weighted
 * average and covariance carry forward across missing positions.
 */
export function ewmValues(
  values: (number | null)[],
  span: number,
  minPeriods: number,
  kind: "mean" | "sum" | "std"
): (number | null)[] {
  const alpha = 2 / (span + 1);
  const oldWtFactor = 1 - alpha;
  const out: (number | null)[] = new Array(values.length).fill(null);
  if (values.length === 0) return out;

  const first = values[0]!;
  const firstObs = first !== null && Number.isFinite(first);
  let nobs = firstObs ? 1 : 0;

  if (kind === "mean" || kind === "sum") {
    const normalize = kind === "mean";
    let weighted = firstObs ? (first as number) : Number.NaN;
    let oldWt = 1;
    const emit = (v: number) => (nobs >= minPeriods && !Number.isNaN(v) ? v : null);
    out[0] = emit(weighted);
    for (let i = 1; i < values.length; i += 1) {
      const cur = values[i]!;
      const isObs = cur !== null && Number.isFinite(cur);
      nobs += isObs ? 1 : 0;
      if (!Number.isNaN(weighted)) {
        if (normalize) {
          oldWt *= oldWtFactor;
          if (isObs) {
            if (weighted !== cur) {
              // pandas' Cython emits fma(old_wt, weighted, new_wt * cur).
              weighted = fma(oldWt, weighted, cur as number);
              weighted /= oldWt + 1;
            }
            oldWt += 1;
          }
        } else {
          weighted = oldWtFactor * weighted;
          if (isObs) weighted += cur as number;
        }
      } else if (isObs) {
        weighted = cur as number;
      }
      out[i] = emit(weighted);
    }
    return out;
  }

  // pandas ewmcov(x, x, bias=False): running weighted mean + covariance with
  // an unbiased correction of sum_wt² / (sum_wt² − sum_wt2).
  let meanX = firstObs ? (first as number) : Number.NaN;
  let cov = 0;
  let sumWt = 1;
  let sumWt2 = 1;
  let oldWt = 1;
  const emitStd = (): number | null => {
    if (nobs < minPeriods) return null;
    const numerator = sumWt * sumWt;
    const denominator = numerator - sumWt2;
    if (denominator <= 0) return null;
    const variance = (numerator / denominator) * cov;
    const s = Math.sqrt(variance);
    return Number.isNaN(s) ? null : s;
  };
  out[0] = emitStd();
  for (let i = 1; i < values.length; i += 1) {
    const cur = values[i]!;
    const isObs = cur !== null && Number.isFinite(cur);
    nobs += isObs ? 1 : 0;
    if (!Number.isNaN(meanX)) {
      sumWt *= oldWtFactor;
      sumWt2 *= oldWtFactor * oldWtFactor;
      oldWt *= oldWtFactor;
      if (isObs) {
        const oldMean = meanX;
        if (meanX !== cur) {
          // pandas' Cython emits fma(old_wt, old_mean, new_wt * cur).
          meanX = fma(oldWt, oldMean, cur as number) / (oldWt + 1);
        }
        // pandas' Cython fuses cov + (old_mean - mean)² into
        // fma(old_mean - mean, old_mean - mean, cov).
        const meanDelta = oldMean - meanX;
        cov =
          fma(
            oldWt,
            fma(meanDelta, meanDelta, cov),
            ((cur as number) - meanX) * ((cur as number) - meanX)
          ) / (oldWt + 1);
        sumWt += 1;
        sumWt2 += 1;
        oldWt += 1;
      }
    } else if (isObs) {
      meanX = cur as number;
    }
    out[i] = emitStd();
  }
  return out;
}

/** Parses a pandas-style frequency string into milliseconds. */
export function parseFreqMs(freq: string): number {
  const match = /^(\d+)?\s*(ms|s|sec|min|T|h|H|D|day|W)$/.exec(freq.trim());
  if (!match) throw new Error(`Unsupported frequency '${freq}'.`);
  const n = match[1] ? Number(match[1]) : 1;
  switch (match[2]) {
    case "ms": return n;
    case "s":
    case "sec": return n * 1000;
    case "min":
    case "T": return n * 60_000;
    case "h":
    case "H": return n * 3_600_000;
    case "D":
    case "day": return n * 86_400_000;
    case "W": return n * 7 * 86_400_000;
  }
  throw new Error(`Unsupported frequency '${freq}'.`);
}
