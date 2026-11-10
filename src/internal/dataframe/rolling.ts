import { ValueError } from "../../errors";
import { windowNumber } from "../shared";
import type { CellValue } from "../../types";
import { isMissing, numericValues } from "../../utils";

export interface RollingWindow {
  mean(): CellValue[];
  sum(): CellValue[];
  min(): CellValue[];
  max(): CellValue[];
  count(): CellValue[];
  std(): CellValue[];
  median(): CellValue[];
}

/**
 * pandas treats booleans and bigints as numeric observations. Infinity is
 * numeric for the aggregate-gate (no DataError) but pandas' rolling engine
 * excludes it from observation slices (it is not finite), whereas `count`
 * tallies it as a non-missing value.
 */
function toWindowNumber(value: CellValue): number | null {
  if (isMissing(value)) return null;
  return windowNumber(value);
}

/**
 * pandas `Cannot aggregate non-numeric type` (DataError): numeric window
 * aggregations require every present cell to be number-like. `count` is
 * exempt — it tallies non-missing values of any dtype.
 */
export function assertWindowAggregable(values: CellValue[], dtype: string): void {
  for (const value of values) {
    if (!isMissing(value) && toWindowNumber(value) === null) {
      throw new ValueError(`Cannot aggregate non-numeric type: ${dtype}`);
    }
  }
}

function sumSlice(slice: number[]): number {
  return slice.reduce((s, v) => s + v, 0);
}

function meanSlice(slice: number[]): number {
  return sumSlice(slice) / slice.length;
}

/**
 * pandas rolling variance uses Welford's online algorithm with Kahan
 * compensation on the running mean (`add_var`/`remove_var` in
 * pandas._libs.window.aggregations); matching it keeps the oracle
 * bitwise-exact.
 */
interface VarState {
  nobs: number;
  mean: number;
  m2: number;
  compAdd: number;
  compRemove: number;
}

function varAdd(st: VarState, val: number): void {
  st.nobs += 1;
  const prevMean = st.mean - st.compAdd;
  const y = val - st.compAdd;
  const t = y - st.mean;
  st.compAdd = t + st.mean - y;
  const delta = t;
  st.mean = st.mean + delta / st.nobs;
  st.m2 = st.m2 + (val - prevMean) * (val - st.mean);
}

function varRemove(st: VarState, val: number): void {
  st.nobs -= 1;
  if (st.nobs) {
    const prevMean = st.mean - st.compRemove;
    const y = val - st.compRemove;
    const t = y - st.mean;
    st.compRemove = t + st.mean - y;
    const delta = t;
    st.mean = st.mean - delta / st.nobs;
    st.m2 = st.m2 - (val - prevMean) * (val - st.mean);
  } else {
    st.mean = 0;
    st.m2 = 0;
  }
}

function medianSlice(slice: number[]): number {
  const sorted = [...slice].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1
    ? sorted[mid]!
    : ((sorted[mid - 1]! + sorted[mid]!) as number) / 2;
}

interface WindowFrame {
  /** Numeric observations inside the window (missing/non-numeric excluded). */
  nums: number[];
  /** True when the window holds at least min_periods positions. */
  positionsFull: boolean;
  /** Non-missing cells inside the window (any kind). */
  present: number;
}

/**
 * pandas-style rolling window over a column's values. Windows shorter than
 * `window` (at the start) yield null unless `min_periods` is satisfied.
 * pandas gates count on window *positions* (a full window of NaNs reports 0)
 * and the numeric aggregations on the count of numeric observations.
 */
export function computeRolling(
  values: CellValue[],
  window: number,
  minPeriods: number | undefined
): RollingWindow {
  const effectiveMin = minPeriods ?? window;
  if (window < 1) {
    throw new Error("rolling window must be >= 1.");
  }

  const numeric = values.map(toWindowNumber);
  const present = values.map((value) => !isMissing(value));
  const frames: WindowFrame[] = [];
  const stdOut: CellValue[] = [];
  const varState: VarState = { nobs: 0, mean: 0, m2: 0, compAdd: 0, compRemove: 0 };
  for (let i = 0; i < values.length; i += 1) {
    const slice: number[] = [];
    let presentCount = 0;
    for (let j = Math.max(0, i - window + 1); j <= i; j += 1) {
      const v = numeric[j];
      if (v !== null && v !== undefined && Number.isFinite(v)) {
        slice.push(v);
      }
      if (present[j]) {
        presentCount += 1;
      }
    }
    frames.push({
      nums: slice,
      positionsFull: Math.min(i + 1, window) >= effectiveMin,
      present: presentCount,
    });
    // Incremental Welford pass matching pandas' add_var/remove_var; pandas
    // removes the outgoing value before adding the incoming one.
    const outPos = i - window;
    if (outPos >= 0) {
      const outV = numeric[outPos]!;
      if (outV !== null && outV !== undefined && Number.isFinite(outV)) {
        varRemove(varState, outV);
      }
    }
    const inV = numeric[i]!;
    if (inV !== null && inV !== undefined && Number.isFinite(inV)) {
      varAdd(varState, inV);
    }
    stdOut.push(
      varState.nobs >= effectiveMin && varState.nobs > 1
        ? Math.sqrt(varState.m2 / (varState.nobs - 1))
        : null
    );
  }

  return {
    mean() {
      return frames.map((f) =>
        f.nums.length >= effectiveMin ? meanSlice(f.nums) : null
      );
    },
    sum() {
      return frames.map((f) =>
        f.nums.length >= effectiveMin ? sumSlice(f.nums) : null
      );
    },
    min() {
      return frames.map((f) =>
        f.nums.length >= effectiveMin ? Math.min(...f.nums) : null
      );
    },
    max() {
      return frames.map((f) =>
        f.nums.length >= effectiveMin ? Math.max(...f.nums) : null
      );
    },
    count() {
      return frames.map((f) => (f.positionsFull ? f.present : null));
    },
    std() {
      return stdOut;
    },
    median() {
      return frames.map((f) =>
        f.nums.length >= effectiveMin ? medianSlice(f.nums) : null
      );
    },
  };
}

/**
 * Expanding window: cumulative statistics from the start to the
 * current row. `minPeriods` defaults to 1.
 */
export function computeExpanding(
  values: CellValue[],
  minPeriods = 1
): RollingWindow {
  const numeric = values.map(toWindowNumber);
  const present = values.map((value) => !isMissing(value));
  const seen: number[] = [];
  const frames: WindowFrame[] = [];
  const stdOut: CellValue[] = [];
  const varState: VarState = { nobs: 0, mean: 0, m2: 0, compAdd: 0, compRemove: 0 };
  let presentCount = 0;

  for (let i = 0; i < values.length; i += 1) {
    const v = numeric[i];
    if (v !== null && v !== undefined && Number.isFinite(v)) {
      seen.push(v);
      varAdd(varState, v);
    }
    if (present[i]) {
      presentCount += 1;
    }
    frames.push({
      nums: [...seen],
      positionsFull: i + 1 >= minPeriods,
      present: presentCount,
    });
    stdOut.push(
      varState.nobs >= minPeriods && varState.nobs > 1
        ? Math.sqrt(varState.m2 / (varState.nobs - 1))
        : null
    );
  }

  return {
    mean() {
      return frames.map((f) =>
        f.nums.length >= minPeriods ? meanSlice(f.nums) : null
      );
    },
    sum() {
      return frames.map((f) =>
        f.nums.length >= minPeriods ? sumSlice(f.nums) : null
      );
    },
    min() {
      return frames.map((f) =>
        f.nums.length >= minPeriods ? Math.min(...f.nums) : null
      );
    },
    max() {
      return frames.map((f) =>
        f.nums.length >= minPeriods ? Math.max(...f.nums) : null
      );
    },
    count() {
      return frames.map((f) => (f.positionsFull ? f.present : null));
    },
    std() {
      return stdOut;
    },
    median() {
      return frames.map((f) =>
        f.nums.length >= minPeriods ? medianSlice(f.nums) : null
      );
    },
  };
}

/** Convenience for tests and internal use. */
export function rollingNumeric(values: CellValue[]): number[] {
  return numericValues(values);
}
