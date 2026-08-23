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
 * pandas-style rolling window over a numeric column's values. Windows
 * shorter than `window` (at the start) yield null unless min_periods
 * is satisfied by the available entries.
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

  const windows = (): number[][] => {
    const out: number[][] = [];
    const numeric = values.map((value) =>
      !isMissing(value) && typeof value === "number" && Number.isFinite(value)
        ? value
        : null
    );
    for (let i = 0; i < values.length; i += 1) {
      const slice: number[] = [];
      for (let j = Math.max(0, i - window + 1); j <= i; j += 1) {
        const v = numeric[j];
        if (v !== null && v !== undefined) {
          slice.push(v);
        }
      }
      out.push(slice.length >= effectiveMin ? slice : []);
    }
    return out;
  };

  const computed = windows();

  return {
    mean() {
      return computed.map((slice) =>
        slice.length ? slice.reduce((s, v) => s + v, 0) / slice.length : null
      );
    },
    sum() {
      return computed.map((slice) =>
        slice.length ? slice.reduce((s, v) => s + v, 0) : null
      );
    },
    min() {
      return computed.map((slice) => (slice.length ? Math.min(...slice) : null));
    },
    max() {
      return computed.map((slice) => (slice.length ? Math.max(...slice) : null));
    },
    count() {
      return computed.map((slice) => slice.length);
    },
    std() {
      return computed.map((slice) => {
        if (slice.length < 2) {
          return null;
        }
        const mean = slice.reduce((s, v) => s + v, 0) / slice.length;
        const variance =
          slice.reduce((s, v) => s + (v - mean) ** 2, 0) / (slice.length - 1);
        return Math.sqrt(variance);
      });
    },
    median() {
      return computed.map((slice) => {
        if (!slice.length) {
          return null;
        }
        const sorted = [...slice].sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        return sorted.length % 2 === 1
          ? sorted[mid]!
          : ((sorted[mid - 1]! + sorted[mid]!) as number) / 2;
      });
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
  const seen: number[] = [];
  const perRow: number[][] = [];

  for (const value of values) {
    if (
      !isMissing(value) &&
      typeof value === "number" &&
      Number.isFinite(value)
    ) {
      seen.push(value);
    }
    perRow.push(seen.length >= minPeriods ? [...seen] : []);
  }

  return {
    mean() {
      return perRow.map((slice) =>
        slice.length ? slice.reduce((s, v) => s + v, 0) / slice.length : null
      );
    },
    sum() {
      return perRow.map((slice) =>
        slice.length ? slice.reduce((s, v) => s + v, 0) : null
      );
    },
    min() {
      return perRow.map((slice) => (slice.length ? Math.min(...slice) : null));
    },
    max() {
      return perRow.map((slice) => (slice.length ? Math.max(...slice) : null));
    },
    count() {
      return perRow.map((slice) => slice.length);
    },
    std() {
      return perRow.map((slice) => {
        if (slice.length < 2) {
          return null;
        }
        const mean = slice.reduce((s, v) => s + v, 0) / slice.length;
        const variance =
          slice.reduce((s, v) => s + (v - mean) ** 2, 0) / (slice.length - 1);
        return Math.sqrt(variance);
      });
    },
    median() {
      return perRow.map((slice) => {
        if (!slice.length) {
          return null;
        }
        const sorted = [...slice].sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        return sorted.length % 2 === 1
          ? sorted[mid]!
          : ((sorted[mid - 1]! + sorted[mid]!) as number) / 2;
      });
    },
  };
}

/** Convenience for tests and internal use. */
export function rollingNumeric(values: CellValue[]): number[] {
  return numericValues(values);
}
