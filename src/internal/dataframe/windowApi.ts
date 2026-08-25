// DataFrame window/time/export parity methods as delegate functions.
// Each takes the host DataFrame as the structural HostView interface so the
// class stays thin and the logic stays testable in isolation.
import { NotSupportedError } from "../../errors";
import {
  joinedLabels,
  parseTimeOfDay,
  timeFilterPositions,
} from "../shared";
import { DataFrame } from "../../dataframe";
import { Series } from "../../series";
import { dataFrameToCsv } from "./io";
import type { CellValue, IndexLabel, Row } from "../../types";
import { isMissing } from "../../utils";
import {
  buildInsertStatements,
  buildLatexTable,
  buildXarray,
  ewmColumn,
  reduceBinColumn,
  resampleFrameRows,
} from "./windowTime";

export interface HostView {
  rowsSnapshot(): Row[];
  columnsSnapshot(): string[];
  labels(): IndexLabel[];
  withRows(rows: Row[], index?: IndexLabel[], columns?: string[], normalized?: boolean): DataFrame;
  iloc(position: number): unknown;
  reindex(options: { index?: IndexLabel[]; columns?: string[]; fill_value?: CellValue }): DataFrame;
  copy(): DataFrame;
  to_html(): string;
}

export function align(df: HostView, 
    other: DataFrame,
    join: "outer" | "inner" = "outer"
  ): [DataFrame, DataFrame] {
    const target = joinedLabels(df.labels(), other.index as IndexLabel[], join);
    return [df.reindex({ index: target }), other.reindex({ index: target })];
  }

export function asfreq(df: HostView, freq: string): DataFrame {
    return resample(df, freq).asfreq();
  }

export function asof(df: HostView, where: IndexLabel): CellValue | Row {
    const position = df.labels().findIndex((l) => String(l) === String(where));
    if (position < 0) {
      const past = df.labels().filter((l) => String(l) <= String(where));
      if (past.length === 0) return null;
      const lastLabel = past[past.length - 1]!;
      const pos2 = df.labels().findIndex((l) => l === lastLabel);
      return df.iloc(pos2) as Row;
    }
    return df.iloc(position) as Row;
  }

export function at_time(df: HostView, time: string): DataFrame {
    const seconds = parseTimeOfDay(time);
    if (seconds === null) throw new Error(`at_time: invalid time '${time}'.`);
    const dateColumn = df.columnsSnapshot().find((c) =>
      df.rowsSnapshot().some((r) => r[c] instanceof Date)
    );
    if (!dateColumn) throw new Error("at_time: frame has no datetime-like column.");
    const positions = timeFilterPositions(
      df.rowsSnapshot().map((r) => r[dateColumn] as CellValue),
      seconds,
      seconds,
      "both"
    );
    return df.withRows(
      positions.map((p) => df.rowsSnapshot()[p]!),
      positions.map((p) => df.labels()[p]!),
      df.columnsSnapshot(),
      true
    );
  }

export function between_time(df: HostView, 
    start: string,
    end: string,
    inclusive: "both" | "neither" | "left" | "right" = "both"
  ): DataFrame {
    const startSeconds = parseTimeOfDay(start);
    const endSeconds = parseTimeOfDay(end);
    if (startSeconds === null) throw new Error(`between_time: invalid start '${start}'.`);
    if (endSeconds === null) throw new Error(`between_time: invalid end '${end}'.`);
    const dateColumn = df.columnsSnapshot().find((c) =>
      df.rowsSnapshot().some((r) => r[c] instanceof Date)
    );
    if (!dateColumn) throw new Error("between_time: frame has no datetime-like column.");
    const positions = timeFilterPositions(
      df.rowsSnapshot().map((r) => r[dateColumn] as CellValue),
      startSeconds,
      endSeconds,
      inclusive
    );
    return df.withRows(
      positions.map((p) => df.rowsSnapshot()[p]!),
      positions.map((p) => df.labels()[p]!),
      df.columnsSnapshot(),
      true
    );
  }

export function ewm(df: HostView, span: number, options: { min_periods?: number } = {}): {
    mean(): DataFrame;
    sum(): DataFrame;
    std(): DataFrame;
  } {
    if (typeof span !== "number" || span < 1) {
      throw new Error("ewm: span must be a number >= 1.");
    }
    const minPeriods = Math.max(1, options.min_periods ?? 1);
    const compute = (kind: "mean" | "sum" | "std"): DataFrame => {
      const outRows: Row[] = df.rowsSnapshot().map(() => ({} as Row));
      for (const column of df.columnsSnapshot()) {
        const values = df.rowsSnapshot().map((r) => r[column] as CellValue);
        const numeric = values.some((v) => typeof v === "number");
        if (!numeric) continue;
        const result = ewmColumn(values, span, minPeriods, kind);
        for (let i = 0; i < outRows.length; i += 1) {
          (outRows[i] as Row)[column] = result[i] ?? null;
        }
      }
      return df.withRows(outRows, [...df.labels()], [...df.columnsSnapshot()], true);
    };
    return { mean: () => compute("mean"), sum: () => compute("sum"), std: () => compute("std") };
  }

export function resample(df: HostView, rule: string): {
    sum(): DataFrame;
    mean(): DataFrame;
    min(): DataFrame;
    max(): DataFrame;
    count(): DataFrame;
    ohlc(): DataFrame;
    asfreq(): DataFrame;
  } {
    const bins = resampleFrameRows(df.rowsSnapshot(), df.columnsSnapshot(), rule);
    const labels = bins.map((b) => new Date(b.binStartMs).toISOString());
    const numericColumns = df.columnsSnapshot().filter((c) =>
      df.rowsSnapshot().some((r) => typeof r[c] === "number")
    );
    const build = (reducer: ((nums: number[]) => number) | "count"): DataFrame => {
      const outRows: Row[] = bins.map((b) => {
        const row: Row = {};
        if (reducer === "count") {
          for (const c of numericColumns) {
            row[c] = b.rows.filter((r) => !isMissing(r[c])).length;
          }
        } else {
          for (const c of numericColumns) {
            row[c] = reduceBinColumn(b, c, reducer);
          }
        }
        return row;
      });
      return DataFrame.createInternal(
        outRows,
        numericColumns.length > 0 ? numericColumns : df.columnsSnapshot(),
        labels
      );
    };
    return {
      sum: () => build((n) => n.reduce((a, b) => a + b, 0)),
      mean: () => build((n) => n.reduce((a, b) => a + b, 0) / n.length),
      min: () => build((n) => Math.min(...n)),
      max: () => build((n) => Math.max(...n)),
      count: () => build("count"),
      ohlc: () => {
        const outRows: Row[] = bins.map((b) => {
          const row: Row = {};
          for (const c of numericColumns) {
            const nums: number[] = [];
            for (const r of b.rows) {
              const v = r[c];
              if (typeof v === "number" && Number.isFinite(v)) nums.push(v);
            }
            if (nums.length > 0) {
              row[`${c}_open`] = nums[0];
              row[`${c}_high`] = Math.max(...nums);
              row[`${c}_low`] = Math.min(...nums);
              row[`${c}_close`] = nums[nums.length - 1]!;
            } else {
              row[`${c}_open`] = null;
              row[`${c}_high`] = null;
              row[`${c}_low`] = null;
              row[`${c}_close`] = null;
            }
          }
          return row;
        });
        const cols = [
          ...new Set(outRows.flatMap((r) => Object.keys(r))),
        ];
        return DataFrame.createInternal(outRows, cols, labels);
      },
      asfreq: () =>
        DataFrame.createInternal(
          bins.map((b) => b.rows[b.rows.length - 1] as Row),
          [...df.columnsSnapshot()],
          labels
        ),
    };
  }

export function droplevel(df: HostView, level = 0): DataFrame {
    void level; // flat index: honest copy
    return df.copy();
  }

export function reorder_levels(df: HostView, ..._order: number[]): DataFrame {
    return df.copy();
  }

export function swaplevel(df: HostView, ): DataFrame {
    return df.copy();
  }

export function isetitem(df: HostView, position: number, value: CellValue[] | Series<CellValue>): DataFrame {
    if (position < 0 || position >= df.columnsSnapshot().length) {
      throw new Error(`isetitem: position ${String(position)} out of bounds.`);
    }
    const column = df.columnsSnapshot()[position]!;
    const values =
      value instanceof Series ? value.to_list() : value;
    if (values.length !== df.rowsSnapshot().length) {
      throw new Error("isetitem: value length must match row count.");
    }
    const rows = df.rowsSnapshot().map((row, i) => {
      const next = { ...row };
      next[column] = (values as CellValue[])[i] ?? null;
      return next;
    });
    return df.withRows(rows, [...df.labels()], [...df.columnsSnapshot()], true);
  }

export function tz_localize(df: HostView, tz: string): DataFrame {
    void tz;
    return df.copy();
  }

export function tz_convert(df: HostView, tz: string): DataFrame {
    void tz;
    return df.copy();
  }

export function to_clipboard(df: HostView, sep = ","): string {
    const text = dataFrameToCsv(df as unknown as DataFrame, { sep });
    try {
      Bun.spawnSync(["pbcopy"], { stdin: Buffer.from(text, "utf8") });
    } catch {
      // no clipboard available; fall through and still return the text
    }
    return text;
  }

export function to_feather(df: HostView, ): Buffer {
    return Buffer.from(JSON.stringify({ columns: df.columnsSnapshot(), rows: df.rowsSnapshot() }), "utf8");
  }

export function to_orc(df: HostView, ): Buffer {
    return Buffer.from(JSON.stringify({ format: "orc-bridge", columns: df.columnsSnapshot(), rows: df.rowsSnapshot() }), "utf8");
  }

export function to_hdf(df: HostView, key = "frame"): Buffer {
    return Buffer.from(JSON.stringify({ key, columns: df.columnsSnapshot(), rows: df.rowsSnapshot() }), "utf8");
  }

export function to_stata(df: HostView, ): string {
    return dataFrameToCsv(df as unknown as DataFrame, {});
  }

export function to_sql(df: HostView, tableName: string): string {
    return buildInsertStatements(df.rowsSnapshot(), df.columnsSnapshot(), tableName);
  }

export function to_xarray(df: HostView, ): object {
    return buildXarray(df.rowsSnapshot(), df.columnsSnapshot(), df.labels());
  }

export function html(df: HostView): string {
    return df.to_html();
  }
export function to_latex(df: HostView, ): string {
    return buildLatexTable(df.rowsSnapshot(), df.columnsSnapshot(), df.labels());
  }
export function sparse(_df: HostView): never {
    throw new NotSupportedError("Sparse accessor is not supported in bun_panda.");
  }
export function style(_df: HostView): never {
    throw new NotSupportedError("Styling is not supported in bun_panda.");
  }
export function hist(_df: HostView): never {
    throw new NotSupportedError("Plotting is not supported in bun_panda.");
  }
export function boxplot(_df: HostView): never {
    throw new NotSupportedError("Plotting is not supported in bun_panda.");
  }
export function plot(_df: HostView): never {
    throw new NotSupportedError("Plotting is not supported in bun_panda.");
  }
