import { expect, test } from "bun:test";
import { DataFrame, Series, NA, NaT } from "../src/index";
import type { CellValue, DType } from "../src/types";
import type { DTypeSelector } from "../src/dtypes";
import fixture from "./fixtures/pandas-dtype-queries.json";

function decode(value: unknown): CellValue {
  if (value && typeof value === "object" && "$missing" in value) return value.$missing === "NA" ? NA : value.$missing === "NaT" ? NaT : NaN;
  return value as CellValue;
}

for (const [i, entry] of fixture.cases.entries()) {
  test(`pandas dtype query ${i}: ${entry.api}`, () => {
    if (entry.api === "equals") {
      const left = new Series(entry.left!.values.map(decode), { dtype: entry.left!.dtype as DType });
      const right = new Series(entry.right!.values.map(decode), { dtype: entry.right!.dtype as DType });
      expect(left.equals(right)).toBe(entry.expected as boolean);
      expect(left.to_frame("v").equals(right.to_frame("v"))).toBe(entry.expected as boolean);
      return;
    }
    const frame = new DataFrame(Object.fromEntries(Object.keys(entry.columns!).map(key => [key, [1]]))).astype(entry.columns as Record<string, DType>);
    const run = () => frame.select_dtypes(entry.include as DTypeSelector | DTypeSelector[] | undefined ?? undefined, entry.exclude as DTypeSelector | undefined ?? undefined);
    if (entry.error) expect(run).toThrow();
    else {
      const result = run();
      expect(result.columns).toEqual(entry.expected as string[]);
      expect(result.index).toEqual([0]);
      for (const column of result.columns) expect(result.dtypes()[column]).toBe(frame.dtypes()[column]);
    }
  });
}
