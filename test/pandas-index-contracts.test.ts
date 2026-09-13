import { expect, test } from "bun:test";
import { DataFrame, Index, Series } from "../src/index";
import type { IndexLabel } from "../src/types";
import fixture from "./fixtures/pandas-index-contracts.json";

const decode = (value: unknown): IndexLabel => typeof value === "object" ? NaN : value as IndexLabel;
const encode = (value: unknown): unknown => typeof value === "number" && Number.isNaN(value) ? { $number: "NaN" } : value;

for (const [i, entry] of fixture.cases.entries()) {
  test(`pandas scalar index ${i}: ${entry.api}`, () => {
    const labels = entry.labels.map(decode), target = entry.target.map(decode);
    if (entry.api === "Index.equals") {
      expect<unknown>(new Index(labels).equals(new Index(target))).toBe(entry.expected);
      return;
    }
    const values = labels.map((_, i) => i);
    for (const source of [new Series(values, { index: labels, name: "v", dtype: "int64" }), new DataFrame({ v: values }, { index: labels, dtype: "int64" })]) {
      const run = () => source instanceof Series ? source.reindex(target) : source.reindex({ index: target }).get("v");
      if (entry.error) {
        try { run(); throw new Error("Expected pandas error"); }
        catch (error) { expect({ type: (error as Error).name, message: (error as Error).message }).toEqual(entry.error); }
      } else {
        const result = run();
        expect<unknown>({ dtype: result.dtype, values: result.to_list().map(encode), index: result.index.map(encode) }).toEqual(entry.expected);
      }
    }
  });
}

test("NaN labels support lookup and frame/Series equality", () => {
  const frame = new DataFrame({ v: [3, 4, 5] }, { index: [1, "1", NaN] });
  expect(frame.loc(NaN)).toEqual({ v: 5 });
  expect(frame.get("v").loc(NaN)).toBe(5);
  expect(frame.equals(frame.copy())).toBe(true);
  expect(frame.get("v").equals(frame.get("v").copy())).toBe(true);
  expect(frame.set_flags({ allows_duplicate_labels: false }).shape).toEqual([3, 1]);
  const other = new DataFrame({ v: [7, 8] }, { index: ["1", NaN] });
  expect(frame.combine_first(other).get("v").to_list()).toEqual([3, 4, 5]);
});
