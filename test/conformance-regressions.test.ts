import { describe, expect, test } from "bun:test";
import { DataFrame } from "../src/dataframe";
import { Series } from "../src/series";

describe("differential-conformance regressions", () => {
  test("drop_duplicates rejects a missing subset label", () => {
    const frame = new DataFrame([{ a: 1 }]);
    expect(() => frame.drop_duplicates(["missing"])).toThrow("does not exist");
  });

  test("shift and diff reject fractional periods", () => {
    const frame = new DataFrame([{ a: 1 }, { a: 2 }]);
    expect(() => frame.shift(1.5)).toThrow("periods must be an integer");
    expect(() => frame.diff(1.5)).toThrow("periods must be an integer");
  });

  test("dropna preserves dtype information when every row is removed", () => {
    const frame = new DataFrame([
      { group: "A", value: null, weight: 12.23, flag: true, label: "x" },
    ]);
    expect(frame.dropna(["value"]).dtypes()).toEqual({
      group: "string",
      value: "unknown",
      weight: "number",
      flag: "boolean",
      label: "string",
    });
  });

  test("diff retains numeric dtype for an all-missing numeric result", () => {
    const frame = new DataFrame([{ value: -12, weight: 30.17 }]);
    expect(frame.diff(-1).dtypes()).toEqual({ value: "number", weight: "number" });
  });

  test("diff rejects an object-like all-missing column when subtraction is required", () => {
    const frame = new DataFrame([
      { value: -2.96, weight: null },
      { value: -5.48, weight: null },
    ]);
    expect(() => frame.diff(-1)).toThrow(TypeError);
  });

  test("right merge follows right-key order and retains numeric/string dtype hints", () => {
    const left = new DataFrame([
      { group: "B", value: 32.73, label: "x" },
      { group: "B", value: -2.21, label: "x" },
    ]);
    const right = new DataFrame([
      { group: "D", right_value: -8.09 },
      { group: "A", right_value: -10.17 },
      { group: "B", right_value: -14.55 },
    ]);
    const result = left.merge(right, { on: "group", how: "right" });
    expect(result.to_records().map((row) => row.group)).toEqual(["D", "A", "B", "B"]);
    expect(result.dtypes()).toEqual({
      group: "string",
      value: "number",
      label: "string",
      right_value: "number",
    });
  });

  test("rank treats missing values as one tie group", () => {
    const frame = new DataFrame([
      { a: null },
      { a: 10 },
      { a: null },
      { a: 20 },
      { a: null },
    ]);

    expect(frame.rank({ method: "max", na_option: "top" }).to_dict("list")).toEqual({
      a: [3, 4, 3, 5, 3],
    });
    expect(frame.rank({ method: "average", na_option: "bottom" }).to_dict("list")).toEqual({
      a: [4, 1, 4, 2, 4],
    });
    expect(frame.rank({ method: "dense", na_option: "top" }).to_dict("list")).toEqual({
      a: [1, 2, 1, 3, 1],
    });
    expect(new Series([null, 10, null, 20, null]).rank({ method: "max", na_option: "top" }).to_list()).toEqual(
      [3, 4, 3, 5, 3]
    );
  });
});
