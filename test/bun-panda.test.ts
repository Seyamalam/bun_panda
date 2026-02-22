import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  DataFrame,
  concat,
  merge,
  parse_csv,
  pivot_table,
  read_csv_sync,
} from "../index";

const tempDir = mkdtempSync(join(tmpdir(), "bun-panda-"));

afterAll(() => {
  rmSync(tempDir, { recursive: true, force: true });
});

describe("DataFrame basics", () => {
  test("shape, head, and missing-value operations", () => {
    const df = new DataFrame([
      { city: "Austin", temp: 30, rain: 1.2 },
      { city: "Seattle", temp: 18, rain: null },
    ]);

    expect(df.shape).toEqual([2, 3]);
    expect(df.head(1).to_records()).toEqual([{ city: "Austin", temp: 30, rain: 1.2 }]);
    expect(df.dropna().shape).toEqual([1, 3]);
    expect(df.fillna(0).to_records()[1]?.rain).toBe(0);
  });

  test("column selection and Series math", () => {
    const df = new DataFrame([
      { a: 1, b: 10 },
      { a: 2, b: 20 },
      { a: 3, b: null },
    ]);

    expect(df.get("a").mean()).toBe(2);
    expect(df.get("b").sum()).toBe(30);
    expect(df.select(["a"]).columns).toEqual(["a"]);
  });
});

describe("dtypes and astype", () => {
  test("dtypes infers simple and mixed column types", () => {
    const df = new DataFrame([
      { a: 1, b: "x", c: true, d: null, e: 1 },
      { a: 2, b: "y", c: false, d: null, e: "oops" },
    ]);

    expect(df.dtypes()).toEqual({
      a: "number",
      b: "string",
      c: "boolean",
      d: "unknown",
      e: "mixed",
    });
  });

  test("astype can cast selected columns", () => {
    const df = new DataFrame([
      { amount: "10.5", active: "true", when: "2026-01-01T00:00:00.000Z" },
      { amount: "3", active: "false", when: "bad-date" },
    ]).astype({
      amount: "number",
      active: "boolean",
      when: "date",
    });

    expect(df.get("amount").to_list()).toEqual([10.5, 3]);
    expect(df.get("active").to_list()).toEqual([true, false]);
    const values = df.get("when").to_list();
    expect(values[0]).toBeInstanceOf(Date);
    expect(values[1]).toBeNull();
  });

  test("astype can cast all columns with a scalar dtype", () => {
    const df = new DataFrame([
      { a: 1, b: true },
      { a: 0, b: false },
    ]).astype("string");

    expect(df.to_records()).toEqual([
      { a: "1", b: "true" },
      { a: "0", b: "false" },
    ]);
  });

  test("astype throws for unknown column in mapping", () => {
    const df = new DataFrame([{ amount: "10" }]);
    expect(() => df.astype({ missing_col: "number" })).toThrow(
      "Column 'missing_col' does not exist."
    );
  });
});

describe("groupby + merge + concat", () => {
  test("groupby agg behaves like pandas-style mean", () => {
    const df = new DataFrame([
      { team: "A", points: 10 },
      { team: "A", points: 20 },
      { team: "B", points: 15 },
    ]);

    const grouped = df.groupby("team").agg({ points: "mean" }).sort_values("team");
    expect(grouped.to_records()).toEqual([
      { team: "A", points: 15 },
      { team: "B", points: 15 },
    ]);
  });

  test("merge right join includes unmatched right rows", () => {
    const left = new DataFrame([
      { id: 1, name: "Ada" },
      { id: 2, name: "Grace" },
    ]);
    const right = new DataFrame([
      { id: 2, score: 95 },
      { id: 3, score: 99 },
    ]);

    const joined = merge(left, right, { on: "id", how: "right" }).sort_values("id");
    expect(joined.to_records()).toEqual([
      { id: 2, name: "Grace", score: 95 },
      { id: 3, name: undefined, score: 99 },
    ]);
  });

  test("merge outer join keeps unmatched rows from both sides", () => {
    const left = new DataFrame([
      { id: 1, name: "Ada" },
      { id: 2, name: "Grace" },
    ]);
    const right = new DataFrame([
      { id: 2, score: 95 },
      { id: 4, score: 88 },
    ]);

    const joined = merge(left, right, { on: "id", how: "outer" }).sort_values("id");
    expect(joined.to_records()).toEqual([
      { id: 1, name: "Ada", score: undefined },
      { id: 2, name: "Grace", score: 95 },
      { id: 4, name: undefined, score: 88 },
    ]);
  });

  test("merge applies suffixes when non-key columns overlap", () => {
    const left = new DataFrame([{ id: 1, score: 50 }]);
    const right = new DataFrame([{ id: 1, score: 70 }]);
    const joined = merge(left, right, {
      on: "id",
      suffixes: ["_left", "_right"],
    });

    expect(joined.columns).toEqual(["id", "score_left", "score_right"]);
    expect(joined.to_records()).toEqual([{ id: 1, score_left: 50, score_right: 70 }]);
  });

  test("concat preserves expected columns", () => {
    const left = new DataFrame([
      { id: 1, name: "Ada" },
      { id: 2, name: "Grace" },
    ]);
    const right = new DataFrame([{ id: 3, name: "Linus", score: 99 }]);

    const stacked = concat([left, right], { axis: 0, ignore_index: true });
    expect(stacked.shape).toEqual([3, 3]);
    expect(stacked.iloc(2)).toEqual({ id: 3, name: "Linus", score: 99 });
  });
});

describe("indexing and dedup operations", () => {
  test("sort_values supports multiple columns with per-column ascending", () => {
    const df = new DataFrame([
      { team: "A", points: 20, player: "z" },
      { team: "A", points: 20, player: "a" },
      { team: "A", points: 10, player: "m" },
      { team: "B", points: 50, player: "n" },
      { team: "B", points: 60, player: "b" },
    ]);

    const sorted = df.sort_values(["team", "points", "player"], [true, false, true]);
    expect(sorted.to_records()).toEqual([
      { team: "A", points: 20, player: "a" },
      { team: "A", points: 20, player: "z" },
      { team: "A", points: 10, player: "m" },
      { team: "B", points: 60, player: "b" },
      { team: "B", points: 50, player: "n" },
    ]);
  });

  test("sort_values throws when ascending array length does not match columns", () => {
    const df = new DataFrame([
      { a: 1, b: 2 },
      { a: 2, b: 1 },
    ]);
    expect(() => df.sort_values(["a", "b"], [true])).toThrow(
      "Length mismatch for ascending. Expected 2, received 1."
    );
  });

  test("sort_values limit matches full sort + head for single column", () => {
    const df = new DataFrame([
      { id: 1, score: 30 },
      { id: 2, score: 10 },
      { id: 3, score: 50 },
      { id: 4, score: 20 },
      { id: 5, score: 40 },
    ]);

    const limited = df.sort_values("score", false, 3).to_records();
    const fullThenHead = df.sort_values("score", false).head(3).to_records();
    expect(limited).toEqual(fullThenHead);
  });

  test("sort_values limit matches full sort + head for multi-column sort", () => {
    const df = new DataFrame([
      { team: "A", points: 20, id: 2 },
      { team: "A", points: 20, id: 1 },
      { team: "B", points: 10, id: 3 },
      { team: "B", points: 30, id: 4 },
      { team: "C", points: 30, id: 5 },
    ]);

    const limited = df
      .sort_values(["points", "team", "id"], [false, true, true], 4)
      .to_records();
    const fullThenHead = df
      .sort_values(["points", "team", "id"], [false, true, true])
      .head(4)
      .to_records();
    expect(limited).toEqual(fullThenHead);
  });

  test("sort_values limit validates non-negative integer", () => {
    const df = new DataFrame([{ a: 1 }]);
    expect(() => df.sort_values("a", true, -1)).toThrow(
      "limit must be a non-negative integer."
    );
    expect(() => df.sort_values("a", true, 1.5)).toThrow(
      "limit must be a non-negative integer."
    );
  });

  test("sort_values limit 0 returns empty frame", () => {
    const df = new DataFrame([
      { a: 1 },
      { a: 2 },
    ]);
    expect(df.sort_values("a", true, 0).shape).toEqual([0, 1]);
  });

  test("sort_index supports ascending and descending order", () => {
    const df = new DataFrame(
      [
        { x: "b" },
        { x: "c" },
        { x: "a" },
      ],
      { index: [20, 30, 10] }
    );

    expect(df.sort_index().index).toEqual([10, 20, 30]);
    expect(df.sort_index(false).index).toEqual([30, 20, 10]);
  });

  test("drop_duplicates keeps first or last", () => {
    const df = new DataFrame([
      { id: 1, city: "Austin" },
      { id: 1, city: "Dallas" },
      { id: 2, city: "Seattle" },
      { id: 2, city: "Tacoma" },
      { id: 3, city: "Boston" },
    ]);

    expect(df.drop_duplicates("id", "first").to_records()).toEqual([
      { id: 1, city: "Austin" },
      { id: 2, city: "Seattle" },
      { id: 3, city: "Boston" },
    ]);

    expect(df.drop_duplicates("id", "last").to_records()).toEqual([
      { id: 1, city: "Dallas" },
      { id: 2, city: "Tacoma" },
      { id: 3, city: "Boston" },
    ]);
  });

  test("drop_duplicates with keep=false removes all duplicate groups", () => {
    const df = new DataFrame([
      { id: 1, city: "Austin" },
      { id: 1, city: "Austin" },
      { id: 2, city: "Seattle" },
      { id: 3, city: "Boston" },
    ]);

    expect(df.drop_duplicates("id", false).to_records()).toEqual([
      { id: 2, city: "Seattle" },
      { id: 3, city: "Boston" },
    ]);
  });

  test("drop_duplicates can reset index with ignore_index", () => {
    const df = new DataFrame(
      [
        { id: 1, city: "Austin" },
        { id: 1, city: "Dallas" },
        { id: 2, city: "Seattle" },
      ],
      { index: [10, 11, 20] }
    );

    const deduped = df.drop_duplicates("id", "first", true);
    expect(deduped.index).toEqual([0, 1]);
    expect(deduped.to_records()).toEqual([
      { id: 1, city: "Austin" },
      { id: 2, city: "Seattle" },
    ]);
  });

  test("value_counts supports subset and normalization", () => {
    const df = new DataFrame([
      { team: "A", city: "Austin" },
      { team: "A", city: "Austin" },
      { team: "A", city: "Seattle" },
      { team: "B", city: "Austin" },
    ]);

    expect(df.value_counts({ subset: ["team"] }).to_records()).toEqual([
      { team: "A", count: 3 },
      { team: "B", count: 1 },
    ]);

    expect(df.value_counts({ subset: ["team"], normalize: true }).to_records()).toEqual([
      { team: "A", proportion: 0.75 },
      { team: "B", proportion: 0.25 },
    ]);
  });

  test("value_counts dropna option can include missing rows", () => {
    const df = new DataFrame([
      { team: "A", city: null },
      { team: "A", city: null },
      { team: "B", city: "Austin" },
    ]);

    expect(df.value_counts({ subset: ["team", "city"] }).to_records()).toEqual([
      { team: "B", city: "Austin", count: 1 },
    ]);
    expect(df.value_counts({ subset: ["team", "city"], dropna: false }).to_records()).toEqual([
      { team: "A", city: null, count: 2 },
      { team: "B", city: "Austin", count: 1 },
    ]);
  });

  test("value_counts sorts ties deterministically", () => {
    const df = new DataFrame([
      { code: "b" },
      { code: "a" },
      { code: "a" },
      { code: "b" },
    ]);

    expect(df.value_counts({ subset: ["code"] }).to_records()).toEqual([
      { code: "a", count: 2 },
      { code: "b", count: 2 },
    ]);
  });
});

describe("pivot table", () => {
  test("pivot_table without columns acts as grouped aggregation", () => {
    const df = new DataFrame([
      { team: "A", value: 10 },
      { team: "A", value: 20 },
      { team: "B", value: 7 },
    ]);

    const table = df.pivot_table({
      index: "team",
      values: "value",
      aggfunc: "mean",
    });
    expect(table.sort_values("team").to_records()).toEqual([
      { team: "A", value: 15 },
      { team: "B", value: 7 },
    ]);
  });

  test("pivot_table with columns flattens output columns", () => {
    const df = new DataFrame([
      { team: "A", quarter: "Q1", sales: 10 },
      { team: "A", quarter: "Q2", sales: 20 },
      { team: "B", quarter: "Q1", sales: 15 },
    ]);

    const table = df.pivot_table({
      index: "team",
      columns: "quarter",
      values: "sales",
      aggfunc: "sum",
      fill_value: 0,
    });

    expect(table.sort_values("team").to_records()).toEqual([
      { team: "A", Q1: 10, Q2: 20 },
      { team: "B", Q1: 15, Q2: 0 },
    ]);
  });

  test("pivot_table supports margins for row and grand totals", () => {
    const df = new DataFrame([
      { team: "A", quarter: "Q1", sales: 10 },
      { team: "A", quarter: "Q2", sales: 20 },
      { team: "B", quarter: "Q1", sales: 15 },
    ]);

    const table = df.pivot_table({
      index: "team",
      columns: "quarter",
      values: "sales",
      aggfunc: "sum",
      fill_value: 0,
      margins: true,
    });

    expect(table.to_records()).toEqual([
      { team: "A", Q1: 10, Q2: 20, All: 30 },
      { team: "B", Q1: 15, Q2: 0, All: 15 },
      { team: "All", Q1: 25, Q2: 20, All: 45 },
    ]);
  });

  test("pivot_table sort=false preserves first-seen order", () => {
    const df = new DataFrame([
      { team: "B", quarter: "Q2", sales: 5 },
      { team: "A", quarter: "Q1", sales: 6 },
      { team: "B", quarter: "Q1", sales: 7 },
    ]);

    const table = df.pivot_table({
      index: "team",
      columns: "quarter",
      values: "sales",
      aggfunc: "sum",
      fill_value: 0,
      sort: false,
    });

    expect(table.columns).toEqual(["team", "Q2", "Q1"]);
    expect(table.to_records()).toEqual([
      { team: "B", Q2: 5, Q1: 7 },
      { team: "A", Q2: 0, Q1: 6 },
    ]);
  });

  test("pivot_table dropna=false includes missing column values", () => {
    const df = new DataFrame([
      { team: "A", quarter: null, sales: 10 },
      { team: "A", quarter: "Q1", sales: 5 },
      { team: "B", quarter: null, sales: 2 },
    ]);

    const table = df.pivot_table({
      index: "team",
      columns: "quarter",
      values: "sales",
      aggfunc: "sum",
      fill_value: 0,
      dropna: false,
    });

    expect(table.columns).toEqual(["team", "Q1", "null"]);
    expect(table.sort_values("team").to_records()).toEqual([
      { team: "A", Q1: 5, null: 10 },
      { team: "B", Q1: 0, null: 2 },
    ]);
  });

  test("pivot_table supports multiple value columns", () => {
    const df = new DataFrame([
      { team: "A", quarter: "Q1", sales: 10, units: 2 },
      { team: "A", quarter: "Q2", sales: 20, units: 3 },
    ]);

    const table = df.pivot_table({
      index: "team",
      columns: "quarter",
      values: ["sales", "units"],
      aggfunc: "sum",
      fill_value: 0,
    });

    expect(table.to_records()).toEqual([
      { team: "A", sales_Q1: 10, sales_Q2: 20, units_Q1: 2, units_Q2: 3 },
    ]);
  });

  test("top-level pivot_table helper works", () => {
    const df = new DataFrame([
      { k: "x", c: "m", v: 1 },
      { k: "x", c: "n", v: 2 },
    ]);

    const out = pivot_table(df, {
      index: "k",
      columns: "c",
      values: "v",
      aggfunc: "sum",
      fill_value: 0,
    });

    expect(out.to_records()).toEqual([{ k: "x", m: 1, n: 2 }]);
  });
});

describe("CSV IO", () => {
  test("read_csv_sync infers primitive types and supports index_col", () => {
    const csvPath = join(tempDir, "sample.csv");
    writeFileSync(
      csvPath,
      ["id,city,temp,is_raining", "101,Austin,30,true", "102,Seattle,18,false", ""].join("\n"),
      "utf8"
    );

    const df = read_csv_sync(csvPath, { index_col: "id" });
    expect(df.index).toEqual([101, 102]);
    expect(df.columns).toEqual(["city", "temp", "is_raining"]);
    expect(df.loc(101)).toEqual({ city: "Austin", temp: 30, is_raining: true });
  });

  test("parse_csv strips UTF-8 BOM and matches na_values case-insensitively", () => {
    const csv = "\uFEFFid,value\n1,NA\n2,null\n3,Real";
    const df = parse_csv(csv);
    expect(df.to_records()).toEqual([
      { id: 1, value: null },
      { id: 2, value: null },
      { id: 3, value: "Real" },
    ]);
  });

  test("parse_csv handles quoted separators and multiline values", () => {
    const csv = 'id,text\n1,"hello, world"\n2,"line1\nline2"\n';
    const df = parse_csv(csv);
    expect(df.to_records()).toEqual([
      { id: 1, text: "hello, world" },
      { id: 2, text: "line1\nline2" },
    ]);
  });

  test("parse_csv supports custom separators", () => {
    const df = parse_csv("id;name\n1;Ada\n2;Grace\n", { sep: ";" });
    expect(df.to_records()).toEqual([
      { id: 1, name: "Ada" },
      { id: 2, name: "Grace" },
    ]);
  });

  test("to_csv writes a round-trippable CSV string", () => {
    const df = new DataFrame([
      { id: 1, text: "hello, world" },
      { id: 2, text: 'quoted "value"' },
    ]);

    const csv = df.to_csv();
    expect(csv).toContain('"hello, world"');
    expect(csv).toContain('"quoted ""value"""');
  });
});
