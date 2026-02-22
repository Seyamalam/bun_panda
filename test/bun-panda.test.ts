import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { DataFrame, concat, merge, read_csv_sync } from "../index";

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

  test("merge and concat preserve expected columns", () => {
    const left = new DataFrame([
      { id: 1, name: "Ada" },
      { id: 2, name: "Grace" },
    ]);
    const right = new DataFrame([
      { id: 1, score: 98 },
      { id: 2, score: 95 },
    ]);

    const joined = merge(left, right, { on: "id" });
    expect(joined.columns).toEqual(["id", "name", "score"]);
    expect(joined.shape).toEqual([2, 3]);

    const extra = new DataFrame([{ id: 3, name: "Linus", score: 99 }]);
    const stacked = concat([joined, extra], { axis: 0, ignore_index: true });
    expect(stacked.shape).toEqual([3, 3]);
    expect(stacked.iloc(2)).toEqual({ id: 3, name: "Linus", score: 99 });
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

  test("to_csv writes a round-trippable CSV string", () => {
    const df = new DataFrame([
      { id: 1, text: "hello, world" },
      { id: 2, text: "quoted \"value\"" },
    ]);

    const csv = df.to_csv();
    expect(csv).toContain('"hello, world"');
    expect(csv).toContain('"quoted ""value"""');
  });
});
