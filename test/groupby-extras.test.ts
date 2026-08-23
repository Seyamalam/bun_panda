import { describe, expect, test } from "bun:test";
import { DataFrame } from "../src/index";

describe("GroupBy extras", () => {
  const df = new DataFrame([
    { g: "a", x: 1 },
    { g: "b", x: 2 },
    { g: "a", x: 3 },
    { g: "b", x: 4 },
  ]);

  test("cumsum within groups aligned to source rows", () => {
    expect(df.groupby("g").cumsum(["x"]).to_records()).toEqual([
      { g: "a", x: 1 },
      { g: "b", x: 2 },
      { g: "a", x: 4 },
      { g: "b", x: 6 },
    ]);
  });

  test("shift within groups", () => {
    expect(df.groupby("g").shift(1).to_records()).toEqual([
      { g: "a", x: null },
      { g: "b", x: null },
      { g: "a", x: 1 },
      { g: "b", x: 2 },
    ]);
  });

  test("diff within groups", () => {
    expect(df.groupby("g").diff().to_records()).toEqual([
      { g: "a", x: null },
      { g: "b", x: null },
      { g: "a", x: 2 },
      { g: "b", x: 2 },
    ]);
  });

  test("quantile per group", () => {
    const out = df.groupby("g").quantile(0.5);
    const records = out.sort_values("g").to_records();
    expect(records).toEqual([
      { g: "a", x: 2 },
      { g: "b", x: 3 },
    ]);
  });

  test("value_counts per group", () => {
    const tagged = new DataFrame([
      { g: "a", f: "p" },
      { g: "a", f: "p" },
      { g: "a", f: "q" },
      { g: "b", f: "r" },
    ]);
    const counts = tagged.groupby("g").value_counts("f");
    expect(counts.to_records()).toEqual([
      { g: "a", f: "p", count: 2 },
      { g: "a", f: "q", count: 1 },
      { g: "b", f: "r", count: 1 },
    ]);
  });

  test("describe produces stat rows per group", () => {
    const described = df.groupby("g").describe();
    expect(described.columns).toEqual(["g", "stat", "x"]);
    const statsA = described
      .to_records()
      .filter((row) => row.g === "a")
      .map((row) => row.stat);
    expect(statsA).toEqual(["count", "mean", "std", "min", "25%", "50%", "75%", "max"]);
  });

  test("apply concatenates transformed groups", () => {
    const doubled = df.groupby("g").apply((group) => {
      return new DataFrame(
        group.to_records().map((row) => ({ ...row, x: (row.x as number) * 2 }))
      );
    });
    expect(doubled.to_records().map((r) => r.x)).toEqual([2, 6, 4, 8]);
  });
});
