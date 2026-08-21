import { describe, expect, test } from "bun:test";
import { DataFrame } from "../index";

function parityFrame(): DataFrame {
  return new DataFrame([
    { team: "A", player: "x", points: 10, assists: 5 },
    { team: "A", player: "y", points: 20, assists: 2 },
    { team: "A", player: "z", points: 30, assists: null },
    { team: "B", player: "w", points: 40, assists: 8 },
    { team: "B", player: "v", points: 50, assists: 4 },
    { team: "C", player: "u", points: null, assists: 6 },
  ]);
}

describe("groupby agg parity names", () => {
  test("median aggregates per group", () => {
    const result = parityFrame().groupby("team").agg({ points: "median" });
    expect(result.to_records()).toEqual([
      { team: "A", points: 20 },
      { team: "B", points: 45 },
      { team: "C", points: null },
    ]);
  });

  test("std and var use sample ddof=1 and null on insufficient data", () => {
    const grouped = parityFrame().groupby("team");
    const stdRows = grouped.agg({ points: "std" }).to_records();
    const varRows = grouped.agg({ points: "var" }).to_records();

    expect(stdRows[0]!.points).toBeCloseTo(10, 10);
    expect(varRows[0]!.points).toBeCloseTo(100, 10);
    expect(stdRows[1]!.points).toBeCloseTo(Math.sqrt(50), 10);
    expect(stdRows[2]!.points).toBeNull();
    expect(varRows[2]!.points).toBeNull();
  });

  test("first and last skip missing values", () => {
    const result = parityFrame()
      .groupby("team")
      .agg({ points: "first", assists: "last" });
    expect(result.to_records()).toEqual([
      { team: "A", points: 10, assists: null },
      { team: "B", points: 40, assists: 4 },
      { team: "C", points: null, assists: 6 },
    ]);
  });

  test("nunique counts distinct non-missing values", () => {
    const df = new DataFrame([
      { k: "a", v: "p" },
      { k: "a", v: "q" },
      { k: "a", v: "p" },
      { k: "a", v: null },
      { k: "b", v: "r" },
      { k: "b", v: null },
    ]);
    const result = df.groupby("k").agg({ v: "nunique" });
    expect(result.to_records()).toEqual([
      { k: "a", v: 2 },
      { k: "b", v: 1 },
    ]);
  });

  test("all-null column yields null aggregates and zero count/nunique", () => {
    const df = new DataFrame([
      { g: "x", v: null },
      { g: "x", v: undefined },
      { g: "y", v: null },
    ]);
    const grouped = df.groupby("g");
    for (const name of [
      "sum",
      "mean",
      "min",
      "max",
      "median",
      "std",
      "var",
      "first",
      "last",
    ] as const) {
      const rows = grouped.agg({ v: name }).to_records();
      expect(rows[0]!.v).toBeNull();
      expect(rows[1]!.v).toBeNull();
    }
    expect(grouped.agg({ v: "count" }).to_records()[0]!.v).toBe(0);
    expect(grouped.agg({ v: "nunique" }).to_records()[0]!.v).toBe(0);
  });

  test("fast path results unchanged for basic names, mixed specs take general path", () => {
    const df = parityFrame();
    const fastOnly = df.groupby("team").agg({ points: "sum", assists: "count" });
    expect(fastOnly.to_records()).toEqual([
      { team: "A", points: 60, assists: 2 },
      { team: "B", points: 90, assists: 2 },
      { team: "C", points: null, assists: 1 },
    ]);

    const mixed = df
      .groupby("team")
      .agg({ points: "sum", assists: "count", player: "first" });
    expect(mixed.to_records()).toEqual([
      { team: "A", points: 60, assists: 2, player: "x" },
      { team: "B", points: 90, assists: 2, player: "w" },
      { team: "C", points: null, assists: 1, player: "u" },
    ]);
  });

  test("multi-key groupby supports new agg names", () => {
    const df = new DataFrame([
      { region: "east", tier: "gold", amount: 10, rep: "ann" },
      { region: "east", tier: "gold", amount: 30, rep: "bob" },
      { region: "east", tier: "silver", amount: 20, rep: "ann" },
      { region: "west", tier: "gold", amount: 40, rep: "cal" },
    ]);
    const result = df.groupby(["region", "tier"]).agg({
      amount: "median",
      rep: "nunique",
    });
    expect(result.to_records()).toEqual([
      { region: "east", tier: "gold", amount: 20, rep: 2 },
      { region: "east", tier: "silver", amount: 20, rep: 1 },
      { region: "west", tier: "gold", amount: 40, rep: 1 },
    ]);
  });

  test("as_index=true still applies to agg with new names", () => {
    const result = parityFrame()
      .groupby("team", { as_index: true })
      .agg({ points: "median" });
    expect(result.columns).toEqual(["points"]);
    expect(result.index).toEqual(["A", "B", "C"]);
    expect(result.to_records()).toEqual([{ points: 20 }, { points: 45 }, { points: null }]);
  });

  test("custom functions still work alongside named plans", () => {
    const result = parityFrame()
      .groupby("team")
      .agg({ points: (values) => values.filter((v) => v !== null).length });
    expect(result.to_records()).toEqual([
      { team: "A", points: 3 },
      { team: "B", points: 2 },
      { team: "C", points: 0 },
    ]);
  });
});

describe("groupby transform", () => {
  test("dict mode broadcasts aggregates to source rows and preserves index", () => {
    const df = parityFrame();
    const result = df.groupby("team").transform({ points: "mean" });

    expect(result.columns).toEqual(["points"]);
    expect(result.index).toEqual(df.index);
    expect(result.to_records()).toEqual([
      { points: 20 },
      { points: 20 },
      { points: 20 },
      { points: 45 },
      { points: 45 },
      { points: null },
    ]);
  });

  test("dict mode outputs only spec keys and supports custom functions", () => {
    const df = parityFrame();
    const result = df.groupby("team").transform({
      points: (values) => {
        const numbers = values.filter((v): v is number => typeof v === "number");
        return numbers.length > 0 ? Math.max(...numbers) : null;
      },
    });
    expect(result.columns).toEqual(["points"]);
    expect(result.to_records()).toEqual([
      { points: 30 },
      { points: 30 },
      { points: 30 },
      { points: 50 },
      { points: 50 },
      { points: null },
    ]);
  });

  test("rows dropped by dropna get null in every transformed column", () => {
    const df = new DataFrame([
      { g: "a", v: 1 },
      { g: null, v: 2 },
      { g: "a", v: 3 },
      { g: "b", v: 7 },
    ]);
    const result = df.groupby("g").transform({ v: "mean" });
    expect(result.columns).toEqual(["v"]);
    expect(result.index).toEqual(df.index);
    expect(result.to_records()).toEqual([{ v: 2 }, { v: null }, { v: 2 }, { v: 7 }]);
  });

  test("function mode scalar broadcast per group across all non-key columns", () => {
    const df = parityFrame();
    const result = df.groupby("team").transform(
      (column) => column.values.filter((v) => v !== null).length
    );
    expect(result.columns).toEqual(["player", "points", "assists"]);
    expect(result.to_records()).toEqual([
      { player: 3, points: 3, assists: 3 },
      { player: 3, points: 3, assists: 3 },
      { player: 3, points: 3, assists: 3 },
      { player: 2, points: 2, assists: 2 },
      { player: 2, points: 2, assists: 2 },
      { player: 1, points: 1, assists: 1 },
    ]);
  });

  test("function mode array result maps back to group rows", () => {
    const df = parityFrame();
    const result = df.groupby("team").transform((column) =>
      column.values.map((value) => (typeof value === "number" ? value * 2 : value))
    );
    expect(result.to_records()[0]).toEqual({ player: "x", points: 20, assists: 10 });
    expect(result.to_records()[3]).toEqual({ player: "w", points: 80, assists: 16 });
  });

  test("function mode wrong-length array throws", () => {
    const df = parityFrame();
    expect(() => df.groupby("team").transform(() => [1, 2])).toThrow(
      "transform function must return a value per group row."
    );
  });

  test("function mode receives sub-series, column name and position", () => {
    const df = parityFrame();
    const seen: Array<[string, number]> = [];
    df.groupby("team").transform((column, name, position) => {
      if (position === 0) {
        seen.push([name, column.length]);
      }
      return column.length;
    });
    expect(seen).toEqual([
      ["player", 3],
      ["player", 2],
      ["player", 1],
    ]);
  });

  test("multi-key transform aligns to source order regardless of sort option", () => {
    const df = new DataFrame([
      { a: "b", b: 2, v: 20 },
      { a: "a", b: 1, v: 10 },
      { a: "b", b: 2, v: 30 },
      { a: "a", b: 1, v: 50 },
    ]);
    const sorted = df.groupby(["a", "b"], { sort: true }).transform({ v: "max" });
    expect(sorted.to_records()).toEqual([{ v: 50 }, { v: 10 }, { v: 30 }, { v: 20 }]);

    const unsorted = df.groupby(["a", "b"], { sort: false }).transform({ v: "max" });
    expect(unsorted.to_records()).toEqual([{ v: 50 }, { v: 10 }, { v: 30 }, { v: 20 }]);
  });
});

describe("groupby convenience methods", () => {
  test("min/max default to numeric columns", () => {
    const grouped = parityFrame().groupby("team");
    expect(grouped.min().to_records()).toEqual([
      { team: "A", points: 10, assists: 2 },
      { team: "B", points: 40, assists: 4 },
      { team: "C", points: null, assists: 6 },
    ]);
    expect(grouped.max().to_records()).toEqual([
      { team: "A", points: 30, assists: 5 },
      { team: "B", points: 50, assists: 8 },
      { team: "C", points: null, assists: 6 },
    ]);
  });

  test("median/std/var default to numeric columns", () => {
    const grouped = parityFrame().groupby("team");
    expect(grouped.median().to_records()).toEqual([
      { team: "A", points: 20, assists: 3.5 },
      { team: "B", points: 45, assists: 6 },
      { team: "C", points: null, assists: 6 },
    ]);
    const stdRows = grouped.std().to_records();
    expect(stdRows[0]!.points).toBeCloseTo(10, 10);
    expect(stdRows[0]!.assists).toBeCloseTo(Math.sqrt(4.5), 10);
    const varRows = grouped.var().to_records();
    expect(varRows[0]!.points).toBeCloseTo(100, 10);
  });

  test("first/last/nunique default to all non-key columns", () => {
    const grouped = parityFrame().groupby("team");
    expect(grouped.first().to_records()).toEqual([
      { team: "A", player: "x", points: 10, assists: 5 },
      { team: "B", player: "w", points: 40, assists: 8 },
      { team: "C", player: "u", points: null, assists: 6 },
    ]);
    expect(grouped.last().to_records()).toEqual([
      { team: "A", player: "z", points: 30, assists: null },
      { team: "B", player: "v", points: 50, assists: 4 },
      { team: "C", player: "u", points: null, assists: 6 },
    ]);
    expect(grouped.nunique().to_records()).toEqual([
      { team: "A", player: 3, points: 3, assists: 2 },
      { team: "B", player: 2, points: 2, assists: 2 },
      { team: "C", player: 1, points: 0, assists: 1 },
    ]);
  });

  test("convenience methods accept explicit column lists", () => {
    const grouped = parityFrame().groupby("team");
    expect(grouped.median(["points"]).columns).toEqual(["team", "points"]);
    expect(grouped.nunique(["player"]).to_records()).toEqual([
      { team: "A", player: 3 },
      { team: "B", player: 2 },
      { team: "C", player: 1 },
    ]);
  });
});
