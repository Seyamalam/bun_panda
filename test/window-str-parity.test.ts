import { describe, expect, test } from "bun:test";
import { DataFrame } from "../src/index";
import { Series } from "../src/series";

describe("Series rolling windows", () => {
  const s = new Series<number>([1, 2, 3, 4, 5]);

  test("rolling mean with window 3", () => {
    expect(s.rolling(3).mean()).toEqual([null, null, 2, 3, 4]);
  });

  test("rolling sum/min/max", () => {
    expect(s.rolling(2).sum()).toEqual([null, 3, 5, 7, 9]);
    expect(s.rolling(2).max()).toEqual([null, 2, 3, 4, 5]);
    expect(s.rolling(2).min()).toEqual([null, 1, 2, 3, 4]);
  });

  test("min_periods lowers the bar", () => {
    expect(s.rolling(3, 1).mean()).toEqual([1, 1.5, 2, 3, 4]);
  });

  test("count counts non-null entries in window", () => {
    const sparse = new Series<number | null>([1, null, 3]);
    // count() reports the number of values in each qualifying window;
    // windows below min_periods yield 0.
    expect(sparse.rolling(2).count()).toEqual([0, 0, 0]);
    expect(sparse.rolling(2, 1).count()).toEqual([1, 1, 1]);
  });

  test("expanding mean", () => {
    expect(s.expanding().mean()).toEqual([1, 1.5, 2, 2.5, 3]);
  });

  test("rolling std", () => {
    const r = s.rolling(2).std();
    // sample std of [1,2] = 0.7071...
    expect(Math.abs((r[1] as number) - Math.SQRT1_2)).toBeLessThan(1e-9);
  });

  test("median window", () => {
    expect(new Series<number>([5, 1, 3]).rolling(3).median()).toEqual([
      null,
      null,
      3,
    ]);
  });
});

describe("DataFrame rolling/expanding", () => {
  const df = new DataFrame([{ x: 1, y: 10 }, { x: 2, y: 20 }, { x: 3, y: 30 }]);

  test("rolling mean drops non-numeric columns", () => {
    const tagged = new DataFrame([
      { tag: "a", v: 1 },
      { tag: "b", v: 2 },
      { tag: "c", v: 3 },
    ]);
    const out = tagged.rolling(2).mean();
    expect(out.columns).toEqual(["v"]);
    expect(out.to_records()).toEqual([
      { v: null },
      { v: 1.5 },
      { v: 2.5 },
    ]);
  });

  test("multi-column rolling keeps columns independent", () => {
    expect(df.rolling(2).sum().to_records()).toEqual([
      { x: null, y: null },
      { x: 3, y: 30 },
      { x: 5, y: 50 },
    ]);
  });

  test("expanding max", () => {
    expect(df.expanding().max().to_records()).toEqual([
      { x: 1, y: 10 },
      { x: 2, y: 20 },
      { x: 3, y: 30 },
    ]);
  });

  test("custom aggregate", () => {
    const out = df.rolling(2).aggregate((values) => values[values.length - 1]! - values[0]!);
    expect(out.to_records()[2]).toEqual({ x: 1, y: 10 });
  });
});

describe(".str expansion", () => {
  const s = new Series<string | null>(["Hello World", "abc123", null]);

  test("character class predicates", () => {
    const mixed = new Series<string | null>(["abc", "123", "", null]);
    expect(mixed.str.isalpha()).toEqual([true, false, false, null]);
    expect(mixed.str.isdigit()).toEqual([false, true, false, null]);
    expect(new Series<string>(["  "]).str.isspace()).toEqual([true]);
    expect(new Series<string>(["abc123"]).str.isalnum()).toEqual([true]);
    expect(new Series<string>(["abc"]).str.islower()).toEqual([true]);
    expect(new Series<string>(["ABC"]).str.isupper()).toEqual([true]);
  });

  test("casefold/swapcase/center/ljust/rjust", () => {
    expect(new Series<string>(["STRASSE"]).str.casefold().map((v) => v as string)).toEqual(["strasse"]);
    expect(new Series<string>(["aBc"]).str.swapcase().map((v) => v as string)).toEqual(["AbC"]);
    expect(new Series<string>(["ab"]).str.center(5, "*").map((v) => v as string)).toEqual(["*ab**"]);
    expect(new Series<string>(["ab"]).str.ljust(4, ".").map((v) => v as string)).toEqual(["ab.."]);
    expect(new Series<string>(["ab"]).str.rjust(4, ".").map((v) => v as string)).toEqual(["..ab"]);
  });

  test("removeprefix/removesuffix/repeat", () => {
    expect(new Series<string>(["unhappy"]).str.removeprefix("un").map((v) => v as string)).toEqual(["happy"]);
    expect(new Series<string>(["file.txt"]).str.removesuffix(".txt").map((v) => v as string)).toEqual(["file"]);
    expect(new Series<string>(["ab"]).str.repeat(3).map((v) => v as string)).toEqual(["ababab"]);
  });

  test("findall returns match arrays", () => {
    expect(new Series<string>(["a1b22"]).str.findall(/\d+/)).toEqual([["1", "22"]]);
    expect(s.str.findall(/o/)).toEqual([["o", "o"], [], []]);
  });

  test("extract pulls capture groups", () => {
    const dates = new Series<string>(["2026-01-15", "nope"]);
    expect(dates.str.extract(/(\d+)-(\d+)-(\d+)/)).toEqual([
      ["2026", "01", "15"],
      [],
    ]);
    expect(dates.str.extract(/(\d{4})/)).toEqual(["2026", null]);
  });

  test("partition/rpartition split on separators", () => {
    expect(new Series<string>(["a=b=c"]).str.partition("=")).toEqual([["a", "=", "b=c"]]);
    expect(new Series<string>(["a=b=c"]).str.rpartition("=")).toEqual([["a=b", "=", "c"]]);
  });

  test("rsplit limits from the right", () => {
    expect(new Series<string>(["a.b.c"]).str.rsplit(".", 1)).toEqual([["a.b", "c"]]);
  });

  test("nulls propagate through new methods", () => {
    expect(s.str.isalpha()).toEqual([false, false, null]);
    expect(s.str.repeat(2)).toEqual(["Hello WorldHello World", "abc123abc123", null]);
  });
});
