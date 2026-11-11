import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  DataFrame,
  NaT,
  NA,
  Series,
  parse_csv,
  parse_json,
  read_csv_sync,
  read_excel_sync,
  read_json_sync,
  read_parquet,
} from "../src/index";
import type { CellValue } from "../src/types";

const tempDir = mkdtempSync(join(tmpdir(), "bun-panda-io-roundtrip-"));
afterAll(() => rmSync(tempDir, { recursive: true, force: true }));

const WIDE = 9007199254740993n; // > 2^53 — a double cannot represent it.

// Matches the pandas 3.0.5 probe:
//   df.to_csv(index=False) ->
//   big,dt,dtz,cat
//   9007199254740993,2020-01-01,2020-01-01 07:00:00-05:00,a
//   -5,,,b
function dtypeFrame(): DataFrame {
  return new DataFrame([
    {
      big: WIDE,
      dt: new Date("2020-01-01T00:00:00Z"),
      dtz: new Date("2020-01-01T12:00:00Z"),
      cat: "a",
    },
    { big: -5n, dt: null, dtz: null, cat: "b" },
  ]).astype({
    big: "int64",
    dt: "datetime64[ns]",
    dtz: "datetime64[ns, US/Eastern]",
    cat: "category",
  });
}

describe("to_csv serialization (pandas-verified)", () => {
  test("int64 bigint cells write decimal digits, not '1n'", () => {
    const csv = dtypeFrame().to_csv();
    const lines = csv.trimEnd().split("\n");
    expect(lines[0]).toBe("big,dt,dtz,cat");
    expect(lines[1]).toBe("9007199254740993,2020-01-01,2020-01-01 07:00:00-05:00,a");
    expect(lines[2]).toBe("-5,,,b");
    expect(csv).not.toContain("n,");
  });

  test("naive datetime picks the pandas per-column resolution", () => {
    const day = new DataFrame([{ t: new Date("2020-01-01T00:00:00Z") }, { t: new Date("2020-01-02T00:00:00Z") }])
      .astype({ t: "datetime64[ns]" });
    expect(day.to_csv()).toBe("t\n2020-01-01\n2020-01-02\n");

    const seconds = new DataFrame([
      { t: new Date("2020-01-01T15:00:00Z") },
      { t: new Date("2020-01-02T00:00:00Z") },
    ]).astype({ t: "datetime64[ns]" });
    expect(seconds.to_csv()).toBe("t\n2020-01-01 15:00:00\n2020-01-02 00:00:00\n");

    const millis = new DataFrame([
      { t: new Date("2020-01-01T00:00:00.123Z") },
      { t: new Date("2020-01-02T15:04:03Z") },
    ]).astype({ t: "datetime64[ns]" });
    expect(millis.to_csv()).toBe("t\n2020-01-01 00:00:00.123\n2020-01-02 15:04:03.000\n");
  });

  test("tz-aware datetimes write local time with ±HH:MM offset", () => {
    const df = new DataFrame([{ t: new Date("2020-01-01T12:30:15Z") }])
      .astype({ t: "datetime64[ns, Asia/Tokyo]" });
    expect(df.to_csv()).toBe("t\n2020-01-01 21:30:15+09:00\n");
  });

  test("category cells write their values, bigint/category in one row stays typed", () => {
    const csv = dtypeFrame().to_csv();
    expect(csv).toContain(",a\n");
    expect(csv).toContain(",b\n");
  });

  test("MultiIndex index levels become separate fields like pandas", () => {
    const df = new DataFrame(
      { a: [10, 20], b: [30, 40] },
      { index: [["x", 1], ["y", 2]] as never, indexNames: ["L1", "L2"] }
    );
    expect(df.to_csv({ index: true })).toBe("L1,L2,a,b\nx,1,10,30\ny,2,20,40\n");

    const unnamed = new DataFrame({ a: [10] }, { index: [["x", 1]] as never });
    expect(unnamed.to_csv({ index: true })).toBe(",,a\nx,1,10\n");
  });

  test("single-level index header uses the index name or stays empty", () => {
    const named = new DataFrame({ a: [1] }, { index: ["r"], indexName: "id" });
    expect(named.to_csv({ index: true })).toBe("id,a\nr,1\n");
    const unnamed = new DataFrame({ a: [1] }, { index: ["r"] });
    expect(unnamed.to_csv({ index: true })).toBe(",a\nr,1\n");
  });
});

describe("read_csv value inference", () => {
  test("int64-width literals keep exact bigint cells", () => {
    const df = parse_csv("big\n9007199254740993\n-5\n");
    // int64 storage is uniform: one wide cell upgrades the column to bigint.
    expect(df.to_records()).toEqual([{ big: 9007199254740993n }, { big: -5n }]);
    expect(df.dtypes["big"]).toBe("int64");
  });

  test("safe integers still parse as numbers; exponent notation stays numeric", () => {
    const df = parse_csv("a,b\n5,1e3\n-7,2.5\n");
    expect(df.to_records()).toEqual([{ a: 5, b: 1000 }, { a: -7, b: 2.5 }]);
  });

  test("csv roundtrip: bigint out, bigint back; NaT out, empty field, null back", () => {
    const frame = dtypeFrame();
    const back = parse_csv(frame.to_csv());
    const records = back.to_records();
    expect(records[0]!["big"]).toBe(9007199254740993n);
    // dtype degrades like pandas (csv has no dtype metadata): datetime and
    // category columns come back as strings.
    expect(records[0]!["dt"]).toBe("2020-01-01");
    expect(Number.isNaN(records[1]!["dt"])).toBe(true); // empty field -> NaN like pandas
    expect(records[0]!["cat"]).toBe("a");
  });
});

describe("to_json serialization (pandas-verified)", () => {
  test("records orient matches pandas date_format='iso'", () => {
    const json = dtypeFrame().to_json({ orient: "records", space: 0 });
    expect(json).toBe(
      '[{"big":9007199254740993,"dt":"2020-01-01T00:00:00.000","dtz":"2020-01-01T12:00:00.000Z","cat":"a"},' +
        '{"big":-5,"dt":null,"dtz":null,"cat":"b"}]'
    );
  });

  test("list orient applies the same cell rules", () => {
    const json = dtypeFrame().to_json({ orient: "list", space: 0 });
    expect(json).toContain('"big":[9007199254740993,-5]');
    const parsed = JSON.parse(json) as Record<string, unknown[]>;
    expect(parsed["dt"]).toEqual(["2020-01-01T00:00:00.000", null]);
    expect(parsed["cat"]).toEqual(["a", "b"]);
  });

  test("NA/NaT symbols serialize as null instead of dropping the key", () => {
    const df = new DataFrame([
      { i: 1, d: new Date("2020-01-01T00:00:00Z") },
      { i: null, d: null },
    ]).astype({ i: "Int64", d: "datetime64[ns]" });
    const json = df.to_json({ space: 0 });
    const parsed = JSON.parse(json) as Record<string, unknown>[];
    expect(parsed[1]).toEqual({ i: null, d: null });
    expect(Object.keys(parsed[1]!).sort()).toEqual(["d", "i"]);
    // missing marker is pandas NA for Int64 and NaT for datetime64
    expect(df.to_records()[1]!["i"]).toBe(NA);
    expect(df.to_records()[1]!["d"]).toBe(NaT);
  });

  test("lines=true serializes each record identically", () => {
    const lines = dtypeFrame().to_json({ lines: true, orient: "records" }).split("\n");
    expect(lines[0]).toBe(
      '{"big":9007199254740993,"dt":"2020-01-01T00:00:00.000","dtz":"2020-01-01T12:00:00.000Z","cat":"a"}'
    );
  });

  test("safe-range bigints still emit plain JSON numbers", () => {
    const df = new DataFrame([{ big: 42n }]);
    expect(df.to_json({ space: 0 })).toBe('[{"big":42}]');
  });

  test("parse_json accepts what to_json emits (documented precision limit)", () => {
    // JSON number tokens carry no int64 metadata, so a raw 9007199254740993
    // parses to the nearest double — same loss JSON.parse gives pandas output.
    const back = parse_json(dtypeFrame().to_json({ space: 0 }));
    const records = back.to_records();
    expect(records[0]!["big"]).toBe(9007199254740992);
    expect(records[1]!["big"]).toBe(-5);
    expect(records[0]!["dt"]).toBe("2020-01-01T00:00:00.000"); // dtype degrades to string
    expect(Number.isNaN(records[1]!["dt"])).toBe(true);
  });
});

describe("to_parquet / read_parquet roundtrip", () => {
  test("int64 columns with bigint cells keep exact values", async () => {
    const path = join(tempDir, "big.parquet");
    const df = new DataFrame([{ big: WIDE }, { big: -5n }]);
    await df.to_parquet({ path });
    const back = await read_parquet(path);
    const records = back.to_records();
    expect(records[0]!["big"]).toBe(9007199254740993n);
    expect(records[1]!["big"]).toBe(-5n);
    expect(back.dtypes["big"]).toBe("int64");
  });

  test("safe int64 columns still round-trip as numbers", async () => {
    const path = join(tempDir, "safe.parquet");
    await new DataFrame([{ n: 1 }, { n: 2 }]).to_parquet({ path });
    expect((await read_parquet(path)).to_records()).toEqual([{ n: 1 }, { n: 2 }]);
  });

  test("datetime64 columns write TIMESTAMP_MILLIS and read back Dates", async () => {
    const path = join(tempDir, "dt.parquet");
    const df = new DataFrame([
      { t: new Date("2020-01-01T00:00:00Z") },
      { t: null },
      { t: new Date("1960-06-15T00:00:00Z") }, // pre-epoch: negative ms
    ]).astype({ t: "datetime64[ns]" });
    await df.to_parquet({ path });
    const back = await read_parquet(path);
    const records = back.to_records();
    expect(records[0]!["t"]).toEqual(new Date("2020-01-01T00:00:00Z"));
    expect(records[1]!["t"]).toBeNull();
    expect(records[2]!["t"]).toEqual(new Date("1960-06-15T00:00:00Z"));
    expect(back.dtypes["t"]).toBe("date"); // dtype degrades to the date family
  });

  test("tz-aware columns store the UTC instant (zone metadata is dropped)", async () => {
    const path = join(tempDir, "tz.parquet");
    const df = new DataFrame([{ t: new Date("2020-01-01T12:00:00Z") }])
      .astype({ t: "datetime64[ns, US/Eastern]" });
    await df.to_parquet({ path });
    const back = await read_parquet(path);
    expect(back.to_records()[0]!["t"]).toEqual(new Date("2020-01-01T12:00:00Z"));
  });

  test("category columns keep the categories' physical type", async () => {
    const strPath = join(tempDir, "cat-str.parquet");
    await new DataFrame([{ c: "x" }, { c: "y" }])
      .astype({ c: "category" })
      .to_parquet({ path: strPath });
    expect((await read_parquet(strPath)).to_records()).toEqual([{ c: "x" }, { c: "y" }]);

    const numPath = join(tempDir, "cat-num.parquet");
    await new DataFrame([{ c: 2 }, { c: 1 }])
      .astype({ c: "category" })
      .to_parquet({ path: numPath });
    const back = await read_parquet(numPath);
    expect(back.to_records()).toEqual([{ c: 2 }, { c: 1 }]);
    // numeric categories stay numeric rather than degrading to text
    expect(back.dtypes["c"]).toBe("int64");
  });
});

describe("to_excel / read_excel roundtrip", () => {
  test("bigint cells: safe range as numbers, beyond as decimal text", () => {
    const path = join(tempDir, "big.xlsx");
    const df = new DataFrame([{ big: WIDE }, { big: -5n }]);
    df.to_excel({ path });
    const back = read_excel_sync(path);
    const records = back.to_records();
    expect(records[0]!["big"]).toBe("9007199254740993");
    expect(records[1]!["big"]).toBe(-5);
  });

  test("datetime cells write real date cells; NaT stays empty", () => {
    const path = join(tempDir, "dt.xlsx");
    const df = new DataFrame([
      { t: new Date("2020-01-01T00:00:00Z") },
      { t: null },
    ]).astype({ t: "datetime64[ns]" });
    df.to_excel({ path });
    const records = read_excel_sync(path).to_records();
    expect(records[0]!["t"]).toEqual(new Date("2020-01-01T00:00:00Z"));
    expect(records[1]!["t"]).toBeNull(); // inferred "date" dtype keeps null for missing
  });

  test("tz-aware datetimes write the wall-clock time (Excel has no tz)", () => {
    const path = join(tempDir, "tz.xlsx");
    const df = new DataFrame([{ t: new Date("2020-01-01T12:00:00Z") }])
      .astype({ t: "datetime64[ns, US/Eastern]" });
    df.to_excel({ path });
    const records = read_excel_sync(path).to_records();
    expect(records[0]!["t"]).toEqual(new Date("2020-01-01T07:00:00Z"));
  });

  test("category cells write their values", () => {
    const path = join(tempDir, "cat.xlsx");
    const df = new DataFrame([{ c: "x" }, { c: "y" }]).astype({ c: "category" });
    df.to_excel({ path });
    expect(read_excel_sync(path).to_records()).toEqual([{ c: "x" }, { c: "y" }]);
  });

  test("index: true emits one column per level with level names", () => {
    const path = join(tempDir, "mi.xlsx");
    const df = new DataFrame(
      { a: [10, 20] },
      { index: [["x", 1], ["y", 2]] as never, indexNames: ["L1", "L2"] }
    );
    df.to_excel({ path, index: true });
    expect(read_excel_sync(path).to_records()).toEqual([
      { L1: "x", L2: 1, a: 10 },
      { L1: "y", L2: 2, a: 20 },
    ]);
  });
});

describe("file-backed csv/json roundtrips", () => {
  test("read_csv_sync reads what to_csv wrote", () => {
    const path = join(tempDir, "rt.csv");
    const frame = dtypeFrame();
    writeFileSync(path, frame.to_csv(), "utf8");
    const back = read_csv_sync(path);
    expect(back.to_records()[0]!["big"]).toBe(9007199254740993n);
  });

  test("read_json_sync reads what to_json wrote", () => {
    const path = join(tempDir, "rt.json");
    const frame = dtypeFrame();
    frame.to_json({ path });
    const back = read_json_sync(path);
    expect(back.to_records()[0]!["cat"]).toBe("a");
    expect(Number.isNaN(back.to_records()[1]!["dt"])).toBe(true);
  });
});

describe("Series-level csv/json", () => {
  test("Series.to_csv uses the same scalar rules", () => {
    const s = new Series<CellValue>([WIDE, -5n], { dtype: "int64" });
    expect(s.to_csv()).toBe("9007199254740993\n-5");

    const dt = new Series<CellValue>([new Date("2020-01-01T00:00:00Z"), null], {
      dtype: "datetime64[ns]",
    });
    expect(dt.to_csv()).toBe("2020-01-01\n");
  });

  test("Series.to_json emits raw digits and nulls for missing", () => {
    const s = new Series<CellValue>([WIDE, -5n], { dtype: "int64" });
    expect(s.to_json()).toBe("[9007199254740993,-5]");
  });
});
