import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  DataFrame,
  parse_json,
  parse_table,
  parse_tsv,
  read_json_sync,
  read_table_sync,
  read_tsv_sync,
} from "../index";

const tempDir = mkdtempSync(join(tmpdir(), "bun-panda-io-compat-"));

afterAll(() => {
  rmSync(tempDir, { recursive: true, force: true });
});

describe("table and tsv io", () => {
  test("parse_table uses tab separator by default", () => {
    const text = "id\tcity\tvalue\n1\tAustin\t10\n2\tSeattle\t20\n";
    const df = parse_table(text);
    expect(df.to_records()).toEqual([
      { id: 1, city: "Austin", value: 10 },
      { id: 2, city: "Seattle", value: 20 },
    ]);
  });

  test("read_table_sync supports index_col", () => {
    const path = join(tempDir, "sample.tsv");
    writeFileSync(path, "id\tcity\n101\tAustin\n102\tSeattle\n", "utf8");

    const df = read_table_sync(path, { index_col: "id" });
    expect(df.index).toEqual([101, 102]);
    expect(df.columns).toEqual(["city"]);
    expect(df.loc(101)).toEqual({ city: "Austin" });
  });

  test("tsv aliases match table readers", () => {
    const path = join(tempDir, "alias.tsv");
    writeFileSync(path, "id\tcity\n1\tAustin\n", "utf8");

    expect(parse_tsv("id\tcity\n1\tAustin\n").to_records()).toEqual([
      { id: 1, city: "Austin" },
    ]);
    expect(read_tsv_sync(path).to_records()).toEqual([{ id: 1, city: "Austin" }]);
  });
});

describe("json lines compatibility", () => {
  test("parse_json supports lines=true", () => {
    const text = ['{"id":1,"city":"Austin"}', '{"id":2,"city":"Seattle"}'].join("\n");
    const df = parse_json(text, { lines: true });
    expect(df.to_records()).toEqual([
      { id: 1, city: "Austin" },
      { id: 2, city: "Seattle" },
    ]);
  });

  test("read_json_sync supports lines=true with index_col", () => {
    const path = join(tempDir, "lines.jsonl");
    writeFileSync(path, ['{"id":7,"team":"A"}', '{"id":8,"team":"B"}'].join("\n"), "utf8");

    const df = read_json_sync(path, { lines: true, index_col: "id" });
    expect(df.index).toEqual([7, 8]);
    expect(df.columns).toEqual(["team"]);
  });

  test("parse_json lines=true rejects non-records orient", () => {
    const text = '{"id":1}\n{"id":2}';
    expect(() => parse_json(text, { lines: true, orient: "list" })).toThrow(
      "JSON lines format only supports orient='records'."
    );
  });

  test("to_json supports lines=true and file output", () => {
    const path = join(tempDir, "out.jsonl");
    const df = new DataFrame([
      { id: 1, city: "Austin" },
      { id: 2, city: "Seattle" },
    ]);

    const lines = df.to_json({ lines: true, orient: "records", path });
    const fileText = readFileSync(path, "utf8").trimEnd();
    expect(lines).toBe(fileText);
    expect(lines.split("\n")).toHaveLength(2);
  });
});
