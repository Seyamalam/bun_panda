import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  DataFrame,
  read_excel,
  read_excel_sync,
  read_parquet,
  to_excel,
  to_parquet,
} from "../index";

const tempDir = mkdtempSync(join(tmpdir(), "bun-panda-filetypes-"));

afterAll(() => {
  rmSync(tempDir, { recursive: true, force: true });
});

describe("parquet io", () => {
  test("DataFrame.to_parquet + read_parquet round-trip", async () => {
    const path = join(tempDir, "sample.parquet");
    const df = new DataFrame([
      { id: 1, city: "Austin", score: 10.5, active: true },
      { id: 2, city: "Seattle", score: 8.25, active: false },
    ]);

    await df.to_parquet({ path });
    const out = await read_parquet(path);

    expect(out.sort_values("id").to_records()).toEqual([
      { id: 1, city: "Austin", score: 10.5, active: true },
      { id: 2, city: "Seattle", score: 8.25, active: false },
    ]);
  });

  test("top-level to_parquet supports index_col on read", async () => {
    const path = join(tempDir, "with-index.parquet");
    const df = new DataFrame([
      { id: 101, team: "A" },
      { id: 102, team: "B" },
    ]);

    await to_parquet(df, { path });
    const out = await read_parquet(path, { index_col: "id" });
    expect(out.index).toEqual([101, 102]);
    expect(out.columns).toEqual(["team"]);
  });
});

describe("excel io", () => {
  test("DataFrame.to_excel + read_excel_sync round-trip", () => {
    const path = join(tempDir, "sample.xlsx");
    const df = new DataFrame([
      { id: 1, city: "Austin", score: 10 },
      { id: 2, city: "Seattle", score: 12 },
    ]);

    df.to_excel({ path, sheet_name: "Scores" });
    const out = read_excel_sync(path, { sheet_name: "Scores" });
    expect(out.to_records()).toEqual([
      { id: 1, city: "Austin", score: 10 },
      { id: 2, city: "Seattle", score: 12 },
    ]);
  });

  test("top-level to_excel + async read_excel supports index_col", async () => {
    const path = join(tempDir, "index.xlsx");
    const df = new DataFrame([
      { id: 11, city: "Austin" },
      { id: 12, city: "Seattle" },
    ]);

    to_excel(df, { path, sheet_name: "Cities" });
    const out = await read_excel(path, { sheet_name: "Cities", index_col: "id" });
    expect(out.index).toEqual([11, 12]);
    expect(out.columns).toEqual(["city"]);
  });
});
