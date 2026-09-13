import { describe, expect, test } from "bun:test";
import fc from "fast-check";
import { DataFrame, parse_csv, parse_json } from "../src/index";

// Fixed seeds make failures replayable; fast-check reports a minimal counterexample.
const runs = { seed: 20260913, numRuns: 300 };
const cell = fc.oneof(fc.integer(), fc.boolean(), fc.constant(null), fc.string());
const records = fc.array(fc.record({ a: cell, b: cell, c: cell }), { maxLength: 30 });
const csvString = fc.array(fc.constantFrom("a", "Z", "বাংলা", "🙂", ",", ";", "\t", "\n", "\r", '"', " "), { maxLength: 25 })
  .map((characters) => `text:${characters.join("")}:end`);
const csvRecords = fc.array(fc.record({ a: fc.integer(), b: fc.boolean(), c: csvString }), { maxLength: 30 });

describe("parser properties", () => {
  test("CSV round-trips delimiter, quote, newline and Unicode cells", () => {
    fc.assert(fc.property(csvRecords, fc.constantFrom(",", ";", "\t"), (rows, sep) => {
      const frame = new DataFrame(rows, { columns: ["a", "b", "c"] });
      expect(parse_csv(frame.to_csv({ sep }), { sep }).to_records()).toEqual(rows);
    }), runs);
  });

  test("quoted and unquoted CSV paths agree on typed cells, BOM and CRLF", () => {
    fc.assert(fc.property(fc.array(fc.tuple(fc.integer(), fc.boolean()), { maxLength: 40 }), fc.boolean(), (rows, crlf) => {
      const newline = crlf ? "\r\n" : "\n";
      const raw = ["a,b", ...rows.map((row) => row.join(","))].join(newline);
      const quoted = ['"a","b"', ...rows.map((row) => row.map((v) => `"${v}"`).join(","))].join(newline);
      expect(parse_csv(`\uFEFF${raw}`).to_records()).toEqual(rows.map(([a, b]) => ({ a, b })));
      expect(parse_csv(quoted).to_records()).toEqual(parse_csv(raw).to_records());
    }), runs);
  });

  test("ragged CSV rows expand columns and pad missing values", () => {
    fc.assert(fc.property(fc.array(fc.array(fc.integer(), { minLength: 1, maxLength: 6 }), { minLength: 1, maxLength: 20 }), (rows) => {
      const width = Math.max(...rows.map((row) => row.length));
      const expected = rows.map((row) => Object.fromEntries(Array.from({ length: width }, (_, i) => [`col_${i}`, row[i] ?? NaN])));
      for (const quoted of [false, true]) {
        const csv = rows.map((row) => row.map((v) => quoted ? `"${v}"` : String(v)).join(",")).join("\n");
        expect(parse_csv(csv, { header: false }).to_records()).toEqual(expected);
      }
    }), runs);
  });

  test("JSON records, columns and lines preserve primitive cells", () => {
    fc.assert(fc.property(records, (rows) => {
      expect(parse_json(JSON.stringify(rows)).to_records()).toEqual(new DataFrame(rows).to_records());
      const columnar = Object.fromEntries(["a", "b", "c"].map((key) => [key, rows.map((row) => row[key as keyof typeof row])]));
      expect(parse_json(JSON.stringify(columnar), { orient: "list" }).to_records()).toEqual(new DataFrame(rows).to_records());
      const lines = `\uFEFF\n${rows.map((row) => JSON.stringify(row)).join("\r\n\n")}\n`;
      expect(parse_json(lines, { lines: true }).to_records()).toEqual(new DataFrame(rows).to_records());
      expect(parse_json(new DataFrame(rows).to_json()).to_records()).toEqual(new DataFrame(rows).to_records());
    }), runs);
  });

  test("JSON index extraction preserves row order and remaining columns", () => {
    fc.assert(fc.property(records, (rows) => {
      if (rows.length === 0) return;
      const indexed = rows.map((row, id) => ({ id, ...row }));
      const frame = parse_json(JSON.stringify(indexed), { index_col: "id" });
      expect(frame.index).toEqual(rows.map((_, i) => i));
      expect(frame.to_records()).toEqual(new DataFrame(rows).to_records());
    }), runs);
  });

  test("JSON rejects non-object records and non-array columns", () => {
    fc.assert(fc.property(fc.oneof(fc.integer(), fc.boolean(), fc.string(), fc.constant(null)), (value) => {
      expect(() => parse_json(JSON.stringify([value]))).toThrow();
      expect(() => parse_json(JSON.stringify({ a: value }), { orient: "list" })).toThrow();
      expect(() => parse_json(JSON.stringify(value), { lines: true })).toThrow();
    }), runs);
  });
});
