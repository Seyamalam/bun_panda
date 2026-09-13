import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  read_feather, read_fwf, read_gbq, read_hdf, read_html, read_json_lines,
  read_orc, read_pickle, read_sas, read_spss, read_sql, read_sql_query,
  read_sql_table, read_stata, read_xml,
} from "../src/index";

const directory = mkdtempSync(join(tmpdir(), "bun-panda-format-tests-"));
afterAll(() => rmSync(directory, { recursive: true, force: true }));

describe("text format contracts", () => {
  test("JSON lines reads files, skips blanks and unions columns", () => {
    const path = join(directory, "rows.jsonl");
    writeFileSync(path, '{"a":1}\n\n{"a":2,"b":true}\n');
    expect(read_json_lines(path).to_records()).toEqual([{ a: 1, b: undefined }, { a: 2, b: true }]);
    expect(read_json_lines('{"a":1}\n{"a":2}').get("a").to_list()).toEqual([1, 2]);
    expect(() => read_json_lines(" ")).toThrow("non-empty");
  });

  test("fixed-width accepts widths, explicit headers and whitespace layouts", () => {
    const path = join(directory, "rows.fwf");
    writeFileSync(path, "id  flag\n1   true\n2   false\n");
    expect(read_fwf(path, { widths: [4, 5], header: true }).to_records()).toEqual([{ id: 1, flag: true }, { id: 2, flag: false }]);
    expect(read_fwf("1  alpha\n2  beta\n").to_records()).toEqual([{ col_0: 1, col_1: "alpha" }, { col_0: 2, col_1: "beta" }]);
    expect(read_fwf("1    \n2null\n", { colspecs: [[0, 1], [1, 5]], names: ["id", "value"] }).to_records()).toEqual([{ id: 1, value: null }, { id: 2, value: null }]);
    expect(() => read_fwf("a b\n", { header: true, names: ["a", "b"] })).toThrow("either");
    expect(() => read_fwf("")).toThrow("non-empty");
  });

  test("HTML selects a matching table, decodes entities and pads missing cells", () => {
    const html = '<table><tr><td>other</td></tr></table><table><tr><th>name</th><th>value</th></tr><tr><td><b>A&amp;B</b></td><td>2</td></tr><tr><td>&#65;&#x42;&quot;&apos;&nbsp;&unknown;</td></tr></table>';
    expect(read_html(html, { match: "name" }).to_records()).toEqual([{ name: "A&B", value: 2 }, { name: 'AB"\' &unknown;', value: NaN }]);
    expect(read_html('<table><tr><td>true</td><td>false</td></tr><tr><td>NaN</td></tr></table>', { header: false }).to_records()).toEqual([{ col_0: true, col_1: false }, { col_0: null, col_1: null }]);
    expect(read_html('<table><tr><th></th></tr><tr><td>1</td></tr></table>').columns).toEqual(["col_0"]);
    expect(() => read_html("plain")).toThrow("No <table>");
    expect(() => read_html(html, { match: "absent" })).toThrow("matching");
    expect(() => read_html("<table></table>")).toThrow("no rows");
  });

  test("XML reads chosen row elements, attributes and child text", () => {
    const xml = '<root><!-- ignore --><entry id="a&amp;b"><x>2</x><name><b>first</b></name></entry><entry id="c"><x>null</x><flag>true</flag></entry></root>';
    expect(read_xml(xml, { xpath: "/root/entry" }).to_records()).toEqual([
      { "@id": "a&b", x: 2, name: "first", flag: undefined },
      { "@id": "c", x: NaN, name: NaN, flag: true },
    ]);
    expect(() => read_xml("plain")).toThrow("No XML");
    expect(() => read_xml(xml, { xpath: "/missing" })).toThrow("No <missing>");
    expect(() => read_xml("")).toThrow("non-empty");
  });
});

describe("JSON-backed binary bridges", () => {
  for (const [name, read] of Object.entries({ read_pickle, read_feather, read_orc, read_hdf, read_sas, read_spss, read_stata })) {
    test(`${name} decodes supported JSON payloads and rejects true binary data`, () => {
      const records = [{ id: "a", value: 2 }, { id: "b", value: 4 }];
      const bytes = Buffer.from(JSON.stringify(records));
      expect(read(bytes).to_records()).toEqual(records);
      expect(read(bytes, { index_col: "id" }).index).toEqual(["a", "b"]);
      expect(read(bytes, { index_col: 0 }).get("value").to_list()).toEqual([2, 4]);
      const padded = Buffer.concat([Buffer.from("prefix"), bytes, Buffer.from("suffix")]);
      expect(read(new Uint8Array(padded.buffer, padded.byteOffset + 6, bytes.byteLength)).to_records()).toEqual(records);
      expect(read(Buffer.from('{"columns":["id","value"],"data":[["a",2],["b"]]}')).to_records()).toEqual([{ id: "a", value: 2 }, { id: "b", value: NaN }]);
      expect(read(Buffer.from('{"id":["a","b"],"value":[2]}')).to_records()).toEqual([{ id: "a", value: 2 }, { id: "b", value: NaN }]);
      const path = join(directory, `${name}.json`);
      writeFileSync(path, bytes);
      expect(read(path).to_records()).toEqual(records);
      expect(() => read(bytes, { index_col: "absent" })).toThrow("not found");
      expect(() => read(bytes, { index_col: 9 })).toThrow("not found");
      expect(() => read(Buffer.from([0, 1, 2, 255]))).toThrow("binary");
      expect(() => read(Buffer.from("42"))).toThrow("JSON-encoded");
    });
  }
});

describe("SQL adapters", () => {
  test("query functions receive SQL and parameters and return unioned columns", async () => {
    const query = async (sql: string, params?: unknown[]) => {
      expect(sql).toBe("SELECT * FROM items WHERE id = ?");
      expect(params).toEqual([7]);
      return [{ id: 7 }, { id: 8, value: "x" }];
    };
    expect((await read_sql_query("SELECT * FROM items WHERE id = ?", query, [7])).to_records()).toEqual([{ id: 7, value: NaN }, { id: 8, value: "x" }]);
  });

  test("connection methods retain this and table names are dispatched", async () => {
    const engine = {
      rows: [{ x: 3 }],
      all(sql: string) {
        expect(sql).toBe("SELECT * FROM items");
        return this.rows;
      },
    };
    expect((await read_sql("items", engine)).to_records()).toEqual(engine.rows);
    expect((await read_sql("SELECT * FROM items", engine)).to_records()).toEqual(engine.rows);
    expect((await read_sql_table("items", engine)).to_records()).toEqual(engine.rows);
    expect((await read_sql_query("SELECT 1", { query: () => [] })).empty).toBe(true);
    expect((await read_sql_query("SELECT 1", { run: () => [{ n: 1 }] })).to_records()).toEqual([{ n: 1 }]);
  });

  test("unsupported adapters, non-row results and empty queries fail clearly", async () => {
    await expect(read_sql_query("SELECT 1", {})).rejects.toThrow("Unsupported SQL connection");
    await expect(read_sql_query("SELECT 1", { query: () => ({ changes: 1 }) })).rejects.toThrow("non-array");
    await expect(read_sql_query("", () => [])).rejects.toThrow("non-empty");
    await expect(read_sql_table(" ", () => [])).rejects.toThrow("non-empty");
    await expect(read_gbq("SELECT 1")).rejects.toThrow("credentials");
  });
});
