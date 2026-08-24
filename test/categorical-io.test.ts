import { describe, expect, test } from "bun:test";
import {
  Categorical,
  CategoricalDtype,
  read_fwf,
  read_html,
  read_json_lines,
  read_xml,
  show_versions,
} from "../src/index";

describe("Categorical", () => {
  test("codes map values to category positions", () => {
    const cat = new Categorical(["a", "b", "a", "c"], { categories: ["a", "b", "c"] });
    expect(cat.codes).toEqual([0, 1, 0, 2]);
    expect(cat.categories).toEqual(["a", "b", "c"]);
    expect(cat.length).toBe(4);
  });

  test("equals compares codes and categories", () => {
    const a = new Categorical(["x", "y"], { categories: ["x", "y"] });
    const b = new Categorical(["x", "y"], { categories: ["x", "y"] });
    const c = new Categorical(["x", "z"], { categories: ["x", "y"] });
    expect(a.equals(b)).toBe(true);
    expect(a.equals(c)).toBe(false);
  });

  test("dtype carries ordered flag", () => {
    const dtype = new CategoricalDtype(["low", "high"], true);
    expect(dtype.ordered).toBe(true);
    const cat = new Categorical(["low", "high"], {
      categories: dtype.categories,
      ordered: true,
    });
    expect(cat.ordered).toBe(true);
  });

  test("map remaps categories through a function", () => {
    const cat = new Categorical(["a", "b"]);
    const mapped = cat.map((v) => (v === "a" ? "A" : v));
    expect(mapped.to_list()).toContain("A");
  });
});

describe("top-level io readers", () => {
  test("read_json_lines parses line-delimited JSON", () => {
    const df = read_json_lines(String.raw`{"a":1,"b":"x"}
{"a":2,"b":"y"}`);
    expect(df.columns.sort()).toEqual(["a", "b"]);
    expect(df.to_records()).toHaveLength(2);
  });

  test("read_html scrapes a simple table", () => {
    const df = read_html(
      "<table><tr><th>name</th><th>val</th></tr><tr><td>a</td><td>1</td></tr></table>"
    );
    expect(df.columns).toEqual(["name", "val"]);
    expect(df.to_records()[0]).toEqual({ name: "a", val: 1 });
  });

  test("read_fwf slices fixed-width columns from colspecs", () => {
    const input = "AB1234\nCD5678";
    const df = read_fwf(input, { colspecs: [[0, 2], [2, 6]], names: ["id", "num"] });
    expect(df.columns).toEqual(["id", "num"]);
    expect(df.to_records()[0]).toEqual({ id: "AB", num: 1234 });
  });

  test("read_xml extracts records from repeated elements", () => {
    const xml = "<root><row><a>1</a><b>2</b></row><row><a>3</a><b>4</b></row></root>";
    const df = read_xml(xml);
    expect(df.to_records()).toHaveLength(2);
  });
});

describe("meta helpers", () => {
  test("show_versions returns the version string", () => {
    const text = show_versions();
    expect(text).toContain("INSTALLED VERSIONS");
  });
});
