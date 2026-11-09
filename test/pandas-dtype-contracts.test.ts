import updateFixture from "./fixtures/pandas-frame-update.json";
import assignmentFixture from "./fixtures/pandas-frame-assignment.json";
import { expect, test } from "bun:test";
import { DataFrame, NA, Series } from "../src/index";
import type { CellValue, DType } from "../src/types";
import frameFixture from "./fixtures/pandas-frame-dtypes.json";
import fixture from "./fixtures/pandas-dtype-contracts.json";

function encode(value: CellValue): unknown {
  if (value === NA) return { $missing: "NA" };
  if (typeof value === "number") {
    if (Number.isNaN(value)) return { $missing: "NaN" };
    if (!Number.isFinite(value)) return { $number: value > 0 ? "Infinity" : "-Infinity" };
    if (Object.is(value, -0)) return { $number: "-0" };
  }
  return value;
}

function decode(value: unknown): CellValue {
  if (value && typeof value === "object") {
    if ("$missing" in value) return value.$missing === "NA" ? NA : NaN;
    if ("$number" in value) return value.$number === "-0" ? -0 : value.$number === "Infinity" ? Infinity : -Infinity;
  }
  return value as CellValue;
}

for (const [i, entry] of fixture.cases.entries()) {
  test(`pandas dtype ${i}: ${entry.mode} ${entry.dtype} ${JSON.stringify(entry.values)}`, () => {
    const dtype = entry.dtype as DType;
    const values = entry.values.map(decode);
    const run = () => {
      if (entry.mode === "inferred_constructor") return new Series<CellValue>(values);
      if (entry.mode === "astype") return new Series<CellValue>(values, { dtype: "object" }).astype(dtype);
      const source = new Series<CellValue>(values, { dtype, ...((entry.mode === "selection" || entry.mode === "replacement") ? { index: values.map((_: unknown, i: number) => `row-${i}`), name: "v" } : {}) });
      if ("operation" in entry) {
        const operation: unknown = Reflect.get(source, entry.operation!);
        if (typeof operation !== "function") throw new Error(`Missing operation ${entry.operation}`);
        return operation.apply(source, entry.args) as Series<CellValue>;
      }
      if (entry.mode === "infer_objects") return source.infer_objects();
      if (entry.mode === "convert_dtypes") return source.convert_dtypes();
      return source;
    };
    if (entry.error) expect(run).toThrow();
    else {
      const result = run();
      expect({ dtype: result.dtype, values: result.to_list().map(encode), ...((entry.mode === "selection" || entry.mode === "replacement") ? { index: result.index, name: result.name } : {}) }).toEqual(entry.expected!);
      expect(result.copy().dtype).toBe(entry.expected!.dtype);
      expect(result.head(0).dtype).toBe(entry.expected!.dtype);
    }
  });
}

test("explicit frame casts preserve source, metadata, and missing representations", () => {
  const source = new DataFrame({ id: ["1", null], score: [0.1, null] }, { index: ["a", "b"] });
  const typed = source.astype({ id: "Int16", score: "float32" });
  expect(typed.dtypes()).toEqual({ id: "Int16", score: "float32" });
  expect(typed.get("id").to_list()).toEqual([1, NA]);
  expect(typed.get("score").to_list()).toEqual([Math.fround(0.1), NaN]);
  expect(typed.copy().dtypes()).toEqual(typed.dtypes());
  expect(typed.get("id").dtype).toBe("Int16");
  expect(typed.index).toEqual(["a", "b"]);
  expect(source.get("id").to_list()).toEqual(["1", NaN]);
  expect(new DataFrame([], { columns: ["id"], dtype: "UInt32" }).dtypes()).toEqual({ id: "UInt32" });
});

test("integer ranges are checked and numpy integer casts wrap", () => {
  expect(new Series([128, -129], { dtype: "int16" }).astype("int8").to_list()).toEqual([-128, 127]);
  expect(() => new Series([128], { dtype: "Int8" })).toThrow();
  expect(() => new Series([Number.MAX_SAFE_INTEGER + 1], { dtype: "int64" })).toThrow("safe range");
  expect(() => new Series([], { dtype: "unknown" as DType })).toThrow("not understood");
  expect(() => new DataFrame().astype("unknown" as DType)).toThrow("not understood");
});

test("frame inference preserves text and labels while selecting nullable dtypes", () => {
  const frame = new DataFrame({ code: ["001", null], qty: [1, null], active: [true, null], mixed: [1, "1"] }, { dtype: "object", index: ["a", "b"] });
  const result = frame.convert_dtypes();
  expect(result.dtypes()).toEqual({ code: "string", qty: "Int64", active: "boolean", mixed: "object" });
  expect(result.to_records()).toEqual([
    { code: "001", qty: 1, active: true, mixed: 1 },
    { code: NA, qty: NA, active: NA, mixed: "1" },
  ]);
  expect(result.index).toEqual(["a", "b"]);
  expect(frame.infer_objects().dtypes()).toEqual({ code: "str", qty: "float64", active: "object", mixed: "object" });
  expect(frame.get("qty").to_list()).toEqual([1, null]);
});

for (const entry of fixture.cases) {
  if (!("operation" in entry) || entry.operation === "repeat" || entry.operation === "rename" || entry.operation === "set_axis") continue;
  test(`frame selection preserves ${entry.dtype}: ${entry.operation} ${JSON.stringify(entry.args)} ${JSON.stringify(entry.values)}`, () => {
    const frame = new DataFrame({ v: entry.values.map(decode) }, { dtype: entry.dtype as DType, index: entry.values.map((_: unknown, i: number) => `row-${i}`) });
    const operation: unknown = Reflect.get(frame, entry.operation!);
    if (typeof operation !== "function") throw new Error(`Missing frame operation ${entry.operation}`);
    let args = entry.args;
    if (entry.operation === "sort_values") args = ["v"];
    if (entry.operation === "reindex") args = [{ index: entry.args![0] }];
    const run = () => operation.apply(frame, args) as DataFrame;
    if (entry.error && !("frameExpected" in entry)) { expect(run).toThrow(); return; }
    const result = run();
    expect({ dtype: result.dtypes().v, values: result.get("v").to_list().map(encode), index: result.index, name: "v" }).toEqual("frameExpected" in entry ? entry.frameExpected! : entry.expected!);
  });
}

test("column and index selection retain dtypes by column name", () => {
  const frame = new DataFrame({ a: [1, 2], b: [3, 4] }).astype({ a: "Int16", b: "float32" });
  expect(frame.select(["b"]).dtypes()).toEqual({ b: "float32" });
  expect(frame.drop("b").dtypes()).toEqual({ a: "Int16" });
  expect(frame.filter([false, false]).dtypes()).toEqual(frame.dtypes());
  expect(frame.iloc([1, 0])).toBeInstanceOf(DataFrame);
  expect((frame.iloc([1, 0]) as DataFrame).dtypes()).toEqual(frame.dtypes());
  expect((frame.loc([1]) as DataFrame).dtypes()).toEqual(frame.dtypes());
  expect(frame.rename({ a: "id" }).dtypes()).toEqual({ id: "Int16", b: "float32" });
  expect(frame.get("a").to_frame().dtypes()).toEqual({ a: "Int16" });
  expect(frame.set_axis(["id", "score"], "columns").dtypes()).toEqual({ id: "Int16", score: "float32" });
  expect((frame.get("a").reset_index() as DataFrame).dtypes().a).toBe("Int16");
  expect(frame.pop("b").dtype).toBe("float32");
});

for (const [i, entry] of frameFixture.cases.entries()) {
  test(`pandas inferred frame ${i}: ${entry.operation}`, () => {
    const values = entry.values.map(decode);
    const source = new DataFrame({ v: values }, { index: values.map((_, i) => `row-${i}`) });
    const operation: unknown = Reflect.get(source, entry.operation);
    if (typeof operation !== "function") throw new Error(`Missing operation ${entry.operation}`);
    const result = operation.apply(source, entry.args) as DataFrame;
    expect<unknown>({ dtypes: result.dtypes(), columns: result.columns, index: result.index, values: result.values().map(row => row.map(encode)) }).toEqual(entry.expected);
  });
}

for (const [i, entry] of assignmentFixture.cases.entries()) {
  test(`pandas frame assignment ${i}: ${entry.operation}`, () => {
    const source = new DataFrame({ v: entry.values.map(decode), keep: [null, null] }, { dtype: "object" }).astype({ v: entry.dtype as DType });
    const incoming = entry.incoming.map(decode);
    const value = entry.inputDtype ? new Series<CellValue>(incoming, { dtype: entry.inputDtype as DType, index: [1, 0] }) : incoming;
    const result = entry.operation === "assign" ? source.assign({ v: value }) : entry.operation === "insert" ? (source.insert(1, "new", value), source) : (source.isetitem(0, value), source);
    expect<unknown>({ dtypes: result.dtypes(), columns: result.columns, values: result.values().map(row => row.map(encode)) }).toEqual(entry.expected);
  });
}

for (const [i, entry] of updateFixture.cases.entries()) {
  test(`pandas frame update ${i}: ${entry.dtype}`, () => {
    const source = new DataFrame({ v: entry.values.map(decode) }, { dtype: entry.dtype as DType });
    const incoming = new DataFrame({ v: entry.incoming.map(decode) });
    const run = () => source.update(incoming);
    if (entry.error) expect(run).toThrow();
    else {
      expect(run()).toBeUndefined();
      expect<unknown>({ dtype: source.dtypes().v, values: source.get("v").to_list().map(encode) }).toEqual(entry.expected);
    }
  });
}

test("rejected updates leave stored values and dtypes consistent", () => {
  const frame = new DataFrame({ n: [1, 2] }, { dtype: "int16" });
  const original = frame.copy();
  expect(() => frame.update(new DataFrame({ n: [1.5, null] }))).toThrow();
  expect(frame.equals(original)).toBe(true);
  expect(frame.get("n").to_list()).toEqual([1, 2]);
});
