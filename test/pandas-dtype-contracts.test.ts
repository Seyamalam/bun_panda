import updateFixture from "./fixtures/pandas-frame-update.json";
import assignmentFixture from "./fixtures/pandas-frame-assignment.json";
import { expect, test } from "bun:test";
import { concat, DataFrame, NA, Series } from "../src/index";
import type { CellValue, DType } from "../src/types";
import frameFixture from "./fixtures/pandas-frame-dtypes.json";
import fixture from "./fixtures/pandas-dtype-contracts.json";

function encode(value: CellValue): unknown {
  if (value === NA) return { $missing: "NA" };
  if (typeof value === "bigint") return Number(value);
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

type FrameSpec = { dtypes: Record<string, DType>; columns: Record<string, unknown[]>; index?: (string | number)[] };

function buildFrame(spec: FrameSpec): DataFrame {
  return new DataFrame(
    Object.fromEntries(Object.entries(spec.columns).map(([column, columnValues]) => [column, columnValues.map(decode)])),
    { dtype: "object", ...(spec.index ? { index: spec.index } : {}) }
  ).astype(spec.dtypes);
}

function frameResult(frame: DataFrame): unknown {
  return {
    dtypes: frame.dtypes(),
    columns: frame.columns,
    index: frame.index,
    indexName: frame.index_name ?? null,
    // The oracle encodes via itertuples, which yields no rows for a
    // zero-column frame, so normalize values() to [] in that case.
    values: frame.columns.length === 0 ? [] : frame.values().map(row => row.map(encode)),
  };
}

for (const [i, entry] of fixture.cases.entries()) {
  test(`pandas dtype ${i}: ${entry.mode} ${entry.dtype} ${JSON.stringify(entry.values)}`, () => {
    const dtype = entry.dtype as DType;
    const values = (entry.values ?? []).map(decode);
    const run = () => {
      if (entry.mode === "inferred_constructor") return new Series<CellValue>(values);
      if (entry.mode === "astype") return new Series<CellValue>(values, { dtype: "object" }).astype(dtype);
      if (entry.mode === "frame_arithmetic") {
        const frameEntry = entry as unknown as FrameSpec & { operand: Record<string, unknown> };
        const source = buildFrame(frameEntry);
        const operandSpec = frameEntry.operand;
        const operand: number | DataFrame = "scalar" in operandSpec
          ? operandSpec.scalar as number
          : buildFrame(operandSpec as FrameSpec);
        const operation: unknown = Reflect.get(source, entry.operation!);
        if (typeof operation !== "function") throw new Error(`Missing operation ${entry.operation}`);
        return operation.apply(source, [operand]) as unknown as Series<CellValue>;
      }
      if (entry.mode === "frame_reduction" || entry.mode === "frame_selection" || entry.mode === "groupby") {
        const frameEntry = entry as unknown as FrameSpec & { by?: string };
        const source = buildFrame(frameEntry);
        const target: unknown = entry.mode === "groupby" ? source.groupby(frameEntry.by!) : source;
        const operation: unknown = Reflect.get(target as object, entry.operation!);
        if (typeof operation !== "function") throw new Error(`Missing operation ${entry.operation}`);
        return operation.apply(target, entry.args) as unknown as Series<CellValue>;
      }
      if (entry.mode === "frame_binary") {
        const binaryEntry = entry as unknown as { left: FrameSpec; right: FrameSpec; args: Record<string, unknown>[] };
        const left = buildFrame(binaryEntry.left);
        const right = buildFrame(binaryEntry.right);
        if (entry.operation === "concat") return concat([left, right], binaryEntry.args[0]!) as unknown as Series<CellValue>;
        const operation: unknown = Reflect.get(left, entry.operation!);
        if (typeof operation !== "function") throw new Error(`Missing operation ${entry.operation}`);
        return operation.apply(left, [right, ...binaryEntry.args]) as unknown as Series<CellValue>;
      }
      const source = new Series<CellValue>(values, {
        dtype,
        ...((entry.mode === "selection" || entry.mode === "replacement" || entry.mode === "elementwise" || entry.mode === "series_view" || entry.mode === "series_reduction" || entry.mode === "series_array") ? { index: values.map((_: unknown, i: number) => `row-${i}`), name: "v" } : {}),
        ...((entry as { index?: unknown[] }).index ? { index: (entry as { index: (string | number)[] }).index } : {}),
      });
      if (entry.mode === "arithmetic") {
        const operandSpec = (entry as { operand: Record<string, unknown> }).operand;
        const operand: CellValue | Series<CellValue> = "scalar" in operandSpec
          ? operandSpec.scalar as CellValue
          : new Series<CellValue>(
            (operandSpec.values as unknown[]).map(decode),
            {
              dtype: operandSpec.dtype as DType,
              ...(operandSpec.index ? { index: operandSpec.index as (string | number)[] } : {}),
            }
          );
        const operation: unknown = Reflect.get(source, entry.operation!);
        if (typeof operation !== "function") throw new Error(`Missing operation ${entry.operation}`);
        return operation.apply(source, [operand]) as Series<CellValue>;
      }
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
    else if (entry.mode === "frame_arithmetic") {
      const frame = run() as unknown as DataFrame;
      expect<unknown>({ dtypes: frame.dtypes(), columns: frame.columns, index: frame.index, values: frame.values().map(row => row.map(encode)) }).toEqual(entry.expected!);
    } else if (entry.mode === "frame_reduction") {
      const record = run() as unknown as Record<string, CellValue>;
      expect<unknown>({ index: Object.keys(record), values: Object.values(record).map(encode) }).toEqual(entry.expected!);
    } else if (entry.mode === "frame_selection" || entry.mode === "frame_binary") {
      const result = run() as unknown;
      const rowExpected = (entry as { rowExpected?: { index: unknown[]; name: string | null; values: unknown[] } }).rowExpected;
      if (rowExpected) {
        if (result instanceof Series) {
          expect<unknown>({ dtype: result.dtype, index: result.index, name: result.name ?? null, values: result.to_list().map(encode) }).toEqual(rowExpected);
        } else {
          const row = result as Record<string, CellValue>;
          expect<unknown>({ index: Object.keys(row), values: Object.values(row).map(encode) }).toEqual({ index: rowExpected.index, values: rowExpected.values });
        }
      } else {
        expect<unknown>(frameResult(result as DataFrame)).toEqual(entry.expected!);
      }
    } else if (entry.mode === "groupby") {
      const result = run() as unknown as DataFrame;
      const frameEntry = entry as unknown as { by: string; seriesExpected?: { dtype: string; values: unknown[]; index: unknown[] } };
      // bun_panda keeps group keys as columns (as_index=false); pandas moves
      // them into the index. Adapt before comparing.
      if (frameEntry.seriesExpected) {
        const keyValues = (result as unknown as DataFrame).get(frameEntry.by).to_list();
        const size = result.get("size") as Series<CellValue>;
        expect<unknown>({ dtype: size.dtype, values: size.to_list().map(encode), index: keyValues.map(encode) }).toEqual(frameEntry.seriesExpected);
      } else {
        const keyValues = result.get(frameEntry.by).to_list();
        const dataColumns = result.columns.filter((column) => column !== frameEntry.by);
        const positions = dataColumns.map((column) => result.columns.indexOf(column));
        const expected = entry.expected as { dtypes: Record<string, string>; columns: string[]; index: unknown[]; indexName: string | null; values: unknown[][] };
        expect<unknown>({
          dtypes: Object.fromEntries(dataColumns.map((column) => [column, result.dtypes()[column]])),
          columns: dataColumns,
          index: keyValues.map(encode),
          values: result.values().map((row) => positions.map((p) => encode(row[p]!))),
        }).toEqual({ dtypes: expected.dtypes, columns: expected.columns, index: expected.index, values: expected.values });
      }
    } else if (entry.mode === "series_reduction") {
      expect<unknown>(encode(run() as unknown as CellValue)).toEqual(entry.expected!);
    } else if (entry.mode === "series_array") {
      const result = run() as unknown;
      if (entry.operation === "factorize") {
        const [codes, uniques] = result as [number[], CellValue[]];
        expect<unknown>({ codes, uniques: uniques.map(encode) }).toEqual(entry.expected!);
      } else {
        expect<unknown>({ values: (result as CellValue[]).map(encode) }).toEqual(entry.expected!);
      }
    } else {
      const result = run();
      const expectIndex = (entry.mode === "selection" || entry.mode === "replacement" || entry.mode === "arithmetic" || entry.mode === "elementwise" || entry.mode === "series_view") && entry.expected && "index" in entry.expected;
      const expectName = entry.mode === "selection" || entry.mode === "replacement" || entry.mode === "elementwise" || entry.mode === "series_view";
      const expected = entry.expected as { indexName?: string | null };
      expect({ dtype: result.dtype, values: result.to_list().map(encode), ...(expectIndex ? { index: result.index.map((label) => encode(label as CellValue)) } : {}), ...(expectName ? { name: result.name ?? null } : {}), ...(expected && "indexName" in expected ? { indexName: result.index_name ?? null } : {}) }).toEqual(entry.expected!);
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
  // Unsafe numbers no longer throw: int64 stores the double's exact integer
  // value as bigint (the represented value, since the literal already rounded).
  expect(new Series([Number.MAX_SAFE_INTEGER + 1], { dtype: "int64" }).to_list()).toEqual([9007199254740992n] as unknown as number[]);
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
  if (!("operation" in entry) || !["selection", "replacement", "elementwise"].includes(entry.mode) || entry.operation === "repeat" || entry.operation === "rename" || entry.operation === "set_axis") continue;
  test(`frame selection preserves ${entry.dtype}: ${entry.operation} ${JSON.stringify(entry.args)} ${JSON.stringify(entry.values)}`, () => {
    const frame = new DataFrame({ v: entry.values.map(decode) }, { dtype: entry.dtype as DType, index: entry.values.map((_: unknown, i: number) => `row-${i}`) });
    const operation: unknown = Reflect.get(frame, entry.operation!);
    if (typeof operation !== "function") throw new Error(`Missing frame operation ${entry.operation}`);
    let args = entry.args;
    if (entry.operation === "sort_values") args = ["v"];
    if (entry.operation === "reindex") args = [{ index: entry.args![0] }];
    const run = () => operation.apply(frame, args) as DataFrame;
    if ("frameError" in entry && entry.frameError) { expect(run).toThrow(); return; }
    if (entry.error && !("frameExpected" in entry)) { expect(run).toThrow(); return; }
    const result = run();
    expect({ dtype: result.dtypes().v, values: result.get("v").to_list().map(encode), index: result.index.map((label) => encode(label as CellValue)), name: "v" }).toEqual("frameExpected" in entry ? entry.frameExpected! : entry.expected!);
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
