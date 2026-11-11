/* eslint-disable */
// Diagnostic: run each fixture case like the contract harness and print a
// compact expected-vs-actual diff for failures. Usage:
//   bun scripts/diag-contracts.ts [modeFilter] [opFilter]
import { concat, DataFrame, NA, NaT, Series } from "../src/index";
import type { CellValue, DType } from "../src/types";
import { isMissing } from "../src/utils";
import fixture from "../test/fixtures/pandas-dtype-contracts.json";

function encode(value: CellValue): unknown {
  if (value === NA) return { $missing: "NA" };
  if (value === NaT) return { $missing: "NaT" };
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
    if ("$missing" in value) return value.$missing === "NA" ? NA : value.$missing === "NaT" ? NaT : NaN;
    if ("$number" in value) return value.$number === "-0" ? -0 : value.$number === "Infinity" ? Infinity : -Infinity;
  }
  return value as CellValue;
}
// Missing-sentinel-insensitive normalization for accessor-style results
// (window/str/dt/pivot families): every missing form collapses together.
function normExp(value: unknown): unknown {
  if (value === null) return "__missing__";
  if (value && typeof value === "object" && !Array.isArray(value)) {
    if ("$missing" in value || "$number" in value) {
      const decoded = decode(value);
      return isMissing(decoded) ? "__missing__" : decoded;
    }
    return Object.fromEntries(Object.entries(value as Record<string, unknown>).map(([key, cell]) => [key, normExp(cell)]));
  }
  if (Array.isArray(value)) return value.map(normExp);
  return value;
}
function normAct(value: unknown): unknown {
  if (isMissing(value)) return "__missing__";
  if (Array.isArray(value)) return value.map(normAct);
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "bigint") return Number(value);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value as Record<string, unknown>).map(([key, cell]) => [key, normAct(cell)]));
  }
  return value;
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
    dtypes: frame.dtypes,
    columns: frame.columns,
    index: frame.index,
    indexName: frame.index_name ?? null,
    // The oracle encodes via itertuples, which yields no rows for a
    // zero-column frame, so normalize values() to [] in that case.
    values: frame.columns.length === 0 ? [] : frame.values().map(row => row.map(encode)),
  };
}
function eq(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true;
  if (typeof a !== "object" || typeof b !== "object" || a === null || b === null) return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((v, i) => eq(v, (b as unknown[])[i]));
  }
  const ka = Object.keys(a as object), kb = Object.keys(b as object);
  return ka.length === kb.length && ka.every((k) => eq((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k]));
}
const modeFilter = process.argv[2];
const opFilter = process.argv[3];
let fail = 0, pass = 0;
for (const [i, entry] of fixture.cases.entries()) {
  if (modeFilter && entry.mode !== modeFilter) continue;
  if (opFilter && entry.operation !== opFilter) continue;
  const dtype = entry.dtype as DType;
  const values = (entry.values ?? []).map(decode);
  const run = () => {
    if (entry.mode === "inferred_constructor") return new Series<CellValue>(values);
    if (entry.mode === "astype") return new Series<CellValue>(values, { dtype: "object" }).astype(dtype);
    if (entry.mode === "frame_arithmetic") {
      const fe = entry as unknown as FrameSpec & { operand: Record<string, unknown> };
      const source = buildFrame(fe);
      const os = fe.operand;
      const operand: number | DataFrame = "scalar" in os ? os.scalar as number : buildFrame(os as FrameSpec);
      const operation: unknown = Reflect.get(source, entry.operation!);
      return (operation as Function).apply(source, [operand]);
    }
    if (entry.mode === "frame_window") {
      const fe = entry as unknown as FrameSpec & { windowArgs: unknown[]; method: string };
      const source = buildFrame(fe);
      const factory: unknown = Reflect.get(source, entry.operation!);
      if (typeof factory !== "function") throw new Error(`Missing operation ${entry.operation}`);
      const accessor = (factory as Function).apply(source, fe.windowArgs ?? []) as object;
      const method: unknown = Reflect.get(accessor, fe.method);
      if (typeof method !== "function") throw new Error(`Missing window method ${fe.method}`);
      return (method as Function).apply(accessor, []);
    }
    if (entry.mode === "frame_pivot" || entry.mode === "frame_reduction" || entry.mode === "frame_selection" || entry.mode === "groupby") {
      const fe = entry as unknown as FrameSpec & { by?: string };
      const source = buildFrame(fe);
      const target: unknown = entry.mode === "groupby" ? source.groupby(fe.by!) : source;
      const operation: unknown = Reflect.get(target as object, entry.operation!);
      if (typeof operation !== "function") throw new Error(`Missing operation ${entry.operation}`);
      return (operation as Function).apply(target, entry.args);
    }
    if (entry.mode === "frame_binary") {
      const be = entry as unknown as { left: FrameSpec; right: FrameSpec; args: Record<string, unknown>[] };
      const left = buildFrame(be.left);
      const right = buildFrame(be.right);
      if (entry.operation === "concat") return concat([left, right], be.args[0]!);
      const operation: unknown = Reflect.get(left, entry.operation!);
      return (operation as Function).apply(left, [right, ...be.args]);
    }
    const source = new Series<CellValue>(values, {
      dtype,
      ...((entry.mode === "selection" || entry.mode === "replacement" || entry.mode === "elementwise" || entry.mode === "series_view" || entry.mode === "series_reduction" || entry.mode === "series_array" || entry.mode === "window") ? { index: values.map((_: unknown, i: number) => `row-${i}`), name: "v" } : {}),
      ...((entry as { index?: unknown[] }).index ? { index: (entry as { index: (string | number)[] }).index } : {}),
    });
    if (entry.mode === "window") {
      const we = entry as unknown as { windowArgs: unknown[]; method: string };
      const factory: unknown = Reflect.get(source, entry.operation!);
      if (typeof factory !== "function") throw new Error(`Missing operation ${entry.operation}`);
      const accessor = (factory as Function).apply(source, we.windowArgs ?? []) as object;
      const method: unknown = Reflect.get(accessor, we.method);
      if (typeof method !== "function") throw new Error(`Missing window method ${we.method}`);
      return (method as Function).apply(accessor, []);
    }
    if (entry.mode === "str" || entry.mode === "dt") {
      const accessor = (entry.mode === "str" ? source.str : source.dt) as unknown as Record<string, (...args: unknown[]) => unknown>;
      const method = accessor[entry.operation!];
      if (typeof method !== "function") throw new Error(`Missing accessor method ${entry.operation}`);
      return method.apply(accessor, entry.args);
    }
    if (entry.mode === "arithmetic") {
      const os = (entry as { operand: Record<string, unknown> }).operand;
      const operand: CellValue | Series<CellValue> = "scalar" in os
        ? os.scalar as CellValue
        : new Series<CellValue>((os.values as unknown[]).map(decode), { dtype: os.dtype as DType, ...(os.index ? { index: os.index as (string | number)[] } : {}) });
      const operation: unknown = Reflect.get(source, entry.operation!);
      return (operation as Function).apply(source, [operand]);
    }
    if (entry.mode === "pairwise") {
      // corr/cov take an index-aligned operand Series; unary ops like
      // autocorr only get args. Results are scalars compared missing-aware.
      const os = (entry as { operand?: { dtype: DType; values: unknown[]; index?: (string | number)[] } }).operand;
      const args: unknown[] = [...(entry.args ?? [])];
      if (os) {
        args.unshift(new Series<CellValue>(os.values.map(decode), {
          dtype: os.dtype,
          ...(os.index ? { index: os.index } : {}),
        }));
      }
      const operation: unknown = Reflect.get(source, entry.operation!);
      if (typeof operation !== "function") throw new Error(`Missing operation ${entry.operation}`);
      return (operation as Function).apply(source, args);
    }
    if ("operation" in entry) {
      const operation: unknown = Reflect.get(source, entry.operation!);
      if (typeof operation !== "function") throw new Error(`Missing operation ${entry.operation}`);
      return (operation as Function).apply(source, entry.args);
    }
    if (entry.mode === "infer_objects") return source.infer_objects();
    if (entry.mode === "convert_dtypes") return source.convert_dtypes();
    return source;
  };
  let result: unknown, threw: Error | null = null;
  try { result = run(); } catch (e) { threw = e as Error; }
  const failMsg = (what: string, exp: unknown, got: unknown) => {
    fail++;
    console.log(`#${i} ${entry.mode} ${entry.dtype ?? ""} ${entry.operation ?? ""} ${JSON.stringify(entry.args ?? "")} ${JSON.stringify(entry.values ?? "")} | ${what}: exp ${JSON.stringify(exp)} | got ${JSON.stringify(got)}`);
  };
  if (entry.error) {
    if (!threw) failMsg("should throw", entry.error, result);
    else pass++;
    continue;
  }
  if (threw) { failMsg("threw", entry.expected ?? entry.frameExpected, threw.message); continue; }
  const s = result as Series<CellValue>;
  let actual: unknown;
  if (entry.mode === "frame_arithmetic") {
    const f = result as DataFrame;
    actual = { dtypes: f.dtypes, columns: f.columns, index: f.index, values: f.values().map(r => r.map(encode)) };
  } else if (entry.mode === "frame_reduction") {
    const rec = result as Record<string, CellValue>;
    actual = { index: Object.keys(rec), values: Object.values(rec).map(encode) };
  } else if (entry.mode === "frame_selection" || entry.mode === "frame_binary") {
    const rowExp = (entry as { rowExpected?: { index: unknown[]; name: string | null; values: unknown[] } }).rowExpected;
    if (rowExp) {
      const got = result instanceof Series
        ? { dtype: result.dtype, index: result.index, name: result.name ?? null, values: result.to_list().map(encode) }
        : { index: Object.keys(result as object), values: Object.values(result as object).map(encode) };
      const want = result instanceof Series
        ? rowExp
        : { index: rowExp.index, values: rowExp.values };
      if (!eq(got, want)) failMsg("row", want, got); else pass++;
      continue;
    }
    actual = frameResult(result as DataFrame);
  } else if (entry.mode === "window" || entry.mode === "str" || entry.mode === "dt") {
    const exp = entry.expected as { values: unknown[]; index?: unknown[]; name?: string | null; dtype?: string };
    if (result instanceof Series) {
      actual = {
        dtype: result.dtype,
        index: result.index.map((l) => encode(l as CellValue)),
        name: result.name ?? null,
        values: normAct(result.to_list()),
      };
      if (!eq(actual, { dtype: exp.dtype, index: exp.index, name: exp.name ?? null, values: normExp(exp.values) })) {
        failMsg("window", { dtype: exp.dtype, index: exp.index, name: exp.name ?? null, values: normExp(exp.values) }, actual);
        continue;
      }
      pass++; continue;
    }
    actual = normAct(result);
    if (!eq(actual, normExp(exp.values))) failMsg("accessor", normExp(exp.values), actual); else pass++;
    continue;
  } else if (entry.mode === "frame_window") {
    const f = result as DataFrame;
    actual = normAct({ dtypes: f.dtypes, columns: f.columns, index: f.index, indexName: f.index_name ?? null, values: f.columns.length === 0 ? [] : f.values() });
    if (!eq(actual, normExp(entry.expected))) failMsg("frame_window", normExp(entry.expected), actual); else pass++;
    continue;
  } else if (entry.mode === "frame_pivot") {
    const fr = result as DataFrame;
    const keyColumn = (entry as unknown as { keyColumns: string[] }).keyColumns[0]!;
    const keyValues = fr.get(keyColumn).to_list();
    const dataColumns = fr.columns.filter((c) => c !== keyColumn);
    const positions = dataColumns.map((c) => fr.columns.indexOf(c));
    actual = normAct({
      dtypes: Object.fromEntries(dataColumns.map((c) => [c, fr.dtypes[c]])),
      columns: dataColumns,
      index: keyValues,
      indexName: keyColumn,
      values: fr.values().map((row) => positions.map((p) => row[p])),
    });
    if (!eq(actual, normExp(entry.expected))) failMsg("frame_pivot", normExp(entry.expected), actual); else pass++;
    continue;
  } else if (entry.mode === "groupby") {
    const fe = entry as unknown as { by: string; seriesExpected?: { dtype: string; values: unknown[]; index: unknown[] } };
    const fr = result as DataFrame;
    if (fe.seriesExpected) {
      const kv = fr.get(fe.by).to_list();
      const sz = fr.get("size") as Series<CellValue>;
      const got = { dtype: sz.dtype, values: sz.to_list().map(encode), index: kv.map(encode) };
      if (!eq(got, fe.seriesExpected)) failMsg("series", fe.seriesExpected, got); else pass++;
      continue;
    } else {
      const kv = fr.get(fe.by).to_list();
      const dc = fr.columns.filter((c) => c !== fe.by);
      const pos = dc.map((c) => fr.columns.indexOf(c));
      const exp = entry.expected as { dtypes: Record<string, string>; columns: string[]; index: unknown[]; values: unknown[][] };
      actual = { dtypes: Object.fromEntries(dc.map((c) => [c, fr.dtypes[c]])), columns: dc, index: kv.map(encode), values: fr.values().map((row) => pos.map((p) => encode(row[p]!))) };
      if (!eq(actual, { dtypes: exp.dtypes, columns: exp.columns, index: exp.index, values: exp.values })) {
        failMsg("groupby", { dtypes: exp.dtypes, columns: exp.columns, index: exp.index, values: exp.values }, actual);
        continue;
      }
      pass++; continue;
    }
  } else if (entry.mode === "series_reduction") {
    actual = encode(result as CellValue);
  } else if (entry.mode === "pairwise") {
    actual = normAct(result);
    if (!eq(actual, normExp(entry.expected))) failMsg("pairwise", normExp(entry.expected), actual); else pass++;
    continue;
  } else if (entry.mode === "series_array") {
    if (entry.operation === "factorize") {
      const [codes, uniques] = result as [number[], CellValue[]];
      actual = { codes, uniques: uniques.map(encode) };
    } else {
      actual = { values: (result as CellValue[]).map(encode) };
    }
  } else {
    const expectIndex = (entry.mode === "selection" || entry.mode === "replacement" || entry.mode === "arithmetic" || entry.mode === "elementwise" || entry.mode === "series_view") && entry.expected && "index" in entry.expected;
    const expectName = entry.mode === "selection" || entry.mode === "replacement" || entry.mode === "elementwise" || entry.mode === "series_view";
    const exp = entry.expected as { indexName?: string | null };
    actual = { dtype: s.dtype, values: s.to_list().map(encode), ...(expectIndex ? { index: s.index.map((l) => encode(l as CellValue)) } : {}), ...(expectName ? { name: s.name ?? null } : {}), ...(exp && "indexName" in exp ? { indexName: s.index_name ?? null } : {}) };
  }
  if (!eq(actual, entry.expected)) failMsg("series", entry.expected, actual); else pass++;

  // Frame-side check for elementwise entries.
  if (entry.mode === "elementwise" && ("frameExpected" in entry || "frameError" in entry)) {
    const fe = entry as unknown as { frameExpected?: unknown; frameError?: string };
    const frame = buildFrame({ dtypes: { v: dtype }, columns: { v: entry.values! }, index: (entry.values ?? []).map((_: unknown, ii: number) => `row-${ii}`) });
    const operation: unknown = Reflect.get(frame, entry.operation!);
    let fr: DataFrame | null = null, ferr: Error | null = null;
    try { fr = (operation as Function).apply(frame, entry.args); } catch (e) { ferr = e as Error; }
    if (fe.frameError) {
      if (!ferr) { fail++; console.log(`#${i} FRAME ${entry.dtype} ${entry.operation} ${JSON.stringify(entry.args)} ${JSON.stringify(entry.values)} | should throw ${fe.frameError}, got ${JSON.stringify(fr && fr.get("v").to_list().map(encode))}`); }
      else pass++;
    } else {
      if (ferr) { fail++; console.log(`#${i} FRAME ${entry.dtype} ${entry.operation} ${JSON.stringify(entry.args)} ${JSON.stringify(entry.values)} | threw ${ferr.message}`); }
      else {
        const col = fr!.get("v") as Series<CellValue>;
        const got = { dtype: fr!.dtypes.v, values: col.to_list().map(encode), index: fr!.index.map((l) => encode(l as CellValue)), name: "v" };
        if (!eq(got, fe.frameExpected)) failMsg("frame", fe.frameExpected, got); else pass++;
      }
    }
  }
}
console.log(`\n${pass} pass, ${fail} fail`);
