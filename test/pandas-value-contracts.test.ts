import { expect, test } from "bun:test";
import { Categorical, CategoricalDtype, DataFrame, Series, factorize, get_dummies } from "../src/index";
import type { CellValue } from "../src/types";
import fixture from "./fixtures/pandas-value-contracts.json";

// Matches the documented fixture transport, with zero numeric tolerance.
function encode(value: unknown): unknown {
  if (typeof value === "number") {
    if (Number.isNaN(value)) return null;
    if (!Number.isFinite(value)) return { $number: value > 0 ? "Infinity" : "-Infinity" };
    if (Object.is(value, -0)) return { $number: "-0" };
  }
  if (Array.isArray(value)) return value.map(encode);
  if (value instanceof Series) return encode(value.to_list());
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([k,v]) => [k,encode(v)]));
  return value ?? null;
}

for (const entry of fixture.cases) {
  test(`pandas ${fixture.pandasVersion}: ${entry.id} ${JSON.stringify(entry.values)} ${JSON.stringify(entry.args)}`, () => {
    const values: CellValue[] = entry.values;
    const args = entry.args as { decimals?: number; categories?: CellValue[]; other?: CellValue[]; ordered?: boolean; drop_first?: boolean; dummy_na?: boolean; positional?: (number|string)[]; metadata?: boolean };
    const execute = (): unknown => {
      if (entry.api.startsWith("Series.str.")) {
        const accessor = new Series(values).str;
        const member: unknown = Reflect.get(accessor, entry.api.slice(11));
        if (typeof member !== "function") throw new Error(`Missing string method: ${entry.api}`);
        return member.apply(accessor,args.positional ?? []);
      }
      if (entry.api.startsWith("Series.")) {
        const series = new Series(values, args.metadata ? {index:values.map((_,i)=>`label-${i}`),name:"v"} : {});
        const member: unknown = Reflect.get(series, entry.api.slice(7));
        if (typeof member !== "function") throw new Error(`Missing Series method: ${entry.api}`);
        const result = member.apply(series,args.positional ?? (entry.api === "Series.round" ? [args.decimals] : []));
        return args.metadata && result instanceof Series ? {values:result.to_list(),index:result.index,name:result.name} : result;
      }
      if (entry.api === "factorize") { const result = factorize(values); return [result.codes, result.uniques]; }
      if (entry.api === "Categorical") {
        const result = new Categorical(values, { categories: args.categories });
        return { categories: result.categories, codes: result.codes, ordered: result.ordered };
      }
      if (entry.api === "CategoricalDtype.equals") return new CategoricalDtype(values,args.ordered).equals(new CategoricalDtype(args.other,args.ordered));
      if (entry.api === "get_dummies") {
        const frame = new DataFrame({ id: values.map((_,i)=>i), color: values }, { index: values.map((_,i)=>`row-${i}`) });
        const result = get_dummies(frame,"color",args);
        return { columns: result.columns, index: result.index, data: result.to_records().map(row=>result.columns.map(column=>row[column])) };
      }
      throw new Error(`Missing adapter for ${entry.api}`);
    };
    if (entry.status === "error") expect(execute).toThrow();
    else expect(encode(execute())).toEqual(entry.expected);
  });
}
