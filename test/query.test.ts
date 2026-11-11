import { describe, expect, test } from "bun:test";
import { DataFrame } from "../src/dataframe";
import { Series } from "../src/series";
import { NotImplementedError, UndefinedVariableError } from "../src/errors";
import type { CellValue } from "../src/types";

// All expected index labels verified against pandas 3.0.5 (`uv run --with
// 'pandas==3.0.5' python3`) unless noted otherwise.
const df = () =>
  new DataFrame([
    { qty: 1, city: "NYC", price: 2.5, flag: true },
    { qty: 6, city: "LA", price: 1.0, flag: false },
    { qty: 8, city: "NYC", price: 4.0, flag: true },
    { qty: 3, city: "SF", price: NaN, flag: false },
  ]);

const ids = (frame: DataFrame): (string | number | readonly (string | number)[])[] => frame.index;

describe("DataFrame.query — comparisons", () => {
  test.each([
    ["qty > 5", [1, 2]],
    ["qty > 5 and city == 'NYC'", [2]],
    ["city == \"LA\" or qty < 2", [0, 1]],
    ["qty >= 3 and qty <= 6", [1, 3]],
    ["city != 'NYC'", [1, 3]],
    ["qty > 2 and qty < 8", [1, 3]],
    ["1 < qty < 8", [1, 3]],
    ["qty < 3 < 7", [0]],
    ["qty == qty", [0, 1, 2, 3]],
    ["qty != 1", [1, 2, 3]],
    ["(qty > 5) == flag", [2, 3]],
    ["qty > 5 and qty < 8 and city == 'NYC'", []],
    ["(qty > 5 or qty < 2) and city != 'LA'", [0, 2]],
    ["'LA' == city", [1]],
    ["city == 'NYC'", [0, 2]],
    ["qty == 6 and price > 1 or city == 'SF'", [3]],
  ])("%s → %j", (expr, expected) => {
    expect(ids(df().query(expr))).toEqual(expected);
  });

  test("index keyword resolves to the index labels", () => {
    expect(ids(df().query("index > 1"))).toEqual([2, 3]);
    const labelled = new DataFrame(
      [{ a: 1 }, { a: 2 }, { a: 3 }, { a: 4 }],
      { index: ["a", "b", "c", "d"] }
    );
    expect(labelled.query("index == 'b'").index).toEqual(["b"]);
  });

  test("a column named 'index' shadows the index term", () => {
    const frame = new DataFrame([{ index: 10, v: 1 }, { index: 20, v: 2 }]);
    expect(frame.query("index > 15").index).toEqual([1]);
  });
});

describe("DataFrame.query — boolean ops", () => {
  test.each([
    ["not (qty > 5)", [0, 3]],
    ["not qty > 5", [0, 3]],
    ["~(qty > 5)", [0, 3]],
    ["~flag", [1, 3]],
    ["flag", [0, 2]],
    ["flag and qty > 5", [2]],
    ["qty > 1 and qty < 8 or city == 'SF'", [1, 3]],
    ["qty == 1 | flag", [0, 2]],
    ["flag | flag", [0, 2]],
    ["flag & flag", [0, 2]],
    ["~ flag | ~ flag", [1, 3]],
    ["not qty > 5 and flag", [0]],
    ["qty > 5 or flag", [0, 1, 2]],
    ["(qty > 5) | price", [0, 1, 2]],
    ["flag or price", [0, 1, 2]],
    ["price > 1 or flag", [0, 2]],
    ["qty > 5 or price", [0, 1, 2]],
    ["qty or flag", [0, 1, 2, 3]],
    ["flag or qty", [0, 1, 2, 3]],
    ["qty and flag", [0]],
    ["flag and qty", [0]],
    ["qty | flag", [0, 1, 2, 3]],
    ["flag & qty", [0]],
    ["flag | True", [0, 1, 2, 3]],
    ["flag & False", []],
    ["True and flag", [0, 2]],
    ["False or flag", [0, 2]],
    ["flag and city", [0, 2]],
    ["not price > 1", [1, 3]],
    ["qty % 2 == 0 and not flag", [1]],
  ])("%s → %j", (expr, expected) => {
    expect(ids(df().query(expr))).toEqual(expected);
  });

  test("& / | have comparison-level precedence like pandas (unlike Python)", () => {
    expect(ids(df().query("qty > 5 & city == 'NYC'"))).toEqual([2]);
    expect(ids(df().query("city == 'LA' | qty == 1"))).toEqual([0, 1]);
    expect(ids(df().query("flag | flag & qty > 5"))).toEqual([0, 2]);
    expect(ids(df().query("flag | qty > 5 & city == 'LA'"))).toEqual([0, 1, 2]);
  });
});

describe("DataFrame.query — arithmetic in expressions", () => {
  test.each([
    ["qty + 1 > 5", [1, 2]],
    ["qty * 2 == 12", [1]],
    ["qty / 2 == 4", [2]],
    ["qty // 2 == 4", [2]],
    ["qty % 3 == 0", [1, 3]],
    ["qty ** 2 > 30", [1, 2]],
    ["-qty < -5", [1, 2]],
    ["qty - 2 > 4", [2]],
  ])("%s → %j", (expr, expected) => {
    expect(ids(df().query(expr))).toEqual(expected);
  });
});

describe("DataFrame.query — in / not in", () => {
  test.each([
    ["qty in [1, 3]", [0, 3]],
    ["qty not in [1, 3]", [1, 2]],
    ["city in ('NYC', 'SF')", [0, 2, 3]],
    ["qty in []", []],
    ["qty not in []", [0, 1, 2, 3]],
    ["qty in [1]", [0]],
    ["qty == [1, 3]", [0, 3]],
    ["qty != [1, 3]", [1, 2]],
    ["'LA' in city", [1]],
    ["'QX' in city", []],
    ["'NY' in city", []],
    ["qty in qty", [0, 1, 2, 3]],
    ["qty in city", []],
    ["city in qty", []],
  ])("%s → %j", (expr, expected) => {
    expect(ids(df().query(expr))).toEqual(expected);
  });

  test("scalar `in` evaluates against the index and raises like pandas", () => {
    expect(() => df().query("2 in qty")).toThrow(/boolean label can not be used/);
    expect(() => df().query("99 in qty")).toThrow(/boolean label can not be used/);
  });
});

describe("DataFrame.query — @variables", () => {
  test("local_dict resolves @name", () => {
    expect(ids(df().query("qty > @thresh", { local_dict: { thresh: 5 } }))).toEqual([1, 2]);
    expect(ids(df().query("qty > @t", { local_dict: { t: 4 } }))).toEqual([1, 2]);
  });

  test("@list participates in membership", () => {
    expect(ids(df().query("city in @cities", { local_dict: { cities: ["NYC", "SF"] } }))).toEqual([0, 2, 3]);
    expect(ids(df().query("qty.isin(@lst)", { local_dict: { lst: [1, 3] } }))).toEqual([0, 3]);
  });

  test("env aliases local_dict; resolvers consulted in order", () => {
    expect(ids(df().query("qty > @t", { env: { t: 4 } }))).toEqual([1, 2]);
    expect(ids(df().query("qty > @t", { resolvers: [{ t: 4 }] }))).toEqual([1, 2]);
    expect(ids(df().query("qty > @t", { local_dict: { t: 9 }, resolvers: [{ t: 4 }] }))).toEqual([]);
    expect(ids(df().query("qty > @t", { global_dict: { t: 4 } }))).toEqual([1, 2]);
  });

  test("unknown @var raises UndefinedVariableError like pandas", () => {
    expect(() => df().query("qty > @missing_var")).toThrow(UndefinedVariableError);
    expect(() => df().query("qty > @missing_var"))
      .toThrow(/local variable 'missing_var' is not defined/);
  });
});

describe("DataFrame.query — backtick-quoted columns", () => {
  const frame = new DataFrame([{ "my col": 1, x: 2 }, { "my col": 5, x: 4 }, { "my col": 9, x: 6 }]);

  test("backtick names with spaces resolve as columns", () => {
    expect(frame.query("`my col` > 3").index).toEqual([1, 2]);
    expect(frame.query("`my col` > 3 and x < 5").index).toEqual([1]);
  });
});

describe("DataFrame.query — missing values", () => {
  test.each([
    ["price > 1", [0, 2]],
    ["price < 3", [0, 1]],
    ["not (price < 3)", [2, 3]],
    ["price != price", [3]],
    ["price == price", [0, 1, 2]],
    ["price.isna()", [3]],
    ["price.isnull()", [3]],
    ["price.notna()", [0, 1, 2]],
    ["price.notnull()", [0, 1, 2]],
    ["price.isna() or flag", [0, 2, 3]],
    ["price.isna() == True", [3]],
    ["qty == None", []],
    ["price == None", []],
    ["qty != None", [0, 1, 2, 3]],
    ["qty == True", [0]],
    ["flag == True", [0, 2]],
  ])("%s → %j", (expr, expected) => {
    expect(ids(df().query(expr))).toEqual(expected);
  });
});

describe("DataFrame.query — method calls", () => {
  test.each([
    ["qty.isin([1, 3])", [0, 3]],
    ["city.isin(['NYC'])", [0, 2]],
    ["city.str.contains('N')", [0, 2]],
    ["city.str.startswith('L')", [1]],
    ["city.str.endswith('A')", [1]],
  ])("%s → %j", (expr, expected) => {
    expect(ids(df().query(expr))).toEqual(expected);
  });

  test("unsupported functions and methods raise pandas-style errors", () => {
    expect(() => df().query("isna(price)")).toThrow(/"isna" is not a supported function/);
    expect(() => df().query("price.median()")).toThrow(/"median" is not a supported function/);
  });
});

describe("DataFrame.query — pandas-faithful errors", () => {
  test("unknown column raises UndefinedVariableError", () => {
    expect(() => df().query("zzz > 1")).toThrow(UndefinedVariableError);
    expect(() => df().query("zzz > 1")).toThrow(/name 'zzz' is not defined/);
    // bare names never resolve outer scope — @ is required
    expect(() => df().query("qty > thresh")).toThrow(/name 'thresh' is not defined/);
  });

  test("malformed expressions raise SyntaxError", () => {
    expect(() => df().query("qty >")).toThrow(SyntaxError);
    expect(() => df().query("qty >")).toThrow(/invalid syntax/);
    expect(() => df().query("qty == 1 ==")).toThrow(SyntaxError);
    expect(() => df().query("qty > 5)")).toThrow(SyntaxError);
    expect(() => df().query("")).toThrow(/expr cannot be an empty string/);
  });

  test("unsupported operator nodes raise AttributeError like pandas", () => {
    expect(() => df().query("qty >> 5")).toThrow(/visit_RShift/);
    expect(() => df().query("qty << 1")).toThrow(/visit_LShift/);
    expect(() => df().query("qty ^ 1")).toThrow(/visit_BitXor/);
  });

  test("is / is not raise NotImplementedError like pandas", () => {
    expect(() => df().query("qty is None")).toThrow(NotImplementedError);
    expect(() => df().query("qty is None")).toThrow(/'Is' nodes are not implemented/);
    expect(() => df().query("qty is not None")).toThrow(/'IsNot' nodes are not implemented/);
    expect(() => df().query("qty if flag else 0")).toThrow(/'IfExp' nodes are not implemented/);
  });

  test("boolean ops on non-boolean operands raise TypeError", () => {
    expect(() => df().query("qty and price")).toThrow(TypeError);
    expect(() => df().query("qty and price")).toThrow(/unsupported operand type\(s\) for &: 'int' and 'float'/);
    expect(() => df().query("price or flag")).toThrow(/unsupported operand type\(s\) for \|: 'float' and 'bool'/);
    expect(() => df().query("city or flag")).toThrow(/unsupported operand type\(s\) for \|: 'str' and 'bool'/);
    expect(() => df().query("price | (qty > 5)")).toThrow(TypeError);
  });

  test("scalar-only bool ops raise NotImplementedError like pandas", () => {
    expect(() => df().query("qty & 1")).toThrow(NotImplementedError);
    expect(() => df().query("qty & 1")).toThrow(/cannot evaluate scalar only bool ops/);
    expect(() => df().query("flag | 1")).toThrow(/cannot evaluate scalar only bool ops/);
    expect(() => df().query("1 | flag")).toThrow(/cannot evaluate scalar only bool ops/);
  });

  test("mixed-type comparisons raise pandas-style TypeError", () => {
    expect(() => df().query("city < qty")).toThrow(TypeError);
    expect(() => df().query("city < qty")).toThrow(/'<' not supported between instances of 'str' and 'int'/);
    expect(() => df().query("qty + city")).toThrow(/unsupported operand type\(s\) for \+/);
    expect(() => df().query("qty in 1")).toThrow(/only list-like objects are allowed to be passed to isin\(\)/);
    expect(() => df().query("~price")).toThrow(TypeError);
  });

  test("non-boolean results go through .loc[] semantics → KeyError", () => {
    expect(() => df().query("qty + 1")).toThrow(/\[7, 9, 4\] not in index/);
    expect(() => df().query("2 * qty + 1")).toThrow(/\[13, 17, 7\] not in index/);
    expect(() => df().query("True")).toThrow(/True: boolean label can not be used without a boolean index/);
    expect(() => df().query("not qty")).toThrow(/None of \[Index\(\[-2, -7, -9, -4\], dtype='int64'\)\] are in the \[index\]/);
    expect(() => df().query("qty & qty")).toThrow(/\[6, 8\] not in index/);
  });

  test("assignment inside query() raises pandas ValueError", () => {
    expect(() => df().query("a = 1")).toThrow(/cannot assign without a target object/);
  });
});

describe("DataFrame.query — options", () => {
  test("inplace: true mutates the frame and returns undefined", () => {
    const frame = df();
    const result = frame.query("qty > 5", { inplace: true });
    expect(result).toBeUndefined();
    expect(frame.index).toEqual([1, 2]);
    expect(frame.to_records()).toEqual([
      { qty: 6, city: "LA", price: 1.0, flag: false },
      { qty: 8, city: "NYC", price: 4.0, flag: true },
    ]);
  });

  test("function predicate keeps the pre-existing filter behavior", () => {
    expect(df().query((row) => (row.qty as number) > 5).index).toEqual([1, 2]);
  });

  test("empty frames evaluate safely", () => {
    const empty = new DataFrame([{ a: 1 }]).filter(() => false);
    expect(empty.query("a > 0").index).toEqual([]);
  });
});

describe("DataFrame.eval", () => {
  test("expression results come back as Series", () => {
    const out = df().eval("qty + price");
    expect(out).toBeInstanceOf(Series);
    expect((out as Series<CellValue>).to_list()).toEqual([3.5, 7.0, 12.0, NaN]);
    expect((df().eval("qty > 5") as Series<CellValue>).to_list()).toEqual([false, true, true, false]);
    expect((df().eval("flag and qty > 5") as Series<CellValue>).to_list()).toEqual([false, false, true, false]);
    expect((df().eval("qty > 5 or city == 'SF'") as Series<CellValue>).to_list()).toEqual([false, true, true, true]);
    expect((df().eval("not flag") as Series<CellValue>).to_list()).toEqual([false, true, false, true]);
    expect((df().eval("flag | True") as Series<CellValue>).to_list()).toEqual([true, true, true, true]);
  });

  test("constant expressions return scalars like pandas", () => {
    expect(df().eval("True", {})).toBe(true);
    expect(df().eval("2 + 3", {})).toBe(5);
  });

  test("@vars resolve via local_dict", () => {
    expect((df().eval("qty + @x", { local_dict: { x: 2 } }) as Series<CellValue>).to_list())
      .toEqual([3, 8, 10, 5]);
  });

  test("string column + string column concatenates like pandas", () => {
    expect((df().eval("city + city") as Series<CellValue>).to_list())
      .toEqual(["NYCNYC", "LALA", "NYCNYC", "SFSF"]);
  });

  test("col = expr assignment returns a new DataFrame", () => {
    const out = df().eval("total = qty * 2", {});
    expect(out).toBeInstanceOf(DataFrame);
    const frame = out as DataFrame;
    expect(frame.columns).toEqual(["qty", "city", "price", "flag", "total"]);
    expect(frame.to_records().map((r) => r.total)).toEqual([2, 12, 16, 6]);
    // overwrite an existing column
    const over = df().eval("qty = qty * 10", {}) as DataFrame;
    expect(over.to_records().map((r) => r.qty)).toEqual([10, 60, 80, 30]);
  });

  test("col = expr with inplace mutates and returns undefined", () => {
    const frame = df();
    const result = frame.eval("total = qty * 2", { inplace: true });
    expect(result).toBeUndefined();
    expect(frame.columns).toEqual(["qty", "city", "price", "flag", "total"]);
    expect(frame.to_records().map((r) => r.total)).toEqual([2, 12, 16, 6]);
  });

  test("inplace without assignment raises pandas ValueError", () => {
    expect(() => df().eval("qty == 1", { inplace: true }))
      .toThrow(/Cannot operate inplace if there is no assignment/);
  });

  test("eval raises pandas-style errors for unknown names and bad syntax", () => {
    expect(() => df().eval("zzz + 1", {})).toThrow(/name 'zzz' is not defined/);
    expect(() => df().eval("qty = ", {})).toThrow(SyntaxError);
  });
});
