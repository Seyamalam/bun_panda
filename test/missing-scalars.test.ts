import { expect, test } from "bun:test";
import { NA, NaT, Series, DataFrame, Categorical, isna, notna, factorize } from "../src/index";
import type { CellValue } from "../src/types";

const absent: CellValue[] = [null, undefined, NaN, NA, NaT, new Date(NaN)];

test("all missing scalar forms are recognized without treating infinities as missing", () => {
  const values: CellValue[] = [...absent,Infinity,-Infinity,0,false,""];
  const expected = [true,true,true,true,true,true,false,false,false,false,false];
  expect(isna(values)).toEqual(expected);
  expect(notna(values)).toEqual(expected.map(x=>!x));
  expect(new Series(values).isna().to_list()).toEqual(expected);
});

test("Series counts, fills, drops and cumulative scans use the same missing mask", () => {
  const values: CellValue[] = [1,...absent,3];
  const series = new Series(values);
  expect(series.count()).toBe(2);
  expect(series.dropna().to_list()).toEqual([1,3]);
  expect(series.fillna(2).to_list()).toEqual([1,2,2,2,2,2,2,3]);
  expect(series.ffill().to_list()).toEqual([1,1,1,1,1,1,1,3]);
  expect(series.bfill().to_list()).toEqual([1,3,3,3,3,3,3,3]);
  expect<CellValue[]>(series.cumsum().to_list()).toEqual([1,null,null,null,null,null,null,4]);
});

test("DataFrame missing operations recognize exported sentinels", () => {
  const frame = new DataFrame({x:[1,...absent,3]});
  expect(frame.count()).toEqual({x:2});
  expect(frame.dropna().get("x").to_list()).toEqual([1,3]);
  expect(frame.fillna(0).get("x").to_list()).toEqual([1,0,0,0,0,0,0,3]);
});

test("factorization and categorical inference exclude every missing marker", () => {
  const values: CellValue[] = ["a",...absent,"b"];
  expect(factorize(values)).toEqual({codes:[0,-1,-1,-1,-1,-1,-1,1],uniques:["a","b"]});
  const cat = new Categorical(values);
  expect(cat.categories).toEqual(["a","b"]);
  expect(cat.codes).toEqual([0,-1,-1,-1,-1,-1,-1,1]);
});
