import { describe, expect, test } from "bun:test";
import { BunPandaValidationError, DataFrame, read_csv_sync } from "../src/index";
import { assertRowsShape } from "../src/internal/dataframe/core";

describe("BunPanda validation (overall feedback)", () => {
  test("BunPandaValidationError is an Error with the right name", () => {
    const err = new BunPandaValidationError("nope");
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe("BunPandaValidationError");
  });

  test("assertRowsShape rejects mismatched index/row counts", () => {
    expect(() => assertRowsShape([{}], [])).toThrow(BunPandaValidationError);
    expect(() => assertRowsShape([{}], ["a"])).not.toThrow();
  });

  test("DataFrame constructor surfaces mismatched index as BunPandaValidationError", () => {
    expect(
      () => new DataFrame([{ a: 1 }, { a: 2 }], { index: ["only-one"] })
    ).toThrow(BunPandaValidationError);
  });

  test("DataFrame.createInternal/ from_normalized reject bad index length", () => {
    // createInternal is private — reach it via the illegal-index constructor path above,
    // and via from_normalized
    expect(() =>
      DataFrame.from_normalized([{ a: 1 }], ["a"], ["x", "extra"])
    ).toThrow(BunPandaValidationError);
  });

  test("read_csv_sync rejects empty file path with BunPandaValidationError", () => {
    expect(() => read_csv_sync("")).toThrow(BunPandaValidationError);
    expect(() => read_csv_sync("   ")).toThrow(BunPandaValidationError);
  });
});
