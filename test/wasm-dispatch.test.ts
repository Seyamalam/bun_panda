import { afterEach, describe, expect, test } from "bun:test";
import { chooseExecutionPath } from "../src/wasm/dispatch";

const priorMode = process.env.BUN_PANDA_WASM;

afterEach(() => {
  if (priorMode === undefined) delete process.env.BUN_PANDA_WASM;
  else process.env.BUN_PANDA_WASM = priorMode;
});

describe("adaptive Wasm dispatch", () => {
  test("explicit modes override calibration", () => {
    process.env.BUN_PANDA_WASM = "0";
    expect(
      chooseExecutionPath({ operation: "sort", rowCount: 100_000 }).path
    ).toBe("typescript");

    process.env.BUN_PANDA_WASM = "1";
    expect(
      chooseExecutionPath({ operation: "filter-mask", rowCount: 10_000 }).path
    ).toBe("wasm");
  });

  test("forced Wasm does not bypass semantic eligibility", () => {
    process.env.BUN_PANDA_WASM = "1";
    expect(
      chooseExecutionPath({
        operation: "groupby-fused",
        rowCount: 100_000,
        planCount: 1,
        dropna: false,
        typedColumnsReused: true,
      })
    ).toMatchObject({
      path: "typescript",
      reason: "groupby-dropna-unsupported",
    });
  });

  test("full sort uses only the measured range", () => {
    delete process.env.BUN_PANDA_WASM;
    expect(chooseExecutionPath({ operation: "sort", rowCount: 9_999 })).toMatchObject({
      path: "typescript",
      reason: "below-measured-sort-range",
    });
    expect(chooseExecutionPath({ operation: "sort", rowCount: 10_000 })).toMatchObject({
      path: "wasm",
      reason: "measured-full-sort-win",
    });
  });

  test("top-k uses Wasm only in the fresh-process measured range", () => {
    delete process.env.BUN_PANDA_WASM;
    expect(
      chooseExecutionPath({ operation: "sort", rowCount: 250_000, limit: 1_000 })
    ).toMatchObject({
      path: "typescript",
      reason: "bounded-selection-preferred",
    });
    expect(
      chooseExecutionPath({ operation: "sort", rowCount: 100_000, limit: 1_000 })
    ).toMatchObject({ path: "typescript", reason: "bounded-selection-preferred" });
    expect(
      chooseExecutionPath({ operation: "sort", rowCount: 10_000, limit: 1_000 })
    ).toMatchObject({ path: "wasm", reason: "measured-top-k-win" });
  });

  test("measured regressions remain in TypeScript by default", () => {
    delete process.env.BUN_PANDA_WASM;
    expect(
      chooseExecutionPath({ operation: "filter-mask", rowCount: 250_000 })
    ).toMatchObject({
      path: "typescript",
      reason: "measured-filter-regression",
    });
    expect(
      chooseExecutionPath({
        operation: "groupby-fused",
        rowCount: 250_000,
        planCount: 4,
        dropna: true,
        typedColumnsReused: false,
      })
    ).toMatchObject({
      path: "typescript",
      reason: "groupby-awaiting-reuse-evidence",
    });
    expect(
      chooseExecutionPath({
        operation: "groupby-fused",
        rowCount: 250_000,
        planCount: 4,
        dropna: true,
        typedColumnsReused: true,
      })
    ).toMatchObject({
      path: "wasm",
      reason: "groupby-enabled-by-calibration",
    });
  });
});
