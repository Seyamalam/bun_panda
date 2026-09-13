import { expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { compareObservation, validateObservations, type Observation } from "../paper/artifact/conformance/comparison";

const frame = (data: unknown[][], dtype = "number"): Observation => ({
  id: "case-1", family: "merge", classification: "valid", status: "ok", inputPreserved: true,
  output: { kind: "frame", columns: ["key", "value"], index: [0, 1], data,
    dtypeFamilies: ["string", "number"], nativeDtypes: ["string", dtype] },
});
const merge = { id: "case-1", operation: { name: "merge", args: { on: ["key"] } } };

test("strict comparison rejects floating drift accepted by the paper contract", () => {
  const left = frame([["A", 1], ["B", 2]]);
  const right = frame([["A", 1 + 1e-12], ["B", 2]]);
  expect(compareObservation(left, right, undefined, "semantic")).toEqual([]);
  expect(compareObservation(left, right, undefined, "strict")).toContain("values-or-row-order");
});
test("strict comparison preserves missing representations, row order and native dtypes", () => {
  const left = frame([["A", null], ["A", 1]], "float64");
  const right = frame([["A", 1], ["A", { $number: "NaN" }]]);
  expect(compareObservation(left, right, merge, "semantic")).toEqual([]);
  expect(compareObservation(left, right, merge, "strict")).toEqual(["values-or-row-order", "native-dtype"]);
});
test("strict comparison checks exception types and messages", () => {
  const left: Observation = { ...frame([]), status: "error", error: { category: "missing-label", nativeType: "KeyError", message: "x" } };
  const right: Observation = { ...left, error: { category: "missing-label", nativeType: "Error", message: "Column x not found" } };
  expect(compareObservation(left, right, undefined, "semantic")).toEqual([]);
  expect(compareObservation(left, right, undefined, "strict")).toEqual(["exception-type", "exception-message"]);
});
test("incomplete, duplicate, extra and empty observations cannot produce a green report", () => {
  expect(() => validateObservations([], [], "empty")).toThrow();
  expect(() => validateObservations(["case-1"], [], "missing")).toThrow();
  expect(() => validateObservations(["case-1"], [frame([]), frame([])], "duplicate")).toThrow();
  expect(() => validateObservations(["case-2"], [frame([])], "unexpected")).toThrow();
  expect(() => validateObservations(["case-1"], [frame([])], "valid")).not.toThrow();
});

test("comparison command fails on a mismatch and preserves the ledger in report-only mode", () => {
  const directory = mkdtempSync(join(tmpdir(), "pandas-comparison-"));
  try {
    writeFileSync(join(directory,"cases.json"),JSON.stringify({cases:[merge]}));
    for (const implementation of ["pandas","typescript","wasm","adaptive"]) {
      writeFileSync(join(directory,`${implementation}.json`),JSON.stringify({implementation,version:"test",
        results:[frame([["A",implementation === "pandas" ? 99 : 1],["B",2]])]}));
    }
    const run = (args: string[]) => Bun.spawnSync([process.execPath,"paper/artifact/conformance/compare.ts",...args], {
      env:{...process.env,BUN_PANDA_CONFORMANCE_DIR:directory},stdout:"pipe",stderr:"pipe",
    });
    expect(run([]).exitCode).toBe(1);
    expect(run(["--report-only"]).exitCode).toBe(0);
    const ledger = require(join(directory,"mismatch-ledger.json"));
    expect(ledger.entries).toHaveLength(1);
    expect(ledger.entries[0].categories).toContain("values-or-row-order");
  } finally {
    rmSync(directory,{recursive:true,force:true});
  }
});
