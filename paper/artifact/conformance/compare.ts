import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { mergeRowsEquivalent } from "./equivalence";

interface Observation {
  id: string;
  family: string;
  classification: "valid" | "boundary";
  status: "ok" | "error";
  output?: Record<string, unknown>;
  error?: { category: string; nativeType: string; message: string };
  inputPreserved: boolean;
}

interface ResultFile {
  implementation: string;
  mode?: string;
  version: string;
  runtime: string;
  results: Observation[];
}

interface ConformanceCase {
  id: string;
  operation: { name: string; args: Record<string, unknown> };
}

const ROOT = process.env.BUN_PANDA_CONFORMANCE_DIR ?? "paper/data/conformance";
const ATOL = 1e-9;
const RTOL = 1e-9;

function read(name: string): ResultFile {
  return JSON.parse(readFileSync(`${ROOT}/${name}.json`, "utf8")) as ResultFile;
}

function isTaggedNumber(value: unknown): value is { $number: string } {
  return Boolean(value && typeof value === "object" && "$number" in value);
}

function isEncodedMissing(value: unknown): boolean {
  return value === null || (isTaggedNumber(value) && value.$number === "NaN");
}

function cellsEqual(left: unknown, right: unknown): boolean {
  // pandas may represent the same missing mask with NaN/NaT/NA while
  // bun_panda uses null. Representation differences remain visible in the
  // native dtype diagnostics; the value contract compares missingness.
  if (isEncodedMissing(left) || isEncodedMissing(right)) {
    return isEncodedMissing(left) && isEncodedMissing(right);
  }
  if (typeof left === "number" && typeof right === "number") {
    return Math.abs(left - right) <= ATOL + RTOL * Math.max(Math.abs(left), Math.abs(right));
  }
  if (isTaggedNumber(left) || isTaggedNumber(right)) {
    return isTaggedNumber(left) && isTaggedNumber(right) && left.$number === right.$number;
  }
  return Object.is(left, right);
}

function arraysEqual(left: unknown, right: unknown, cellMode = false): boolean {
  if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
  for (let i = 0; i < left.length; i += 1) {
    const a = left[i];
    const b = right[i];
    if (Array.isArray(a) || Array.isArray(b)) {
      if (!arraysEqual(a, b, cellMode)) return false;
    } else if (cellMode ? !cellsEqual(a, b) : JSON.stringify(a) !== JSON.stringify(b)) {
      return false;
    }
  }
  return true;
}

function compareObservation(
  reference: Observation,
  candidate: Observation,
  conformanceCase: ConformanceCase | undefined,
): string[] {
  const categories: string[] = [];
  if (!reference.inputPreserved || !candidate.inputPreserved) categories.push("mutation");
  if (reference.status !== candidate.status) return [...categories, "status"];
  if (reference.status === "error") {
    if (reference.error?.category !== candidate.error?.category) categories.push("exception-category");
    return categories;
  }
  const left = reference.output ?? {};
  const right = candidate.output ?? {};
  if (left.kind !== right.kind) categories.push("output-kind");
  if (!arraysEqual(left.columns, right.columns)) categories.push("column-labels-or-order");
  if (!arraysEqual(left.index, right.index, true)) categories.push("index-labels-or-order");
  const mergeKeys = conformanceCase?.operation.name === "merge" &&
      Array.isArray(conformanceCase.operation.args.on)
    ? conformanceCase.operation.args.on.filter((key): key is string => typeof key === "string")
    : [];
  const columns = Array.isArray(left.columns) ? left.columns : [];
  const keyIndices = mergeKeys.map((key) => columns.indexOf(key));
  const compareMergeGroups = mergeKeys.length > 0 && keyIndices.every((index) => index >= 0);
  const dataEqual = compareMergeGroups
    ? mergeRowsEquivalent(
        left.data,
        right.data,
        keyIndices,
        cellsEqual,
        (leftRow, rightRow) => arraysEqual(leftRow, rightRow, true),
      )
    : arraysEqual(left.data, right.data, true);
  if (!dataEqual) categories.push("values-or-row-order");
  if (!arraysEqual(left.dtypeFamilies, right.dtypeFamilies)) categories.push("dtype-family");
  return categories;
}

function indexById(file: ResultFile): Map<string, Observation> {
  return new Map(file.results.map((entry) => [entry.id, entry]));
}

function comparePair(
  referenceName: string,
  reference: ResultFile,
  candidateName: string,
  candidate: ResultFile,
  casesById: Map<string, ConformanceCase>,
): { ledger: Record<string, unknown>[]; summary: Record<string, unknown> } {
  const candidateById = indexById(candidate);
  const ledger: Record<string, unknown>[] = [];
  const family = new Map<string, { total: number; passed: number; valid: number; boundary: number }>();

  for (const expected of reference.results) {
    const counts = family.get(expected.family) ?? { total: 0, passed: 0, valid: 0, boundary: 0 };
    counts.total += 1;
    counts[expected.classification] += 1;
    const actual = candidateById.get(expected.id);
    const categories = actual
      ? compareObservation(expected, actual, casesById.get(expected.id))
      : ["missing-observation"];
    if (categories.length === 0) {
      counts.passed += 1;
    } else {
      ledger.push({
        id: expected.id,
        family: expected.family,
        classification: expected.classification,
        reference: referenceName,
        candidate: candidateName,
        categories,
        referenceStatus: expected.status,
        candidateStatus: actual?.status ?? "missing",
        referenceError: expected.error,
        candidateError: actual?.error,
      });
    }
    family.set(expected.family, counts);
  }

  const total = reference.results.length;
  const passed = total - ledger.length;
  return {
    ledger,
    summary: {
      reference: referenceName,
      referenceVersion: reference.version,
      candidate: candidateName,
      candidateVersion: candidate.version,
      total,
      passed,
      mismatched: ledger.length,
      agreement: total === 0 ? null : passed / total,
      families: Object.fromEntries([...family.entries()].map(([name, counts]) => [
        name,
        { ...counts, mismatched: counts.total - counts.passed, agreement: counts.total === 0 ? null : counts.passed / counts.total },
      ])),
    },
  };
}

const pandas = read("pandas");
const typescript = read("typescript");
const wasm = read("wasm");
const adaptive = read("adaptive");
const casePayload = JSON.parse(readFileSync(`${ROOT}/cases.json`, "utf8")) as {
  cases: ConformanceCase[];
};
const casesById = new Map(casePayload.cases.map((entry) => [entry.id, entry]));

const comparisons = [
  comparePair("pandas", pandas, "bun_panda-adaptive", adaptive, casesById),
  comparePair("bun_panda-typescript", typescript, "bun_panda-wasm", wasm, casesById),
  comparePair("bun_panda-typescript", typescript, "bun_panda-adaptive", adaptive, casesById),
];

const ledger = comparisons.flatMap((comparison) => comparison.ledger);
const summary = {
  schemaVersion: "1.0.0",
  generatedAt: new Date().toISOString(),
  tolerance: { absolute: ATOL, relative: RTOL },
  nativeDtypes: "retained as diagnostics; cross-system comparison uses broad dtype families",
  mergeOrdering: "join-key sequence must match; rows within each contiguous equal-key group compare as a multiset because pandas specifies left-key order, not duplicate-pair order",
  comparisons: comparisons.map((comparison) => comparison.summary),
  mismatchLedgerEntries: ledger.length,
};

mkdirSync(ROOT, { recursive: true });
writeFileSync(`${ROOT}/summary.json`, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
writeFileSync(`${ROOT}/mismatch-ledger.json`, `${JSON.stringify({ schemaVersion: "1.0.0", entries: ledger }, null, 2)}\n`, "utf8");

for (const comparison of comparisons) {
  const row = comparison.summary as { reference: string; candidate: string; passed: number; total: number };
  console.log(`${row.reference} -> ${row.candidate}: ${row.passed}/${row.total} observations agree`);
}
console.log(`wrote ${ledger.length} categorized ledger entries under ${ROOT}`);
