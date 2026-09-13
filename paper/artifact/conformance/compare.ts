import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { compareObservation, validateObservations, type Observation, type ConformanceCase } from "./comparison";

interface ResultFile {
  implementation: string;
  mode?: string;
  version: string;
  runtime: string;
  results: Observation[];
}

const ROOT = process.env.BUN_PANDA_CONFORMANCE_DIR ?? "paper/data/conformance";
const mode = process.argv.includes("--strict") ? "strict" : "semantic";
const ATOL = mode === "strict" ? 0 : 1e-9;
const RTOL = ATOL;

function read(name: string): ResultFile {
  return JSON.parse(readFileSync(`${ROOT}/${name}.json`, "utf8")) as ResultFile;
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
      ? compareObservation(expected, actual, casesById.get(expected.id), mode)
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

for (const file of [pandas, typescript, wasm, adaptive]) {
  validateObservations(casePayload.cases.map((entry) => entry.id), file.results, `${file.implementation}/${file.mode ?? "default"}`);
}

const comparisons = [
  comparePair("pandas", pandas, "bun_panda-adaptive", adaptive, casesById),
  comparePair("bun_panda-typescript", typescript, "bun_panda-wasm", wasm, casesById),
  comparePair("bun_panda-typescript", typescript, "bun_panda-adaptive", adaptive, casesById),
];

const ledger = comparisons.flatMap((comparison) => comparison.ledger);
const summary = {
  schemaVersion: "1.1.0",
  comparisonMode: mode,
  generatedAt: new Date().toISOString(),
  tolerance: { absolute: ATOL, relative: RTOL },
  nativeDtypes: mode === "strict" ? "exact native dtype names" : "retained as diagnostics; cross-system comparison uses broad dtype families",
  mergeOrdering: mode === "strict" ? "exact row order" : "join-key sequence must match; rows within each contiguous equal-key group compare as a multiset because pandas specifies left-key order, not duplicate-pair order",
  comparisons: comparisons.map((comparison) => comparison.summary),
  mismatchLedgerEntries: ledger.length,
};

mkdirSync(ROOT, { recursive: true });
writeFileSync(`${ROOT}/${mode === "strict" ? "summary-strict" : "summary"}.json`, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
writeFileSync(`${ROOT}/${mode === "strict" ? "mismatch-ledger-strict" : "mismatch-ledger"}.json`, `${JSON.stringify({ schemaVersion: "1.0.0", entries: ledger }, null, 2)}\n`, "utf8");

for (const comparison of comparisons) {
  const row = comparison.summary as { reference: string; candidate: string; passed: number; total: number };
  console.log(`${row.reference} -> ${row.candidate}: ${row.passed}/${row.total} observations agree`);
}
console.log(`wrote ${ledger.length} categorized ledger entries under ${ROOT}`);

if (ledger.length > 0 && !process.argv.includes("--report-only")) process.exitCode = 1;
