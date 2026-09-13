import { mergeRowsEquivalent } from "./equivalence";

export interface Observation {
  id: string;
  family: string;
  classification: "valid" | "boundary";
  status: "ok" | "error";
  output?: Record<string, unknown>;
  error?: { category: string; nativeType: string; message: string };
  inputPreserved: boolean;
}

export interface ConformanceCase {
  id: string;
  operation: { name: string; args: Record<string, unknown> };
}

const ATOL = 1e-9;
const RTOL = 1e-9;

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

function arraysEqual(left: unknown, right: unknown, equal: (a: unknown, b: unknown) => boolean = (a, b) => JSON.stringify(a) === JSON.stringify(b)): boolean {
  if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
  for (let i = 0; i < left.length; i += 1) {
    const a = left[i];
    const b = right[i];
    if (Array.isArray(a) || Array.isArray(b)) {
      if (!arraysEqual(a, b, equal)) return false;
    } else if (!equal(a, b)) {
      return false;
    }
  }
  return true;
}

export function compareObservation(
  reference: Observation,
  candidate: Observation,
  conformanceCase: ConformanceCase | undefined,
  mode: "semantic" | "strict" = "semantic",
): string[] {
  const categories: string[] = [];
  const equalCells = mode === "strict"
    ? (a: unknown, b: unknown) => typeof a === "number" && typeof b === "number" ? Object.is(a,b) : JSON.stringify(a) === JSON.stringify(b)
    : cellsEqual;
  if (!reference.inputPreserved || !candidate.inputPreserved) categories.push("mutation");
  if (reference.status !== candidate.status) return [...categories, "status"];
  if (reference.status === "error") {
    if (reference.error?.category !== candidate.error?.category) categories.push("exception-category");
    if (mode === "strict") {
      if (reference.error?.nativeType !== candidate.error?.nativeType) categories.push("exception-type");
      if (reference.error?.message !== candidate.error?.message) categories.push("exception-message");
    }
    return categories;
  }
  const left = reference.output ?? {};
  const right = candidate.output ?? {};
  if (left.kind !== right.kind) categories.push("output-kind");
  if (!arraysEqual(left.columns, right.columns)) categories.push("column-labels-or-order");
  if (!arraysEqual(left.index, right.index, equalCells)) categories.push("index-labels-or-order");
  const mergeKeys = conformanceCase?.operation.name === "merge" &&
      Array.isArray(conformanceCase.operation.args.on)
    ? conformanceCase.operation.args.on.filter((key): key is string => typeof key === "string")
    : [];
  const columns = Array.isArray(left.columns) ? left.columns : [];
  const keyIndices = mergeKeys.map((key) => columns.indexOf(key));
  const compareMergeGroups = mode === "semantic" && mergeKeys.length > 0 && keyIndices.every((index) => index >= 0);
  const dataEqual = compareMergeGroups
    ? mergeRowsEquivalent(
        left.data,
        right.data,
        keyIndices,
        equalCells,
        (leftRow, rightRow) => arraysEqual(leftRow, rightRow, equalCells),
      )
    : arraysEqual(left.data, right.data, equalCells);
  if (!dataEqual) categories.push("values-or-row-order");
  if (!arraysEqual(left.dtypeFamilies, right.dtypeFamilies)) categories.push("dtype-family");
  if (mode === "strict" && !arraysEqual(left.nativeDtypes, right.nativeDtypes)) categories.push("native-dtype");
  return categories;
}

export function validateObservations(caseIds: string[], observations: Observation[], label: string): void {
  const expected = new Set(caseIds);
  if (expected.size === 0 || expected.size !== caseIds.length) throw new Error("Corpus must have unique, nonempty case IDs");
  const seen = new Set<string>();
  for (const observation of observations) {
    if (!expected.has(observation.id)) throw new Error(`${label}: unexpected observation ${observation.id}`);
    if (seen.has(observation.id)) throw new Error(`${label}: duplicate observation ${observation.id}`);
    seen.add(observation.id);
  }
  if (seen.size !== expected.size) throw new Error(`${label}: missing observations (${seen.size}/${expected.size})`);
}
