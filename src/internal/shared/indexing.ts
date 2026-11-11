import { ValueError } from "../../errors";
import { Period } from "../../period";
import type { CellValue, IndexLabel } from "../../types";
import { compareCellValues } from "../../utils";

/** pandas label ordering: scalars by value, tuples lexicographically. */
export function compareIndexLabels(a: IndexLabel, b: IndexLabel): number {
  if (!Array.isArray(a) && !Array.isArray(b)) return compareCellValues(a as CellValue, b as CellValue);
  const at = Array.isArray(a) ? a : [a];
  const bt = Array.isArray(b) ? b : [b];
  for (let i = 0; i < Math.min(at.length, bt.length); i += 1) {
    const compared = compareCellValues(at[i] as CellValue, bt[i] as CellValue);
    if (compared !== 0) return compared;
  }
  return at.length - bt.length;
}

/** True when the label is a MultiIndex tuple. */
export function isIndexTuple(label: unknown): label is (string | number)[] {
  return Array.isArray(label);
}

/** Stable map key for a scalar label or tuple. */
export function indexLabelKey(label: IndexLabel | readonly (string | number)[]): string {
  return isIndexTuple(label)
    ? `tuple:${JSON.stringify(label)}`
    : `${typeof label}:${String(label)}`;
}

export function indexLabelEquals(left: IndexLabel | readonly (string | number)[], right: IndexLabel | readonly (string | number)[]): boolean {
  if (isIndexTuple(left) || isIndexTuple(right)) {
    return isIndexTuple(left) && isIndexTuple(right) &&
      left.length === right.length && left.every((v, i) => indexLabelEquals(v as IndexLabel, right[i]!));
  }
  // Period labels compare by (freq, ordinal) identity, not object identity.
  if (left instanceof Period || right instanceof Period) {
    return left instanceof Period && right instanceof Period && left.equals(right);
  }
  return left === right || (typeof left === "number" && Number.isNaN(left) && typeof right === "number" && Number.isNaN(right));
}

export function indexLabelsEqual(left: readonly (IndexLabel | readonly (string|number)[])[], right: readonly (IndexLabel | readonly (string|number)[])[]): boolean {
  return left.length === right.length && left.every((label, i) => indexLabelEquals(label, right[i]!));
}

/** Resolve scalar labels once. Duplicate source labels only permit an unchanged target. */
export function reindexPositions(source: readonly IndexLabel[], target: readonly IndexLabel[]): number[] {
  if (indexLabelsEqual(source, target)) return source.map((_, i) => i);
  const positions = new Map<string, number>();
  source.forEach((label, i) => positions.set(indexLabelKey(label), i));
  if (positions.size !== source.length) throw new ValueError("cannot reindex on an axis with duplicate labels");
  return target.map(label => positions.get(indexLabelKey(label)) ?? -1);
}

/**
 * pandas `Index.union`: deduplicated labels, sorted when every label shares a
 * comparable primitive type, otherwise first-seen order.
 */
export function unionIndexLabels(
  left: readonly IndexLabel[],
  right: readonly IndexLabel[],
  compare: (a: IndexLabel, b: IndexLabel) => number
): IndexLabel[] {
  const seen = new Set<IndexLabel>();
  const labels: IndexLabel[] = [];
  for (const label of [...left, ...right]) {
    if (!seen.has(label)) {
      seen.add(label);
      labels.push(label);
    }
  }
  const allNumbers = labels.every(l => typeof l === "number");
  const allStrings = labels.every(l => typeof l === "string");
  if (allNumbers || allStrings) labels.sort(compare);
  return labels;
}

/**
 * pandas binary-op label alignment: the union index keeps duplicates at the
 * larger side's multiplicity; rows within a shared label match positionally
 * and a single row broadcasts to N matches. Two multi-row groups of different
 * sizes raise the pandas duplicate-label ValueError.
 */
export function alignedPositions(
  left: readonly IndexLabel[],
  right: readonly IndexLabel[],
  compare: (a: IndexLabel, b: IndexLabel) => number
): { index: IndexLabel[]; leftPos: number[]; rightPos: number[] } {
  const group = (labels: readonly IndexLabel[]) => {
    const groups = new Map<IndexLabel, number[]>();
    labels.forEach((label, i) => {
      const list = groups.get(label);
      if (list) list.push(i);
      else groups.set(label, [i]);
    });
    return groups;
  };
  const leftGroups = group(left);
  const rightGroups = group(right);
  const union = unionIndexLabels(left, right, compare);
  const index: IndexLabel[] = [];
  const leftPos: number[] = [];
  const rightPos: number[] = [];
  for (const label of union) {
    const lp = leftGroups.get(label) ?? [];
    const rp = rightGroups.get(label) ?? [];
    if (lp.length > 1 && rp.length > 1 && lp.length !== rp.length) {
      throw new ValueError("cannot reindex on an axis with duplicate labels");
    }
    const n = Math.max(lp.length, rp.length);
    for (let i = 0; i < n; i += 1) {
      index.push(label);
      leftPos.push(lp.length === 0 ? -1 : lp.length === 1 ? lp[0]! : lp[i]!);
      rightPos.push(rp.length === 0 ? -1 : rp.length === 1 ? rp[0]! : rp[i]!);
    }
  }
  return { index, leftPos, rightPos };
}
