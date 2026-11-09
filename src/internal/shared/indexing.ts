import { ValueError } from "../../errors";
import type { IndexLabel } from "../../types";

export function indexLabelEquals(left: IndexLabel, right: IndexLabel): boolean {
  return left === right || (typeof left === "number" && Number.isNaN(left) && typeof right === "number" && Number.isNaN(right));
}

export function indexLabelsEqual(left: readonly IndexLabel[], right: readonly IndexLabel[]): boolean {
  return left.length === right.length && left.every((label, i) => indexLabelEquals(label, right[i]!));
}

/** Resolve scalar labels once. Duplicate source labels only permit an unchanged target. */
export function reindexPositions(source: readonly IndexLabel[], target: readonly IndexLabel[]): number[] {
  if (indexLabelsEqual(source, target)) return source.map((_, i) => i);
  const positions = new Map<IndexLabel, number>();
  source.forEach((label, i) => positions.set(label, i));
  if (positions.size !== source.length) throw new ValueError("cannot reindex on an axis with duplicate labels");
  return target.map(label => positions.get(label) ?? -1);
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
