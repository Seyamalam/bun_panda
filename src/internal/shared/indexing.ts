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
