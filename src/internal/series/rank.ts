// pandas-style rank computation over raw value arrays.
import type { CellValue } from "../../types";
import { isMissing } from "../../utils";

export interface RankOptions {
  method?: "average" | "min" | "max" | "first" | "dense";
  ascending?: boolean;
  na_option?: "keep" | "bottom" | "top";
  pct?: boolean;
}

interface ScoredEntry {
  position: number;
  value: CellValue;
}

function assignRanks(
  entries: ScoredEntry[],
  target: (number | null)[],
  m: "average" | "min" | "max" | "first" | "dense",
  offset: number
): void {
  let i = 0;
  let denseRank = 0;
  while (i < entries.length) {
    let j = i;
    while (j + 1 < entries.length && compareRankValues(entries[j + 1]!.value, entries[i]!.value) === 0) {
      j += 1;
    }
    denseRank += 1;
    // Ranks are 1-based over non-null entries.
    const first = offset + i + 1;
    const last = offset + j + 1;
    let rankValue: number;
    switch (m) {
      case "min":
        rankValue = first;
        break;
      case "max":
        rankValue = last;
        break;
      case "first":
        rankValue = first;
        for (let k = i; k <= j; k += 1) {
          target[entries[k]!.position] = offset + k + 1;
        }
        i = j + 1;
        continue;
      case "dense":
        rankValue = denseRank;
        break;
      default:
        rankValue = (first + last) / 2;
    }
    for (let k = i; k <= j; k += 1) {
      target[entries[k]!.position] = rankValue;
    }
    i = j + 1;
  }
}

export function computeRank(
  values: CellValue[],
  options: RankOptions = {}
): (number | null)[] {
  const method = options.method ?? "average";
  const ascending = options.ascending !== false;
  const naOption = options.na_option ?? "keep";
  if (!["average", "min", "max", "first", "dense"].includes(method)) {
    throw new Error("rank method must be average, min, max, first, or dense.");
  }
  if (!["keep", "top", "bottom"].includes(naOption)) {
    throw new Error("rank na_option must be keep, top, or bottom.");
  }

  const scored: ScoredEntry[] = [];
  const missingPositions: number[] = [];
  values.forEach((value, position) => {
    if (!isMissing(value)) {
      scored.push({ position, value });
    } else {
      missingPositions.push(position);
    }
  });
  scored.sort((a, b) => (ascending ? 1 : -1) * compareRankValues(a.value, b.value));

  const ranks: (number | null)[] = new Array(values.length).fill(null);
  assignRanks(scored, ranks, method, 0);
  if (missingPositions.length === 0 || naOption === "keep") return percentageRanks(ranks, method, options.pct ?? false);

  if (naOption === "top") {
    const offset = method === "dense" ? 1 : missingPositions.length;
    for (const entry of scored) ranks[entry.position] = (ranks[entry.position] as number) + offset;
    for (let i = 0; i < missingPositions.length; i += 1) {
      ranks[missingPositions[i]!] = missingRank(method, 0, missingPositions.length, i);
    }
    return percentageRanks(ranks, method, options.pct ?? false);
  }

  const denseGroups = method === "dense"
    ? ranks.reduce<number>((max, rank) => rank === null ? max : Math.max(max, rank), 0)
    : 0;
  const base = method === "dense" ? denseGroups : scored.length;
  for (let i = 0; i < missingPositions.length; i += 1) {
    ranks[missingPositions[i]!] = missingRank(method, base, missingPositions.length, i);
  }
  return percentageRanks(ranks, method, options.pct ?? false);
}

function missingRank(
  method: "average" | "min" | "max" | "first" | "dense",
  base: number,
  missingCount: number,
  position: number
): number {
  if (method === "first") return base + position + 1;
  if (method === "max") return base + missingCount;
  if (method === "average") return base + (missingCount + 1) / 2;
  return base + 1;
}

function percentageRanks(ranks: (number | null)[], method: string, pct: boolean): (number | null)[] {
  if (!pct) return ranks;
  const present = ranks.filter((rank): rank is number => rank !== null);
  const denominator = method === "dense" ? present.reduce((max, rank) => Math.max(max, rank), 0) : present.length;
  return ranks.map(rank => rank === null ? null : rank / denominator);
}

function compareRankValues(left: CellValue, right: CellValue): number {
  if (left === right) return 0;
  if ((typeof left === "number" || typeof left === "boolean") && (typeof right === "number" || typeof right === "boolean")) {
    return Number(left) < Number(right) ? -1 : Number(left) > Number(right) ? 1 : 0;
  }
  if (left instanceof Date && right instanceof Date) return left.getTime() - right.getTime();
  if (typeof left === "string" && typeof right === "string") {
    const a = Array.from(left, char => char.codePointAt(0)!);
    const b = Array.from(right, char => char.codePointAt(0)!);
    for (let i = 0; i < Math.min(a.length, b.length); i += 1) {
      if (a[i] !== b[i]) return a[i]! - b[i]!;
    }
    return a.length - b.length;
  }
  throw new TypeError("rank values must be mutually comparable");
}
