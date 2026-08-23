// pandas-style rank computation over raw value arrays.
import type { CellValue } from "../../types";
import { isMissing } from "../../utils";

export interface RankOptions {
  method?: "average" | "min" | "max" | "first" | "dense";
  ascending?: boolean;
  na_option?: "keep" | "bottom" | "top";
}

interface ScoredEntry {
  position: number;
  num: number;
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
    while (j + 1 < entries.length && entries[j + 1]!.num === entries[i]!.num) {
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

  const scored: ScoredEntry[] = [];
  values.forEach((value, position) => {
    const num = Number(value);
    if (!isMissing(value) && Number.isFinite(num)) {
      scored.push({ position, num });
    }
  });
  scored.sort((a, b) => (ascending ? a.num - b.num : b.num - a.num));

  const ranks: (number | null)[] = new Array(values.length).fill(null);
  if (naOption === "top") {
    // Non-null values start after all null positions.
    assignRanks(scored, ranks, method, values.length - scored.length);
  } else {
    assignRanks(scored, ranks, method, 0);
  }
  return ranks;
}
