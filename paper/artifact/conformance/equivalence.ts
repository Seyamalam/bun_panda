export function mergeRowsEquivalent(
  reference: unknown,
  candidate: unknown,
  keyIndices: number[],
  cellsEqual: (left: unknown, right: unknown) => boolean,
  rowsEqual: (left: unknown[], right: unknown[]) => boolean,
): boolean {
  if (!Array.isArray(reference) || !Array.isArray(candidate) || reference.length !== candidate.length) {
    return false;
  }

  const referenceRows = reference as unknown[][];
  const candidateRows = candidate as unknown[][];
  if (referenceRows.some((row) => !Array.isArray(row)) || candidateRows.some((row) => !Array.isArray(row))) {
    return false;
  }

  const sameKey = (left: unknown[], right: unknown[]): boolean =>
    keyIndices.every((index) => cellsEqual(left[index], right[index]));

  for (let index = 0; index < referenceRows.length; index += 1) {
    if (!sameKey(referenceRows[index]!, candidateRows[index]!)) return false;
  }

  let start = 0;
  while (start < referenceRows.length) {
    let end = start + 1;
    while (end < referenceRows.length && sameKey(referenceRows[start]!, referenceRows[end]!)) {
      end += 1;
    }

    const used = new Set<number>();
    for (let leftIndex = start; leftIndex < end; leftIndex += 1) {
      let match = -1;
      for (let rightIndex = start; rightIndex < end; rightIndex += 1) {
        if (!used.has(rightIndex) && rowsEqual(referenceRows[leftIndex]!, candidateRows[rightIndex]!)) {
          match = rightIndex;
          break;
        }
      }
      if (match < 0) return false;
      used.add(match);
    }
    start = end;
  }

  return true;
}
