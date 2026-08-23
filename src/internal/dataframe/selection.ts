// Duplicate-detection and wasm sort/filter position helpers shared by
// DataFrame's ordering methods. Pure functions over (rows, columns).
import { keyForColumns } from "./keys";
import { buildColumnStore } from "../../wasm/columns";
import { wasmArgsortF64, wasmFilterIndices } from "../../wasm/kernel";
import type { Row } from "../../types";

export type DropDuplicatesKeep = "first" | "last" | false;

export function duplicateKeepFlags(
  rows: Row[],
  columns: string[],
  keep: DropDuplicatesKeep
): boolean[] {
  const include = new Array(rows.length).fill(false);

  if (keep === false) {
    const counts = new Map<string, number>();
    for (const row of rows) {
      const key = keyForColumns(row, columns);
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    for (let i = 0; i < rows.length; i += 1) {
      include[i] = (counts.get(keyForColumns(rows[i]!, columns)) ?? 0) === 1;
    }
  } else if (keep === "last") {
    const seen = new Set<string>();
    for (let i = rows.length - 1; i >= 0; i -= 1) {
      const key = keyForColumns(rows[i]!, columns);
      if (!seen.has(key)) {
        seen.add(key);
        include[i] = true;
      }
    }
  } else {
    const seen = new Set<string>();
    for (let i = 0; i < rows.length; i += 1) {
      const key = keyForColumns(rows[i]!, columns);
      if (!seen.has(key)) {
        seen.add(key);
        include[i] = true;
      }
    }
  }

  return include;
}

/** True when every cell in the column is null/undefined or a finite number. */
export function isNumericColumn(rows: Row[], column: string): boolean {
  for (let i = 0; i < rows.length; i += 1) {
    const value = rows[i]![column];
    if (
      value !== null &&
      value !== undefined &&
      (typeof value !== "number" || !Number.isFinite(value))
    ) {
      return false;
    }
  }
  return true;
}

function wasmEnabled(): boolean {
  const env = (process as unknown as { env?: Record<string, string> }).env;
  return env?.BUN_PANDA_WASM !== "0";
}

/** Stable argsort positions for a numeric column, or null when unsupported. */
export function wasmSortPositions(
  rows: Row[],
  column: string,
  ascending: boolean
): Int32Array | null {
  if (!wasmEnabled()) return null;
  const store = buildColumnStore(rows, [column]);
  const entry = store.columns.get(column);
  if (!entry || entry.kind !== "f64") return null;
  return wasmArgsortF64(entry.values, ascending);
}

/** Positions kept by a boolean mask, or null when the kernel is unavailable. */
export function wasmFilterPositions(mask: boolean[]): Int32Array | null {
  if (!wasmEnabled()) return null;
  const bytes = Uint8Array.from(mask, (value) => (value ? 1 : 0));
  return wasmFilterIndices(bytes);
}
