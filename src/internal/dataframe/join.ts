// pandas-style join orchestration: key both frames comparably, merge,
// then rebuild output columns/index. Pure over frame accessors passed
// in as a small context so it stays testable without class internals.
import { DataFrame } from "../../dataframe";
import type { IndexLabel, Row } from "../../types";

export interface JoinOptions {
  on?: string;
  how?: "inner" | "left" | "right" | "outer";
  suffixes?: [string, string];
}

export interface JoinContext {
  rows(): Row[];
  columns(): string[];
  index(): IndexLabel[];
  assertColumnExists(column: string): void;
  withKeyColumn(keyColumn: string, sourceColumn?: string, labels?: IndexLabel[]): DataFrame;
  withRows(
    rows: Row[],
    index?: IndexLabel[],
    columns?: string[],
    rowsAreNormalized?: boolean
  ): DataFrame;
}

export function performJoin(
  ctx: JoinContext,
  right: DataFrame,
  options: JoinOptions = {}
): DataFrame {
  const how = options.how ?? "left";
  const suffixes = options.suffixes ?? ["_x", "_y"];

  // Both sides must be keyed on comparable values: when joining on a
  // column, both frames take that column; otherwise index labels.
  let leftKeyed: DataFrame;
  let rightKeyed: DataFrame;

  if (options.on !== undefined) {
    ctx.assertColumnExists(options.on);
    const rightCols = right.columns as string[];
    if (!rightCols.includes(options.on)) {
      throw new Error(`Column '${options.on}' does not exist.`);
    }
    leftKeyed = ctx.withKeyColumn("__join_key__", options.on);
    rightKeyed = (right as unknown as { withKeyColumn: JoinContext["withKeyColumn"] }).withKeyColumn(
      "__join_key__",
      options.on
    );
  } else {
    leftKeyed = ctx.withKeyColumn("__join_key__");
    rightKeyed = (right as unknown as { withKeyColumn: JoinContext["withKeyColumn"] }).withKeyColumn(
      "__join_key__",
      undefined,
      [...right.index]
    );
  }

  const joined = leftKeyed.merge(rightKeyed, {
    on: "__join_key__",
    how,
    suffixes,
  });

  // When joining `on` a column that exists on both sides, the merge
  // suffixes it (k_x/k_y). Build the output by keeping one copy under
  // the original name and dropping suffixed duplicates.
  if (options.on !== undefined && !joined.columns.includes(options.on)) {
    const leftOnName = `${options.on}${suffixes[0]}`;
    const source = joined.select([leftOnName]).to_records();
    const keptNoOn = joined.columns.filter(
      (column) =>
        column !== "__join_key__" &&
        !column.startsWith("__join_key__") &&
        column !== `${options.on}${suffixes[0]}` &&
        column !== `${options.on}${suffixes[1]}`
    );
    const rows = joined
      .select(keptNoOn)
      .to_records()
      .map((row, i) => {
        const next: Row = {};
        const onName = options.on;
        if (onName !== undefined) {
          next[onName] = source[i]![leftOnName];
        }
        for (const [key, value] of Object.entries(row)) {
          next[key] = value;
        }
        return next;
      });
    const columns = [options.on as string, ...keptNoOn];
    const fixed = new DataFrame(rows, { columns });
    if (how === "left") {
      return ctx.withRows(fixed.to_records(), [...ctx.index()], fixed.columns as string[], true);
    }
    return fixed;
  }

  const kept: string[] = [];
  for (const column of joined.columns) {
    if (column === "__join_key__" || column.startsWith("__join_key__")) {
      continue;
    }
    kept.push(column);
  }
  const result = joined.select(kept);

  // Restore index labels from the join key where possible: when no
  // `on` column was given, the key IS the index label.
  if (options.on === undefined) {
    const labels = joined
      .select(["__join_key__"])
      .to_records()
      .map((row) => row.__join_key__ as IndexLabel);
    return ctx.withRows(result.to_records(), labels, result.columns as string[], true);
  }
  return result;
}
