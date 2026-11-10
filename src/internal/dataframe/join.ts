// pandas-style join orchestration: key both frames comparably, merge,
// then rebuild output columns/index. Pure over frame accessors passed
// in as a small context so it stays testable without class internals.
import { DataFrame } from "../../dataframe";
import { ValueError } from "../../errors";
import type { DType, IndexLabel, Row } from "../../types";
import { range } from "../../utils";

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
  // pandas raises when non-key columns overlap and no suffix is given.
  const overlapping = ctx.columns().filter(
    (column) => column !== options.on && right.columns.includes(column)
  );
  if (options.suffixes === undefined && options.on === undefined && overlapping.length > 0) {
    throw new ValueError(`columns overlap but no suffix specified: ${overlapping.map((c) => `'${c}'`).join(", ")}`);
  }
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
    const joinedDtypes = joined.dtypes() as Record<string, DType>;
    const fixedDtypes: Record<string, DType> = {};
    fixedDtypes[options.on as string] = joinedDtypes[leftOnName] ?? "object";
    for (const column of keptNoOn) {
      fixedDtypes[column] = joinedDtypes[column] ?? "object";
    }
    const fixed = DataFrame.createInternal(rows, columns, range(rows.length), {}, fixedDtypes);
    if (how === "left") {
      // Keep the merge result's dtypes, not the source frame's.
      return DataFrame.createInternal(
        fixed.to_records(),
        columns,
        [...ctx.index()],
        {},
        fixedDtypes
      );
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
    // Preserve the merge result's dtypes across the index restore.
    return DataFrame.createInternal(
      result.to_records(),
      result.columns as string[],
      labels,
      {},
      result.dtypes() as Record<string, DType>
    );
  }
  return result;
}
