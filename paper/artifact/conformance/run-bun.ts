import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { DataFrame } from "../../../src/dataframe";
import type { CellValue, Row } from "../../../src/types";

type Mode = "typescript" | "wasm" | "adaptive";
type TaggedNumber = { $number: "NaN" | "Infinity" | "-Infinity" };
type EncodedCell = CellValue | TaggedNumber;

interface FrameSpec {
  columns: string[];
  index: (string | number)[];
  rows: Record<string, unknown>[];
}

interface CaseSpec {
  id: string;
  family: string;
  classification: "valid" | "boundary";
  input: FrameSpec;
  right?: FrameSpec;
  operation: { name: string; args: Record<string, unknown> };
}

function argument(name: string, fallback: string): string {
  const position = process.argv.indexOf(name);
  return position >= 0 ? process.argv[position + 1] ?? fallback : fallback;
}

const casesPath = argument("--cases", "paper/data/conformance/cases.json");
const mode = argument("--mode", "adaptive") as Mode;
const outputPath = argument("--out", `paper/data/conformance/${mode}.json`);

if (!(["typescript", "wasm", "adaptive"] as string[]).includes(mode)) {
  throw new Error(`unknown mode '${mode}'`);
}
if (mode === "typescript") process.env.BUN_PANDA_WASM = "0";
else if (mode === "wasm") process.env.BUN_PANDA_WASM = "1";
else delete process.env.BUN_PANDA_WASM;

function decodeCell(value: unknown): CellValue {
  if (value && typeof value === "object" && "$number" in value) {
    const tag = (value as TaggedNumber).$number;
    if (tag === "NaN") return Number.NaN;
    if (tag === "Infinity") return Number.POSITIVE_INFINITY;
    return Number.NEGATIVE_INFINITY;
  }
  return value as CellValue;
}

function encodeCell(value: CellValue): EncodedCell {
  if (typeof value === "number") {
    if (Number.isNaN(value)) return { $number: "NaN" };
    if (value === Number.POSITIVE_INFINITY) return { $number: "Infinity" };
    if (value === Number.NEGATIVE_INFINITY) return { $number: "-Infinity" };
    if (Object.is(value, -0)) return 0;
  }
  if (value instanceof Date) return value.toISOString();
  return value ?? null;
}

function frameFromSpec(spec: FrameSpec): DataFrame {
  const rows: Row[] = spec.rows.map((source) => {
    const row: Row = {};
    for (const column of spec.columns) row[column] = decodeCell(source[column]);
    return row;
  });
  return new DataFrame(rows, { columns: spec.columns, index: spec.index });
}

function dtypeFamily(dtype: string): string {
  if (dtype === "number") return "number";
  if (dtype === "boolean") return "boolean";
  if (dtype === "string") return "string";
  if (dtype === "date") return "datetime";
  if (dtype === "unknown") return "unknown";
  return "mixed";
}

function canonicalFrame(frame: DataFrame): Record<string, unknown> {
  const columns = frame.columns;
  const nativeDtypes = frame.dtypes();
  return {
    kind: "frame",
    columns,
    index: frame.index.map((value) => encodeCell(value)),
    data: frame.to_records().map((row) =>
      columns.map((column) => encodeCell(row[column]))
    ),
    dtypeFamilies: columns.map((column) => dtypeFamily(nativeDtypes[column] ?? "unknown")),
    nativeDtypes: columns.map((column) => nativeDtypes[column] ?? "unknown"),
  };
}

function classifyError(error: unknown): string {
  const message = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();
  if (message.includes("does not exist") || message.includes("not found") || message.includes("unknown column")) {
    return "missing-label";
  }
  if (message.includes("not supported") || message.includes("unsupported")) return "unsupported";
  if (message.includes("must") || message.includes("invalid") || message.includes("requires")) {
    return "invalid-argument";
  }
  return error instanceof TypeError ? "type-error" : "runtime-error";
}

function execute(frame: DataFrame, right: DataFrame | undefined, operation: CaseSpec["operation"]): DataFrame {
  const args = operation.args;
  if (operation.name === "sort_values") {
    return frame.sort_values(
      args.by as string[],
      (args.ascending ?? true) as boolean,
      undefined,
      (args.na_position ?? "last") as "first" | "last"
    );
  }
  if (operation.name === "dropna") return frame.dropna(args.subset as string[]);
  if (operation.name === "fillna") return frame.fillna(args.value as CellValue | Record<string, CellValue>);
  if (operation.name === "drop_duplicates") {
    return frame.drop_duplicates(
      args.subset as string[],
      args.keep as "first" | "last" | false,
      (args.ignore_index ?? false) as boolean
    );
  }
  if (operation.name === "shift") return frame.shift(args.periods as number);
  if (operation.name === "diff") return frame.diff(args.periods as number);
  if (operation.name === "rank") {
    return frame.rank(args as {
      method?: "average" | "min" | "max" | "first" | "dense";
      ascending?: boolean;
      na_option?: "keep" | "top" | "bottom";
      pct?: boolean;
    });
  }
  if (operation.name === "groupby_agg") {
    return frame.groupby(args.by as string[], {
      dropna: (args.dropna ?? true) as boolean,
      sort: (args.sort ?? true) as boolean,
      as_index: false,
    }).agg(args.spec as Record<string, "sum" | "mean" | "min" | "max" | "count">);
  }
  if (operation.name === "value_counts") {
    return frame.value_counts(args as {
      subset?: string[];
      normalize?: boolean;
      dropna?: boolean;
      sort?: boolean;
      ascending?: boolean;
    });
  }
  if (operation.name === "merge") {
    if (!right) throw new Error("merge requires a right input");
    return frame.merge(right, {
      on: args.on as string[],
      how: (args.how ?? "inner") as "inner" | "left" | "right" | "outer",
    });
  }
  throw new Error(`unsupported operation '${operation.name}'`);
}

const corpus = JSON.parse(readFileSync(casesPath, "utf8")) as { cases: CaseSpec[] };
const startedAt = new Date().toISOString();
const results = corpus.cases.map((testCase) => {
  const frame = frameFromSpec(testCase.input);
  const right = testCase.right ? frameFromSpec(testCase.right) : undefined;
  const before = JSON.stringify(canonicalFrame(frame));
  const rightBefore = right ? JSON.stringify(canonicalFrame(right)) : null;
  try {
    const output = execute(frame, right, testCase.operation);
    return {
      id: testCase.id,
      family: testCase.family,
      classification: testCase.classification,
      status: "ok",
      output: canonicalFrame(output),
      inputPreserved:
        before === JSON.stringify(canonicalFrame(frame)) &&
        rightBefore === (right ? JSON.stringify(canonicalFrame(right)) : null),
    };
  } catch (error) {
    return {
      id: testCase.id,
      family: testCase.family,
      classification: testCase.classification,
      status: "error",
      error: {
        category: classifyError(error),
        nativeType: error instanceof Error ? error.name : typeof error,
        message: error instanceof Error ? error.message : String(error),
      },
      inputPreserved:
        before === JSON.stringify(canonicalFrame(frame)) &&
        rightBefore === (right ? JSON.stringify(canonicalFrame(right)) : null),
    };
  }
});

const payload = {
  schemaVersion: "1.0.0",
  implementation: "bun_panda",
  version: "0.4.0",
  runtime: `Bun ${Bun.version}`,
  mode,
  startedAt,
  completedAt: new Date().toISOString(),
  cases: results.length,
  results,
};

mkdirSync(dirname(outputPath), { recursive: true });
writeFileSync(outputPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
console.log(`wrote ${results.length} ${mode} observations to ${outputPath}`);
