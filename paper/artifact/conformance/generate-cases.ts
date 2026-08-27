import { mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

type TaggedNumber = { $number: "NaN" | "Infinity" | "-Infinity" };
type JsonCell = string | number | boolean | null | TaggedNumber;
type JsonRow = Record<string, JsonCell>;

interface FrameInput {
  columns: string[];
  index: (string | number)[];
  rows: JsonRow[];
}

interface ConformanceCase {
  id: string;
  seed: number;
  family: string;
  classification: "valid" | "boundary";
  input: FrameInput;
  right?: FrameInput;
  operation: { name: string; args: Record<string, unknown> };
}

const OUTPUT = process.env.BUN_PANDA_CONFORMANCE_CASES ??
  "paper/data/conformance/cases.json";
const BASE_SEED = 20260826;
const VALID_PER_FAMILY = 200;
const BOUNDARY_PER_FAMILY = 50;

function lcg(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(1664525, state) + 1013904223) >>> 0;
    return state / 2 ** 32;
  };
}

function integer(rnd: () => number, min: number, max: number): number {
  return min + Math.floor(rnd() * (max - min + 1));
}

function numericCell(rnd: () => number, row: number): JsonCell {
  if (row > 0 && row % 17 === 0) return { $number: "NaN" };
  if (rnd() < 0.16) return null;
  return integer(rnd, -20, 40) + Math.round(rnd() * 100) / 100;
}

function makeFrame(seed: number, numericOnly = false): FrameInput {
  const rnd = lcg(seed);
  const rowCount = integer(rnd, 0, 12);
  const columns = numericOnly
    ? ["value", "weight"]
    : ["group", "value", "weight", "flag", "label"];
  const rows: JsonRow[] = [];
  const index: (string | number)[] = [];

  for (let row = 0; row < rowCount; row += 1) {
    const value = numericCell(rnd, row);
    const weight = numericCell(rnd, row + 3);
    if (numericOnly) {
      rows.push({ value, weight });
    } else {
      rows.push({
        group: rnd() < 0.12 ? null : ["A", "B", "C"][integer(rnd, 0, 2)]!,
        value,
        weight,
        flag: rnd() < 0.14 ? null : rnd() >= 0.5,
        label: rnd() < 0.12 ? null : `label-${integer(rnd, 0, 3)}`,
      });
    }
    index.push(row > 0 && row % 5 === 0 ? "duplicate" : `row-${row % 5}`);
  }

  return { columns, index, rows };
}

function makeMergeRight(seed: number): FrameInput {
  const rnd = lcg(seed ^ 0x9e3779b9);
  const rowCount = integer(rnd, 0, 9);
  const rows: JsonRow[] = [];
  const index: number[] = [];
  for (let row = 0; row < rowCount; row += 1) {
    rows.push({
      group: rnd() < 0.12 ? null : ["A", "B", "D"][integer(rnd, 0, 2)]!,
      right_value: numericCell(rnd, row + 7),
    });
    index.push(row);
  }
  return { columns: ["group", "right_value"], index, rows };
}

const families = [
  "sort_values",
  "dropna",
  "fillna",
  "drop_duplicates",
  "shift",
  "diff",
  "rank",
  "groupby_agg",
  "value_counts",
  "merge",
] as const;

function validOperation(
  family: (typeof families)[number],
  seed: number
): Pick<ConformanceCase, "input" | "right" | "operation"> {
  const rnd = lcg(seed ^ 0xa5a5a5a5);
  if (family === "sort_values") {
    return {
      input: makeFrame(seed),
      operation: {
        name: family,
        args: {
          by: rnd() < 0.5 ? ["value"] : ["group", "value"],
          ascending: rnd() < 0.5,
          na_position: rnd() < 0.5 ? "first" : "last",
        },
      },
    };
  }
  if (family === "dropna") {
    return {
      input: makeFrame(seed),
      operation: {
        name: family,
        args: { subset: rnd() < 0.5 ? ["value"] : ["group", "value"] },
      },
    };
  }
  if (family === "fillna") {
    return {
      input: makeFrame(seed),
      operation: {
        name: family,
        args: {
          value: rnd() < 0.5
            ? { group: "missing", value: 0, weight: -1, flag: false, label: "missing" }
            : 0,
        },
      },
    };
  }
  if (family === "drop_duplicates") {
    return {
      input: makeFrame(seed),
      operation: {
        name: family,
        args: {
          subset: rnd() < 0.5 ? ["group", "label"] : ["value"],
          keep: ["first", "last", false][integer(rnd, 0, 2)],
          ignore_index: rnd() < 0.5,
        },
      },
    };
  }
  if (family === "shift") {
    return {
      input: makeFrame(seed),
      operation: { name: family, args: { periods: integer(rnd, -3, 3) } },
    };
  }
  if (family === "diff") {
    return {
      input: makeFrame(seed, true),
      operation: { name: family, args: { periods: integer(rnd, -3, 3) } },
    };
  }
  if (family === "rank") {
    return {
      input: makeFrame(seed, true),
      operation: {
        name: family,
        args: {
          method: ["average", "min", "max", "first", "dense"][integer(rnd, 0, 4)],
          ascending: rnd() < 0.5,
          na_option: ["keep", "top", "bottom"][integer(rnd, 0, 2)],
          pct: rnd() < 0.25,
        },
      },
    };
  }
  if (family === "groupby_agg") {
    return {
      input: makeFrame(seed),
      operation: {
        name: family,
        args: {
          by: ["group"],
          spec: { value: ["sum", "mean", "min", "max", "count"][integer(rnd, 0, 4)] },
          dropna: rnd() < 0.5,
          sort: rnd() < 0.5,
        },
      },
    };
  }
  if (family === "value_counts") {
    return {
      input: makeFrame(seed),
      operation: {
        name: family,
        args: {
          subset: rnd() < 0.5 ? ["group"] : ["group", "label"],
          normalize: rnd() < 0.25,
          dropna: rnd() < 0.5,
          sort: true,
          ascending: rnd() < 0.5,
        },
      },
    };
  }
  return {
    input: makeFrame(seed),
    right: makeMergeRight(seed),
    operation: {
      name: family,
      args: {
        on: ["group"],
        how: ["inner", "left", "right", "outer"][integer(rnd, 0, 3)],
      },
    },
  };
}

function boundaryOperation(
  family: (typeof families)[number],
  seed: number
): Pick<ConformanceCase, "input" | "right" | "operation"> {
  const input = makeFrame(seed, family === "diff" || family === "rank");
  if (family === "sort_values") return { input, operation: { name: family, args: { by: ["missing"] } } };
  if (family === "dropna") return { input, operation: { name: family, args: { subset: ["missing"] } } };
  if (family === "fillna") return { input, operation: { name: family, args: { value: { missing: 0 } } } };
  if (family === "drop_duplicates") return { input, operation: { name: family, args: { subset: ["missing"] } } };
  if (family === "shift") return { input, operation: { name: family, args: { periods: 1.5 } } };
  if (family === "diff") return { input, operation: { name: family, args: { periods: 1.5 } } };
  if (family === "rank") return { input, operation: { name: family, args: { method: "invalid" } } };
  if (family === "groupby_agg") return { input, operation: { name: family, args: { by: ["missing"], spec: { value: "mean" } } } };
  if (family === "value_counts") return { input, operation: { name: family, args: { subset: ["missing"] } } };
  return {
    input,
    right: makeMergeRight(seed),
    operation: { name: family, args: { on: ["missing"], how: "inner" } },
  };
}

const cases: ConformanceCase[] = [];
for (let familyIndex = 0; familyIndex < families.length; familyIndex += 1) {
  const family = families[familyIndex]!;
  for (let i = 0; i < VALID_PER_FAMILY; i += 1) {
    const seed = BASE_SEED + familyIndex * 100_000 + i;
    cases.push({
      id: `${family}-valid-${String(i).padStart(3, "0")}`,
      seed,
      family,
      classification: "valid",
      ...validOperation(family, seed),
    });
  }
  for (let i = 0; i < BOUNDARY_PER_FAMILY; i += 1) {
    const seed = BASE_SEED + familyIndex * 100_000 + VALID_PER_FAMILY + i;
    cases.push({
      id: `${family}-boundary-${String(i).padStart(3, "0")}`,
      seed,
      family,
      classification: "boundary",
      ...boundaryOperation(family, seed),
    });
  }
}

const payload = {
  schemaVersion: "1.0.0",
  generatedAt: new Date().toISOString(),
  generator: "paper/artifact/conformance/generate-cases.ts",
  baseSeed: BASE_SEED,
  validPerFamily: VALID_PER_FAMILY,
  boundaryPerFamily: BOUNDARY_PER_FAMILY,
  families,
  cases,
};

mkdirSync(dirname(OUTPUT), { recursive: true });
writeFileSync(OUTPUT, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
console.log(`wrote ${cases.length} deterministic cases to ${OUTPUT}`);
