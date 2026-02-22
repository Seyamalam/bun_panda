import { parse_csv, parse_json, parse_tsv } from "../index";
import { performance } from "node:perf_hooks";
import { writeFileSync } from "node:fs";

const iterations = parseInt(process.env.BUN_PANDA_IO_BENCH_ITERATIONS ?? "10", 10);
const jsonOut = process.env.BUN_PANDA_IO_BENCH_JSON ?? "";

const cases = [
  buildCase("csv_plain_narrow_1k", "csv", 1000, 4, "plain"),
  buildCase("csv_plain_narrow_5k", "csv", 5000, 4, "plain"),
  buildCase("csv_plain_narrow_15k", "csv", 15000, 4, "plain"),
  buildCase("csv_plain_wide_1k", "csv", 1000, 12, "plain"),
  buildCase("csv_plain_wide_5k", "csv", 5000, 12, "plain"),
  buildCase("csv_plain_wide_15k", "csv", 15000, 12, "plain"),
  buildCase("csv_missing_1k", "csv", 1000, 6, "missing"),
  buildCase("csv_missing_5k", "csv", 5000, 6, "missing"),
  buildCase("csv_missing_15k", "csv", 15000, 6, "missing"),
  buildCase("csv_bool_1k", "csv", 1000, 6, "bool"),
  buildCase("csv_bool_5k", "csv", 5000, 6, "bool"),
  buildCase("csv_bool_15k", "csv", 15000, 6, "bool"),
  buildCase("csv_quoted_1k", "csv", 1000, 4, "quoted"),
  buildCase("csv_quoted_5k", "csv", 5000, 4, "quoted"),
  buildCase("tsv_plain_narrow_1k", "tsv", 1000, 4, "plain"),
  buildCase("tsv_plain_narrow_5k", "tsv", 5000, 4, "plain"),
  buildCase("tsv_plain_narrow_15k", "tsv", 15000, 4, "plain"),
  buildCase("json_records_1k", "json", 1000, 6, "plain"),
  buildCase("json_records_5k", "json", 5000, 6, "plain"),
  buildCase("json_records_15k", "json", 15000, 6, "plain"),
];

const results = cases.map((entry) => runCase(entry, iterations));
printReport(results, iterations);

if (jsonOut) {
  writeFileSync(
    jsonOut,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        iterations,
        cases: results,
      },
      null,
      2
    ) + "\n",
    "utf8"
  );
}

function runCase(entry, iterationsCount) {
  const samples = [];
  for (let i = 0; i < iterationsCount; i += 1) {
    const start = performance.now();
    let frame;
    if (entry.kind === "csv") {
      frame = parse_csv(entry.payload);
    } else if (entry.kind === "tsv") {
      frame = parse_tsv(entry.payload);
    } else {
      frame = parse_json(entry.payload);
    }
    const end = performance.now();
    samples.push(end - start);
    if (frame.shape[0] !== entry.rows) {
      throw new Error(`Unexpected row count for ${entry.name}.`);
    }
  }
  return {
    case: entry.name,
    kind: entry.kind,
    rows: entry.rows,
    cols: entry.cols,
    avgMs: average(samples),
    minMs: Math.min(...samples),
    maxMs: Math.max(...samples),
  };
}

function printReport(results, iterationsCount) {
  console.log(`\n# bun_panda io benchmark iterations=${iterationsCount}\n`);
  console.log("| case | kind | rows | cols | avg | min | max |");
  console.log("| --- | --- | ---: | ---: | ---: | ---: | ---: |");
  for (const row of results) {
    console.log(
      `| ${row.case} | ${row.kind} | ${row.rows} | ${row.cols} | ${formatMs(row.avgMs)} | ${formatMs(row.minMs)} | ${formatMs(row.maxMs)} |`
    );
  }
}

function buildCase(name, kind, rows, cols, mode) {
  if (kind === "csv") {
    return { name, kind, rows, cols, payload: buildDelimited(rows, cols, ",", mode) };
  }
  if (kind === "tsv") {
    return { name, kind, rows, cols, payload: buildDelimited(rows, cols, "\t", mode) };
  }
  return { name, kind, rows, cols, payload: buildJson(rows, cols, mode) };
}

function buildDelimited(rows, cols, sep, mode) {
  const headers = Array.from({ length: cols }, (_, i) => `c${i}`).join(sep);
  const lines = [headers];
  for (let r = 0; r < rows; r += 1) {
    const values = [];
    for (let c = 0; c < cols; c += 1) {
      values.push(valueFor(mode, r, c, sep));
    }
    lines.push(values.join(sep));
  }
  lines.push("");
  return lines.join("\n");
}

function buildJson(rows, cols, mode) {
  const records = [];
  for (let r = 0; r < rows; r += 1) {
    const row = {};
    for (let c = 0; c < cols; c += 1) {
      row[`c${c}`] = jsonValueFor(mode, r, c);
    }
    records.push(row);
  }
  return JSON.stringify(records);
}

function valueFor(mode, row, col, sep) {
  if (mode === "missing") {
    if ((row + col) % 7 === 0) {
      return "NA";
    }
    return `${row * (col + 1)}`;
  }
  if (mode === "bool") {
    if (col % 2 === 0) {
      return row % 2 === 0 ? "true" : "false";
    }
    return `${(row + col) % 100}`;
  }
  if (mode === "quoted") {
    if (col === 1) {
      return `"v${row}${sep}q${col}"`;
    }
    return `"${row + col}"`;
  }
  return `${row + col}`;
}

function jsonValueFor(mode, row, col) {
  if (mode === "bool") {
    return col % 2 === 0 ? row % 2 === 0 : row + col;
  }
  if (mode === "missing") {
    return (row + col) % 7 === 0 ? null : row + col;
  }
  return row + col;
}

function average(values) {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function formatMs(value) {
  return `${value.toFixed(2)}ms`;
}
