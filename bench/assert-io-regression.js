import { readFileSync } from "node:fs";

const INPUT = process.env.BUN_PANDA_IO_BENCH_INPUT ?? "bench/results/io.json";

const CASE_MAX_AVG_MS = {
  csv_plain_narrow_15k: 22,
  csv_plain_wide_15k: 48,
  csv_missing_15k: 24,
  csv_bool_15k: 18,
  csv_quoted_5k: 12,
  tsv_plain_narrow_15k: 18,
  json_records_15k: 16,
};

function main() {
  const payload = JSON.parse(readFileSync(INPUT, "utf8"));
  const results = payload.results ?? payload.cases ?? [];
  if (!Array.isArray(results) || results.length === 0) {
    throw new Error(`[io-gate] no benchmark cases in '${INPUT}'.`);
  }

  const resultByCase = new Map(results.map((entry) => [entry.case, entry]));
  const failures = [];

  for (const [caseName, maxMs] of Object.entries(CASE_MAX_AVG_MS)) {
    const entry = resultByCase.get(caseName);
    if (!entry) {
      failures.push(`[io-gate] missing case '${caseName}' in benchmark output.`);
      continue;
    }
    const avgMs = Number(entry.avgMs);
    if (!Number.isFinite(avgMs)) {
      failures.push(`[io-gate] case '${caseName}' has non-finite avgMs.`);
      continue;
    }
    if (avgMs > maxMs) {
      failures.push(
        `[io-gate] case '${caseName}' exceeded ${maxMs.toFixed(2)}ms (actual ${avgMs.toFixed(2)}ms).`
      );
    }
  }

  if (failures.length > 0) {
    for (const failure of failures) {
      console.error(failure);
    }
    process.exit(1);
  }

  console.log(
    `[io-gate] passed: validated ${Object.keys(CASE_MAX_AVG_MS).length} headline IO cases from '${INPUT}'.`
  );
}

main();
