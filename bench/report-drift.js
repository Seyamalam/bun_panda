import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

const INPUT = process.env.BUN_PANDA_BENCH_INPUT ?? "bench/results/arquero.json";
const OUTPUT = process.env.BUN_PANDA_DRIFT_JSON ?? "bench/results/drift.json";

function main() {
  const payload = JSON.parse(readFileSync(INPUT, "utf8"));
  const rows = Array.isArray(payload.results) ? payload.results : [];
  if (rows.length === 0) {
    throw new Error(`[drift-report] no results found in '${INPUT}'.`);
  }

  const byFamily = new Map();
  for (const entry of rows) {
    const family = familyName(entry.case);
    if (!byFamily.has(family)) {
      byFamily.set(family, []);
    }
    byFamily.get(family).push(entry.ratio);
  }

  const families = [...byFamily.entries()]
    .map(([family, ratios]) => summarizeFamily(family, ratios))
    .sort((a, b) => b.p90 - a.p90);

  const slowCases = rows
    .filter((entry) => entry.ratio > 1)
    .sort((a, b) => b.ratio - a.ratio)
    .slice(0, 15)
    .map((entry) => ({
      case: entry.case,
      dataset: entry.dataset,
      ratio: entry.ratio,
      bunAvgMs: entry.bunAvgMs,
      arqueroAvgMs: entry.arqueroAvgMs,
    }));

  const report = {
    generatedAt: new Date().toISOString(),
    input: INPUT,
    cases: rows.length,
    families,
    slowCases,
  };

  mkdirSync(dirname(OUTPUT), { recursive: true });
  writeFileSync(OUTPUT, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  console.log(`# bench drift report`);
  console.log(`input=${INPUT}`);
  console.log(`cases=${rows.length}`);
  console.log("");
  console.log("| family | cases | p50 ratio | p90 ratio | max ratio |");
  console.log("| --- | ---: | ---: | ---: | ---: |");
  for (const family of families) {
    console.log(
      `| ${family.family} | ${family.count} | ${family.p50.toFixed(3)} | ${family.p90.toFixed(
        3
      )} | ${family.max.toFixed(3)} |`
    );
  }

  if (slowCases.length > 0) {
    console.log("");
    console.log("| slow case | dataset | ratio | bun avg | aq avg |");
    console.log("| --- | --- | ---: | ---: | ---: |");
    for (const entry of slowCases) {
      console.log(
        `| ${entry.case} | ${entry.dataset} | ${entry.ratio.toFixed(3)} | ${entry.bunAvgMs.toFixed(
          2
        )}ms | ${entry.arqueroAvgMs.toFixed(2)}ms |`
      );
    }
  }
}

function familyName(caseName) {
  if (caseName.startsWith("merge_")) {
    return "merge";
  }
  if (caseName.startsWith("groupby_") || caseName.startsWith("skewed_groupby_")) {
    return "groupby";
  }
  if (caseName.startsWith("value_counts_")) {
    return "value_counts";
  }
  if (caseName.includes("filter_sort")) {
    return "filter_sort";
  }
  if (caseName.includes("sort_")) {
    return "sort";
  }
  return "other";
}

function summarizeFamily(family, ratios) {
  const sorted = [...ratios].sort((a, b) => a - b);
  return {
    family,
    count: sorted.length,
    p50: percentile(sorted, 0.5),
    p90: percentile(sorted, 0.9),
    max: sorted[sorted.length - 1] ?? 0,
  };
}

function percentile(sorted, q) {
  if (sorted.length === 0) {
    return 0;
  }
  const position = Math.min(sorted.length - 1, Math.max(0, Math.floor((sorted.length - 1) * q)));
  return sorted[position];
}

main();
