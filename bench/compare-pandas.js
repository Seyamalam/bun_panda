import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

const arqueroPath = process.env.BUN_PANDA_BENCH_INPUT ?? "bench/results/arquero.json";
const pandasPath = process.env.BUN_PANDA_PANDAS_INPUT ?? "bench/results/pandas.json";
const outPath = process.env.BUN_PANDA_PANDAS_COMPARE_JSON ?? "bench/results/pandas-compare.json";

const arquero = JSON.parse(readFileSync(arqueroPath, "utf8"));
const pandas = JSON.parse(readFileSync(pandasPath, "utf8"));

const bunByCase = new Map((arquero.results ?? []).map((entry) => [entry.case, entry]));
const combined = [];

for (const entry of pandas.results ?? []) {
  const bun = bunByCase.get(entry.case);
  if (!bun) {
    continue;
  }
  const ratio = bun.bunAvgMs / entry.pandasAvgMs;
  combined.push({
    case: entry.case,
    dataset: entry.dataset,
    bunAvgMs: bun.bunAvgMs,
    pandasAvgMs: entry.pandasAvgMs,
    deltaMs: bun.bunAvgMs - entry.pandasAvgMs,
    ratio,
  });
}

const payload = {
  generatedAt: new Date().toISOString(),
  rows: arquero.rows,
  iterations: arquero.iterations,
  cases: combined.length,
  results: combined,
};

mkdirSync(dirname(outPath), { recursive: true });
writeFileSync(outPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");

const lines = [];
lines.push("# bun_panda vs pandas benchmark");
lines.push(
  `rows=${payload.rows}, iterations=${payload.iterations}, rounds=${pandas.rounds ?? arquero.rounds ?? 1}, cases=${payload.cases}`
);
lines.push("");
lines.push("| case | dataset | bun_panda avg | pandas avg | delta | ratio (bun/pd) |");
lines.push("| --- | --- | ---: | ---: | ---: | ---: |");
for (const entry of combined) {
  const deltaSign = entry.deltaMs > 0 ? "+" : "";
  lines.push(
    `| ${entry.case} | ${entry.dataset} | ${entry.bunAvgMs.toFixed(2)}ms | ${entry.pandasAvgMs.toFixed(
      2
    )}ms | ${deltaSign}${entry.deltaMs.toFixed(2)}ms | ${entry.ratio.toFixed(2)}x |`
  );
}
console.log(lines.join("\n"));
