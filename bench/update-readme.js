import { readFileSync, writeFileSync } from "node:fs";

const readmePath = process.env.BUN_PANDA_README_PATH ?? "README.md";
const arqueroPath = process.env.BUN_PANDA_BENCH_INPUT ?? "bench/results/arquero.json";
const pandasPath = process.env.BUN_PANDA_PANDAS_COMPARE_JSON ?? "bench/results/pandas-compare.json";

const START = "<!-- BENCHMARKS:START -->";
const END = "<!-- BENCHMARKS:END -->";

const arquero = JSON.parse(readFileSync(arqueroPath, "utf8"));
const pandas = JSON.parse(readFileSync(pandasPath, "utf8"));

const headlineCases = [
  "groupby_mean",
  "filter_sort_top100",
  "sort_top1000",
  "sort_multicol_top800",
  "value_counts_city",
  "value_counts_group_city_top10",
  "value_counts_missing_city_dropna_false",
  "value_counts_high_card_city_top20",
  "value_counts_high_card_user_top100",
];

const arqueroByCase = new Map((arquero.results ?? []).map((entry) => [entry.case, entry]));
const arqueroRows = headlineCases.map((name) => arqueroByCase.get(name)).filter(Boolean);

const fasterArquero = (arquero.results ?? []).filter((entry) => entry.ratio <= 1).length;
const totalArquero = arquero.results?.length ?? 0;
const fasterPandas = (pandas.results ?? []).filter((entry) => entry.ratio <= 1).length;
const totalPandas = pandas.results?.length ?? 0;

const block = [
  START,
  "### Automated Benchmark Snapshot",
  "",
  `Generated from benchmark scripts (rows=${arquero.rows}, iterations=${arquero.iterations}).`,
  `bun_panda vs Arquero: faster or equal in ${fasterArquero}/${totalArquero} cases.`,
  `bun_panda vs pandas: faster or equal in ${fasterPandas}/${totalPandas} tracked cases.`,
  "",
  "#### bun_panda vs Arquero (headline cases)",
  "",
  "| case | dataset | bun_panda avg | arquero avg | ratio (bun/aq) |",
  "| --- | --- | ---: | ---: | ---: |",
  ...arqueroRows.map(
    (entry) =>
      `| ${entry.case} | ${entry.dataset} | ${entry.bunAvgMs.toFixed(2)}ms | ${entry.arqueroAvgMs.toFixed(
        2
      )}ms | ${entry.ratio.toFixed(2)}x |`
  ),
  "",
  "#### bun_panda vs pandas",
  "",
  "| case | dataset | bun_panda avg | pandas avg | ratio (bun/pd) |",
  "| --- | --- | ---: | ---: | ---: |",
  ...(pandas.results ?? []).map(
    (entry) =>
      `| ${entry.case} | ${entry.dataset} | ${entry.bunAvgMs.toFixed(2)}ms | ${entry.pandasAvgMs.toFixed(
        2
      )}ms | ${entry.ratio.toFixed(2)}x |`
  ),
  "",
  END,
].join("\n");

const readme = readFileSync(readmePath, "utf8");
const startIdx = readme.indexOf(START);
const endIdx = readme.indexOf(END);

if (startIdx < 0 || endIdx < 0 || endIdx < startIdx) {
  throw new Error(`Could not find benchmark markers in ${readmePath}`);
}

const updated = `${readme.slice(0, startIdx)}${block}${readme.slice(endIdx + END.length)}`;
writeFileSync(readmePath, updated, "utf8");
console.log(`[update-readme] refreshed ${readmePath}`);
