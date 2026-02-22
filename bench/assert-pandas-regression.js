import { readFileSync } from "node:fs";

const INPUT = process.env.BUN_PANDA_PANDAS_COMPARE_INPUT ?? "bench/results/pandas-compare.json";

const CASE_MAX_RATIO = {
  groupby_mean: 2.5,
  filter_sort_top100: 0.8,
  sort_top1000: 3.5,
  sort_multicol_top800: 3.2,
  value_counts_city: 1.2,
  value_counts_group_city_top10: 2.4,
  value_counts_missing_city_dropna_false: 1.3,
  groupby_missing_city_mean: 1.6,
  value_counts_high_card_city_top20: 2.6,
  value_counts_high_card_user_top100: 1.3,
};

function main() {
  const payload = JSON.parse(readFileSync(INPUT, "utf8"));
  const results = payload.results ?? [];
  if (!Array.isArray(results) || results.length === 0) {
    throw new Error(`[pandas-gate] no comparison cases in '${INPUT}'.`);
  }

  const resultByCase = new Map(results.map((entry) => [entry.case, entry]));
  const failures = [];

  for (const [caseName, maxRatio] of Object.entries(CASE_MAX_RATIO)) {
    const entry = resultByCase.get(caseName);
    if (!entry) {
      failures.push(`[pandas-gate] missing case '${caseName}' in comparison output.`);
      continue;
    }

    const ratio = Number(entry.ratio);
    if (!Number.isFinite(ratio)) {
      failures.push(`[pandas-gate] case '${caseName}' has non-finite ratio.`);
      continue;
    }
    if (ratio > maxRatio) {
      failures.push(
        `[pandas-gate] case '${caseName}' ratio ${ratio.toFixed(2)}x exceeded ${maxRatio.toFixed(2)}x.`
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
    `[pandas-gate] passed: validated ${Object.keys(CASE_MAX_RATIO).length} tracked pandas ratios from '${INPUT}'.`
  );
}

main();
