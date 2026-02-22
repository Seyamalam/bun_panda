import { readFileSync } from "node:fs";

const inputPath = process.env.BUN_PANDA_BENCH_INPUT ?? "bench/results/arquero.json";
const maxRatio = Number(process.env.BUN_PANDA_BENCH_MAX_RATIO ?? 1.05);
const mergeMaxRatio = Number(process.env.BUN_PANDA_BENCH_MERGE_MAX_RATIO ?? 4.5);
const maxFailing = Number(process.env.BUN_PANDA_BENCH_MAX_FAILING ?? 0);
const allowSlow = new Set(
  String(process.env.BUN_PANDA_BENCH_ALLOW_SLOW ?? "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
);

const payload = JSON.parse(readFileSync(inputPath, "utf8"));
const failing = [];

for (const entry of payload.results ?? []) {
  if (allowSlow.has(entry.case)) {
    continue;
  }
  const threshold = entry.case.startsWith("merge_") ? mergeMaxRatio : maxRatio;
  if (entry.ratio > threshold) {
    failing.push(entry);
  }
}

if (failing.length > maxFailing) {
  console.error(
    `[bench-gate] failed: ${failing.length} case(s) exceeded ratio thresholds (default>${maxRatio.toFixed(
      2
    )}, merge>${mergeMaxRatio.toFixed(2)}, allowed=${maxFailing}).`
  );
  for (const entry of failing) {
    console.error(
      ` - ${entry.case} [${entry.dataset}] ratio=${entry.ratio.toFixed(3)} bun=${entry.bunAvgMs.toFixed(
        3
      )}ms aq=${entry.arqueroAvgMs.toFixed(3)}ms`
    );
  }
  process.exit(1);
}

const ratios = (payload.results ?? []).map((entry) => entry.ratio).sort((a, b) => a - b);
const maxSeen = ratios.length > 0 ? ratios[ratios.length - 1] : 0;
const median = ratios.length > 0 ? ratios[Math.floor(ratios.length / 2)] : 0;
console.log(
  `[bench-gate] passed: cases=${payload.results?.length ?? 0}, max_ratio=${maxSeen.toFixed(
    3
  )}, median_ratio=${median.toFixed(3)} thresholds={default:${maxRatio.toFixed(
    2
  )}, merge:${mergeMaxRatio.toFixed(2)}}`
);
