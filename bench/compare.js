import * as aq from "arquero";
import { DataFrame } from "../index";

const ROWS = Number(process.env.BUN_PANDA_BENCH_ROWS ?? 25000);
const ITERS = Number(process.env.BUN_PANDA_BENCH_ITERS ?? 12);

function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (1664525 * state + 1013904223) >>> 0;
    return state / 2 ** 32;
  };
}

function buildDataset(rowCount) {
  const rnd = lcg(42);
  const groups = ["A", "B", "C", "D", "E", "F"];
  const cities = ["Austin", "Seattle", "Boston", "Denver", "Miami"];

  return Array.from({ length: rowCount }, (_, id) => {
    const value = Math.floor(rnd() * 1000);
    const weight = Number((rnd() * 5 + 0.5).toFixed(2));
    return {
      id,
      group: groups[id % groups.length],
      city: cities[id % cities.length],
      value,
      weight,
      active: id % 3 === 0,
    };
  });
}

function benchCase(name, fn, iterations = ITERS) {
  for (let i = 0; i < 3; i += 1) {
    fn();
  }

  const times = [];
  for (let i = 0; i < iterations; i += 1) {
    const start = performance.now();
    const out = fn();
    if (out === undefined || out === null) {
      throw new Error(`Benchmark case '${name}' returned no output.`);
    }
    times.push(performance.now() - start);
  }

  const total = times.reduce((sum, value) => sum + value, 0);
  return {
    avgMs: total / times.length,
    minMs: Math.min(...times),
    maxMs: Math.max(...times),
  };
}

function formatMs(value) {
  return `${value.toFixed(2)}ms`;
}

const records = buildDataset(ROWS);
const arqTable = aq.from(records);
const op = aq.op;

const cases = [
  {
    name: "groupby_mean",
    bunPanda: () =>
      new DataFrame(records)
        .groupby("group")
        .agg({ value: "mean", weight: "sum" })
        .sort_values("group")
        .to_records()
        .length,
    arquero: () =>
      arqTable
        .groupby("group")
        .rollup({
          value: (d) => op.mean(d.value),
          weight: (d) => op.sum(d.weight),
        })
        .orderby("group")
        .objects().length,
  },
  {
    name: "filter_sort",
    bunPanda: () =>
      new DataFrame(records)
        .query((row) => Boolean(row.active) && Number(row.value) > 500)
        .sort_values("value", false)
        .head(100)
        .to_records().length,
    arquero: () =>
      arqTable
        .filter((d) => d.active && d.value > 500)
        .orderby(aq.desc("value"))
        .slice(0, 100)
        .objects().length,
  },
  {
    name: "value_counts_city",
    bunPanda: () =>
      new DataFrame(records).value_counts({ subset: "city" }).to_records().length,
    arquero: () =>
      arqTable
        .groupby("city")
        .rollup({ count: () => op.count() })
        .orderby(aq.desc("count"))
        .objects().length,
  },
];

const lines = [];
lines.push(`# bun_panda benchmark`);
lines.push(`rows=${ROWS}, iterations=${ITERS}`);
lines.push("");
lines.push("| case | bun_panda avg | arquero avg | delta |");
lines.push("| --- | ---: | ---: | ---: |");

for (const bench of cases) {
  const bunStats = benchCase(`${bench.name}/bun_panda`, bench.bunPanda);
  const arqueroStats = benchCase(`${bench.name}/arquero`, bench.arquero);
  const delta = bunStats.avgMs - arqueroStats.avgMs;
  const deltaSign = delta > 0 ? "+" : "";

  lines.push(
    `| ${bench.name} | ${formatMs(bunStats.avgMs)} | ${formatMs(arqueroStats.avgMs)} | ${deltaSign}${delta.toFixed(
      2
    )}ms |`
  );
}

console.log(lines.join("\n"));
