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

function buildDataset(rowCount, options = {}) {
  const skew = options.skew ?? false;
  const wide = options.wide ?? false;
  const rnd = lcg(42);
  const groups = ["A", "B", "C", "D", "E", "F"];
  const cities = ["Austin", "Seattle", "Boston", "Denver", "Miami"];
  const segments = ["consumer", "enterprise", "startup"];

  const rows = Array.from({ length: rowCount }, (_, id) => {
    const value = Math.floor(rnd() * 1000);
    const weight = Number((rnd() * 5 + 0.5).toFixed(2));
    const row = {
      id,
      group: skew
        ? id % 100 < 70
          ? "A"
          : groups[id % groups.length]
        : groups[id % groups.length],
      city: cities[id % cities.length],
      segment: segments[id % segments.length],
      value,
      weight,
      revenue: Number((value * weight).toFixed(2)),
      active: id % 3 === 0,
      bucket: value > 700 ? "high" : value > 350 ? "mid" : "low",
    };

    if (wide) {
      for (let i = 0; i < 10; i += 1) {
        row[`extra_${i}`] = (value + i) % (50 + i);
      }
    }

    return row;
  });

  return rows;
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

const datasets = {
  base: buildDataset(ROWS),
  skewed: buildDataset(ROWS, { skew: true }),
  wide: buildDataset(ROWS, { wide: true }),
};

const tables = {
  base: aq.from(datasets.base),
  skewed: aq.from(datasets.skewed),
  wide: aq.from(datasets.wide),
};

const op = aq.op;

const cases = [
  {
    name: "groupby_mean",
    dataset: "base",
    bunPanda: (records) =>
      new DataFrame(records)
        .groupby("group")
        .agg({ value: "mean", revenue: "sum" })
        .sort_values("group")
        .to_records()
        .length,
    arquero: (table) =>
      table
        .groupby("group")
        .rollup({
          value: (d) => op.mean(d.value),
          revenue: (d) => op.sum(d.revenue),
        })
        .orderby("group")
        .objects().length,
  },
  {
    name: "groupby_mean_2keys",
    dataset: "base",
    bunPanda: (records) =>
      new DataFrame(records)
        .groupby(["group", "city"])
        .agg({ value: "mean", revenue: "sum" })
        .sort_values(["group", "city"])
        .to_records()
        .length,
    arquero: (table) =>
      table
        .groupby("group", "city")
        .rollup({
          value: (d) => op.mean(d.value),
          revenue: (d) => op.sum(d.revenue),
        })
        .orderby("group", "city")
        .objects().length,
  },
  {
    name: "filter_sort_top100",
    dataset: "base",
    bunPanda: (records) =>
      new DataFrame(records)
        .query((row) => Boolean(row.active) && Number(row.value) > 500)
        .sort_values("value", false)
        .head(100)
        .to_records().length,
    arquero: (table) =>
      table
        .filter((d) => d.active && d.value > 500)
        .orderby(aq.desc("value"))
        .slice(0, 100)
        .objects().length,
  },
  {
    name: "filter_sort_multicol_top200",
    dataset: "base",
    bunPanda: (records) =>
      new DataFrame(records)
        .query((row) => Number(row.value) > 300)
        .sort_values(["group", "value", "id"], [true, false, true])
        .head(200)
        .to_records().length,
    arquero: (table) =>
      table
        .filter((d) => d.value > 300)
        .orderby("group", aq.desc("value"), "id")
        .slice(0, 200)
        .objects().length,
  },
  {
    name: "value_counts_city",
    dataset: "base",
    bunPanda: (records) =>
      new DataFrame(records).value_counts({ subset: "city" }).to_records().length,
    arquero: (table) =>
      table
        .groupby("city")
        .rollup({ count: () => op.count() })
        .orderby(aq.desc("count"))
        .objects().length,
  },
  {
    name: "value_counts_group_city",
    dataset: "base",
    bunPanda: (records) =>
      new DataFrame(records).value_counts({ subset: ["group", "city"] }).to_records().length,
    arquero: (table) =>
      table
        .groupby("group", "city")
        .rollup({ count: () => op.count() })
        .orderby(aq.desc("count"), "group", "city")
        .objects().length,
  },
  {
    name: "drop_duplicates_group_city",
    dataset: "base",
    bunPanda: (records) =>
      new DataFrame(records).drop_duplicates(["group", "city"], "first", true).shape[0],
    arquero: (table) =>
      table
        .groupby("group", "city")
        .rollup({ count: () => op.count() })
        .objects().length,
  },
  {
    name: "skewed_groupby_mean",
    dataset: "skewed",
    bunPanda: (records) =>
      new DataFrame(records)
        .groupby("group")
        .agg({ value: "mean", revenue: "sum" })
        .sort_values("group")
        .to_records().length,
    arquero: (table) =>
      table
        .groupby("group")
        .rollup({
          value: (d) => op.mean(d.value),
          revenue: (d) => op.sum(d.revenue),
        })
        .orderby("group")
        .objects().length,
  },
  {
    name: "wide_groupby_sum",
    dataset: "wide",
    bunPanda: (records) =>
      new DataFrame(records)
        .groupby(["group", "segment"])
        .agg({ extra_1: "sum", extra_2: "mean", revenue: "sum" })
        .sort_values(["group", "segment"])
        .to_records().length,
    arquero: (table) =>
      table
        .groupby("group", "segment")
        .rollup({
          extra_1: (d) => op.sum(d.extra_1),
          extra_2: (d) => op.mean(d.extra_2),
          revenue: (d) => op.sum(d.revenue),
        })
        .orderby("group", "segment")
        .objects().length,
  },
  {
    name: "wide_filter_sort",
    dataset: "wide",
    bunPanda: (records) =>
      new DataFrame(records)
        .query((row) => Number(row.extra_4) > 10 && Number(row.revenue) > 900)
        .sort_values(["extra_7", "revenue"], [false, false])
        .head(150)
        .to_records().length,
    arquero: (table) =>
      table
        .filter((d) => d.extra_4 > 10 && d.revenue > 900)
        .orderby(aq.desc("extra_7"), aq.desc("revenue"))
        .slice(0, 150)
        .objects().length,
  },
];

const lines = [];
lines.push(`# bun_panda benchmark`);
lines.push(`rows=${ROWS}, iterations=${ITERS}`);
lines.push("");
lines.push("| case | dataset | bun_panda avg | arquero avg | delta | ratio (bun/aq) |");
lines.push("| --- | --- | ---: | ---: | ---: | ---: |");

for (const bench of cases) {
  const records = datasets[bench.dataset];
  const table = tables[bench.dataset];

  const bunStats = benchCase(`${bench.name}/bun_panda`, () => bench.bunPanda(records));
  const arqueroStats = benchCase(`${bench.name}/arquero`, () => bench.arquero(table));
  const delta = bunStats.avgMs - arqueroStats.avgMs;
  const deltaSign = delta > 0 ? "+" : "";
  const ratio = bunStats.avgMs / arqueroStats.avgMs;

  lines.push(
    `| ${bench.name} | ${bench.dataset} | ${formatMs(bunStats.avgMs)} | ${formatMs(
      arqueroStats.avgMs
    )} | ${deltaSign}${delta.toFixed(2)}ms | ${ratio.toFixed(2)}x |`
  );
}

console.log(lines.join("\n"));
