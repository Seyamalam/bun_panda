import * as aq from "arquero";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { DataFrame } from "../index";

const ROWS = Number(process.env.BUN_PANDA_BENCH_ROWS ?? 25000);
const ITERS = Number(process.env.BUN_PANDA_BENCH_ITERS ?? 8);
const ROUNDS = Number(process.env.BUN_PANDA_BENCH_ROUNDS ?? 3);
const JSON_OUT = process.env.BUN_PANDA_BENCH_JSON ?? "bench/results/arquero.json";

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
  const highCardinality = options.highCardinality ?? false;
  const includeMissing = options.includeMissing ?? false;
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
      city: includeMissing && id % 23 === 0 ? null : cities[id % cities.length],
      segment: includeMissing && id % 31 === 0 ? null : segments[id % segments.length],
      value,
      weight,
      revenue: Number((value * weight).toFixed(2)),
      active: id % 3 === 0,
      bucket: value > 700 ? "high" : value > 350 ? "mid" : "low",
      user_key: highCardinality ? `u_${id}` : `u_${id % 120}`,
      session_key: highCardinality ? `s_${id * 7}` : `s_${id % 300}`,
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

function benchCase(name, fn, iterations = ITERS, rounds = ROUNDS) {
  for (let i = 0; i < 3; i += 1) {
    fn();
  }

  const roundAverages = [];

  for (let round = 0; round < rounds; round += 1) {
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
    roundAverages.push(total / times.length);
  }

  const avgMs = median(roundAverages);
  return {
    avgMs,
    roundAverages,
  };
}

function formatMs(value) {
  return `${value.toFixed(2)}ms`;
}

const datasets = {
  base: buildDataset(ROWS),
  skewed: buildDataset(ROWS, { skew: true }),
  wide: buildDataset(ROWS, { wide: true }),
  high_card: buildDataset(ROWS, { highCardinality: true }),
  missing: buildDataset(ROWS, { includeMissing: true }),
};

const tables = {
  base: aq.from(datasets.base),
  skewed: aq.from(datasets.skewed),
  wide: aq.from(datasets.wide),
  high_card: aq.from(datasets.high_card),
  missing: aq.from(datasets.missing),
};

const frames = {
  base: new DataFrame(datasets.base),
  skewed: new DataFrame(datasets.skewed),
  wide: new DataFrame(datasets.wide),
  high_card: new DataFrame(datasets.high_card),
  missing: new DataFrame(datasets.missing),
};

const op = aq.op;

function makeSortCase(name, dataset, by, ascending, limit, predicateBun, predicateAq) {
  return {
    name,
    dataset,
    bunPanda: (_records, frame) => {
      const input = predicateBun ? frame.query(predicateBun) : frame;
      return input.sort_values(by, ascending, limit).shape[0];
    },
    arquero: (table) => {
      const input = predicateAq ? table.filter(predicateAq) : table;
      const orderArgs = toArqueroOrderArgs(by, ascending);
      return input.orderby(...orderArgs).slice(0, limit).numRows();
    },
  };
}

function makeValueCountCase(name, dataset, subset, options = {}) {
  const subsetArr = Array.isArray(subset) ? subset : [subset];
  const limit = options.limit;
  const normalize = options.normalize ?? false;
  const dropna = options.dropna ?? true;

  return {
    name,
    dataset,
    bunPanda: (_records, frame) =>
      frame.value_counts({ subset: subsetArr, limit, normalize, dropna }).shape[0],
    arquero: (table) => {
      let input = table;
      if (dropna) {
        for (const column of subsetArr) {
          input = input.filter(aq.escape((d) => d[column] != null));
        }
      }

      let out = input
        .groupby(...subsetArr)
        .rollup({ count: () => op.count() })
        .orderby(aq.desc("count"), ...subsetArr);

      if (normalize) {
        const denominator = input.numRows();
        out = out
          .derive({
            proportion: aq.escape((d) => (denominator > 0 ? d.count / denominator : 0)),
          })
          .select(...subsetArr, "proportion");
      }

      if (limit !== undefined) {
        out = out.slice(0, limit);
      }
      return out.numRows();
    },
  };
}

function makeGroupByCase(name, dataset, by, aggBun, aggAq) {
  const keys = Array.isArray(by) ? by : [by];
  return {
    name,
    dataset,
    bunPanda: (_records, frame) => frame.groupby(keys).agg(aggBun).shape[0],
    arquero: (table) => table.groupby(...keys).rollup(aggAq).numRows(),
  };
}

function toArqueroOrderArgs(by, ascending) {
  const cols = Array.isArray(by) ? by : [by];
  const ascArr = Array.isArray(ascending)
    ? ascending
    : Array.from({ length: cols.length }, () => ascending);
  return cols.map((col, i) => (ascArr[i] ? col : aq.desc(col)));
}

const groupCases = [
  makeGroupByCase(
    "groupby_mean",
    "base",
    "group",
    { value: "mean", revenue: "sum" },
    {
      value: (d) => op.mean(d.value),
      revenue: (d) => op.sum(d.revenue),
    }
  ),
  makeGroupByCase(
    "skewed_groupby_mean",
    "skewed",
    "group",
    { value: "mean", revenue: "sum" },
    {
      value: (d) => op.mean(d.value),
      revenue: (d) => op.sum(d.revenue),
    }
  ),
  makeGroupByCase(
    "groupby_sum_value",
    "base",
    "group",
    { value: "sum" },
    {
      value: (d) => op.sum(d.value),
    }
  ),
  makeGroupByCase(
    "groupby_count_city_non_missing",
    "missing",
    "group",
    { city: "count" },
    {
      city: (d) => op.sum(d.city != null ? 1 : 0),
    }
  ),
  makeGroupByCase(
    "groupby_wide_group_sum",
    "wide",
    "group",
    { extra_7: "sum", revenue: "sum" },
    {
      extra_7: (d) => op.sum(d.extra_7),
      revenue: (d) => op.sum(d.revenue),
    }
  ),
  makeGroupByCase(
    "groupby_high_card_city_mean",
    "high_card",
    "city",
    { value: "mean", revenue: "mean" },
    {
      value: (d) => op.mean(d.value),
      revenue: (d) => op.mean(d.revenue),
    }
  ),
  makeGroupByCase(
    "groupby_missing_city_mean",
    "missing",
    "city",
    { value: "mean" },
    {
      value: (d) => op.mean(d.value),
    }
  ),
];

const sortCases = [
  makeSortCase("filter_sort_top50", "base", "value", false, 50, (row) => row.active && row.value > 500, (d) => d.active && d.value > 500),
  makeSortCase("filter_sort_top100", "base", "value", false, 100, (row) => row.active && row.value > 500, (d) => d.active && d.value > 500),
  makeSortCase("filter_sort_top250", "base", "value", false, 250, (row) => row.active && row.value > 500, (d) => d.active && d.value > 500),
  makeSortCase("filter_sort_top500", "base", "value", false, 500, (row) => row.active && row.value > 500, (d) => d.active && d.value > 500),

  makeSortCase("filter_sort_multicol_top100", "base", ["group", "value", "id"], [true, false, true], 100, (row) => row.value > 300, (d) => d.value > 300),
  makeSortCase("filter_sort_multicol_top200", "base", ["group", "value", "id"], [true, false, true], 200, (row) => row.value > 300, (d) => d.value > 300),
  makeSortCase("filter_sort_multicol_top400", "base", ["group", "value", "id"], [true, false, true], 400, (row) => row.value > 300, (d) => d.value > 300),

  makeSortCase("sort_top50", "base", "value", false, 50),
  makeSortCase("sort_top100", "base", "value", false, 100),
  makeSortCase("sort_top300", "base", "value", false, 300),
  makeSortCase("sort_top700", "base", "value", false, 700),
  makeSortCase("sort_top1000", "base", "value", false, 1000),
  makeSortCase("sort_top1500", "base", "value", false, 1500),
  makeSortCase("sort_top2000", "base", "value", false, 2000),
  makeSortCase("sort_top2500", "base", "value", false, 2500),
  makeSortCase("sort_multicol_top150", "base", ["city", "value"], [true, false], 150),
  makeSortCase("sort_multicol_top350", "base", ["city", "value", "id"], [true, false, true], 350),
  makeSortCase("sort_multicol_top800", "base", ["city", "value", "id"], [true, false, true], 800),
  makeSortCase("sort_multicol_top1200", "base", ["city", "value", "id"], [true, false, true], 1200),

  makeSortCase("wide_sort_top200", "wide", "extra_7", false, 200),
  makeSortCase("wide_sort_top500", "wide", "extra_7", false, 500),
  makeSortCase("wide_sort_multicol_top300", "wide", ["extra_7", "revenue"], [false, false], 300),
  makeSortCase("wide_sort_multicol_top600", "wide", ["extra_7", "revenue"], [false, false], 600),

  makeSortCase("skewed_sort_top400", "skewed", ["group", "value"], [true, false], 400),
  makeSortCase("skewed_sort_top800", "skewed", ["group", "value"], [true, false], 800),
  makeSortCase("skewed_sort_multicol_top220", "skewed", ["group", "city", "value"], [true, true, false], 220),
  makeSortCase("skewed_sort_multicol_top700", "skewed", ["group", "city", "value"], [true, true, false], 700),

  makeSortCase("wide_filter_sort_top80", "wide", ["extra_7", "revenue"], [false, false], 80, (row) => row.extra_4 > 10 && row.revenue > 900, (d) => d.extra_4 > 10 && d.revenue > 900),
  makeSortCase("wide_filter_sort_top150", "wide", ["extra_7", "revenue"], [false, false], 150, (row) => row.extra_4 > 10 && row.revenue > 900, (d) => d.extra_4 > 10 && d.revenue > 900),
  makeSortCase("wide_filter_sort_top300", "wide", ["extra_7", "revenue"], [false, false], 300, (row) => row.extra_4 > 10 && row.revenue > 900, (d) => d.extra_4 > 10 && d.revenue > 900),
  makeSortCase("skewed_filter_sort_top120", "skewed", ["group", "value"], [true, false], 120, (row) => row.value > 250, (d) => d.value > 250),

  makeSortCase("high_card_sort_top300", "high_card", "value", false, 300),
  makeSortCase("high_card_sort_multicol_top400", "high_card", ["city", "value", "id"], [true, false, true], 400),
  makeSortCase("high_card_filter_sort_top200", "high_card", ["city", "value"], [true, false], 200, (row) => row.value > 450, (d) => d.value > 450),
];

const valueCountCases = [
  makeValueCountCase("value_counts_city", "base", "city"),
  makeValueCountCase("value_counts_city_top3", "base", "city", { limit: 3 }),
  makeValueCountCase("value_counts_city_top1", "base", "city", { limit: 1 }),
  makeValueCountCase("value_counts_city_normalize", "base", "city", { normalize: true }),
  makeValueCountCase("value_counts_city_top3_normalize", "base", "city", {
    normalize: true,
    limit: 3,
  }),

  makeValueCountCase("value_counts_group_city", "base", ["group", "city"]),
  makeValueCountCase("value_counts_group_city_top10", "base", ["group", "city"], { limit: 10 }),
  makeValueCountCase("value_counts_group_city_top5", "base", ["group", "city"], { limit: 5 }),
  makeValueCountCase("value_counts_group_city_top1", "base", ["group", "city"], { limit: 1 }),
  makeValueCountCase("value_counts_group_city_normalize", "base", ["group", "city"], {
    normalize: true,
  }),
  makeValueCountCase(
    "value_counts_group_city_top10_normalize",
    "base",
    ["group", "city"],
    { normalize: true, limit: 10 }
  ),

  makeValueCountCase("value_counts_group_segment", "base", ["group", "segment"]),
  makeValueCountCase("value_counts_group_segment_top5", "base", ["group", "segment"], {
    limit: 5,
  }),
  makeValueCountCase("value_counts_bucket", "base", "bucket"),
  makeValueCountCase("value_counts_bucket_top2", "base", "bucket", { limit: 2 }),
  makeValueCountCase("value_counts_segment", "base", "segment"),
  makeValueCountCase("value_counts_segment_top2", "base", "segment", { limit: 2 }),
  makeValueCountCase("value_counts_city_segment", "base", ["city", "segment"]),
  makeValueCountCase("value_counts_city_segment_top8", "base", ["city", "segment"], {
    limit: 8,
  }),

  makeValueCountCase("value_counts_skewed_group", "skewed", "group"),
  makeValueCountCase("value_counts_skewed_group_top3", "skewed", "group", { limit: 3 }),
  makeValueCountCase(
    "value_counts_skewed_group_city_top10",
    "skewed",
    ["group", "city"],
    { limit: 10 }
  ),

  makeValueCountCase("value_counts_wide_extra7", "wide", "extra_7"),
  makeValueCountCase("value_counts_wide_extra7_top10", "wide", "extra_7", { limit: 10 }),

  makeValueCountCase("value_counts_missing_city_dropna_true", "missing", "city", {
    dropna: true,
  }),
  makeValueCountCase("value_counts_missing_city_dropna_false", "missing", "city", {
    dropna: false,
  }),
  makeValueCountCase(
    "value_counts_missing_city_segment_dropna_false_top20",
    "missing",
    ["city", "segment"],
    { dropna: false, limit: 20 }
  ),

  makeValueCountCase("value_counts_high_card_city_top20", "high_card", ["user_key", "city"], {
    limit: 20,
  }),
  makeValueCountCase("value_counts_high_card_city_top50", "high_card", ["user_key", "city"], {
    limit: 50,
  }),
  makeValueCountCase("value_counts_high_card_user_top100", "high_card", "user_key", {
    limit: 100,
  }),
  makeValueCountCase("value_counts_high_card_session_top100", "high_card", "session_key", {
    limit: 100,
  }),
  makeValueCountCase(
    "value_counts_high_card_user_city_top100",
    "high_card",
    ["user_key", "city"],
    { limit: 100 }
  ),
];

const cases = [...groupCases, ...sortCases, ...valueCountCases];

const lines = [];
lines.push(`# bun_panda benchmark`);
lines.push(`rows=${ROWS}, iterations=${ITERS}`);
lines.push(`rounds=${ROUNDS}`);
lines.push(`cases=${cases.length}`);
lines.push("");
lines.push("| case | dataset | bun_panda avg | arquero avg | delta | ratio (bun/aq) |");
lines.push("| --- | --- | ---: | ---: | ---: | ---: |");
const results = [];

for (const bench of cases) {
  const records = datasets[bench.dataset];
  const table = tables[bench.dataset];
  const frame = frames[bench.dataset];

  const bunStats = benchCase(`${bench.name}/bun_panda`, () => bench.bunPanda(records, frame));
  const arqueroStats = benchCase(`${bench.name}/arquero`, () => bench.arquero(table, records));
  const delta = bunStats.avgMs - arqueroStats.avgMs;
  const deltaSign = delta > 0 ? "+" : "";
  const ratio = bunStats.avgMs / arqueroStats.avgMs;
    results.push({
      case: bench.name,
      dataset: bench.dataset,
      bunAvgMs: bunStats.avgMs,
      arqueroAvgMs: arqueroStats.avgMs,
      bunRoundAverages: bunStats.roundAverages,
      arqueroRoundAverages: arqueroStats.roundAverages,
      deltaMs: delta,
      ratio,
    });

  lines.push(
    `| ${bench.name} | ${bench.dataset} | ${formatMs(bunStats.avgMs)} | ${formatMs(
      arqueroStats.avgMs
    )} | ${deltaSign}${delta.toFixed(2)}ms | ${ratio.toFixed(2)}x |`
  );
}

console.log(lines.join("\n"));

if (JSON_OUT) {
  const payload = {
    generatedAt: new Date().toISOString(),
    rows: ROWS,
    iterations: ITERS,
    rounds: ROUNDS,
    cases: results.length,
    results,
  };
  mkdirSync(dirname(JSON_OUT), { recursive: true });
  writeFileSync(JSON_OUT, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

function median(values) {
  if (!values.length) {
    return 0;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return sorted[mid];
  }
  return (sorted[mid - 1] + sorted[mid]) / 2;
}
