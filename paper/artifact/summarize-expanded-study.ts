import { readFileSync, writeFileSync } from "node:fs";

const competitor = JSON.parse(readFileSync("paper/data/competitor-study.json", "utf8"));
const publicCompetitor = JSON.parse(readFileSync("paper/data/competitor-uci-bank.json", "utf8"));
const browser = JSON.parse(readFileSync("paper/data/browser-study.json", "utf8"));

function mean(values: number[]): number {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function median(values: number[]): number {
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
}

function percentile(values: number[], q: number): number {
  const sorted = [...values].sort((left, right) => left - right);
  const position = (sorted.length - 1) * q;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  if (lower === upper) return sorted[lower];
  return sorted[lower] * (upper - position) + sorted[upper] * (position - lower);
}

function bootstrapMedianInterval(values: number[], seed: number) {
  let state = seed >>> 0;
  const random = () => {
    state = (Math.imul(1664525, state) + 1013904223) >>> 0;
    return state / 0x100000000;
  };
  const estimates = Array.from({ length: 2000 }, () => median(
    Array.from({ length: values.length }, () => values[Math.floor(random() * values.length)]),
  ));
  return {
    lower95: percentile(estimates, 0.025),
    upper95: percentile(estimates, 0.975),
    bootstrapReplicates: estimates.length,
  };
}

const competitorWorkloads = ["groupby_sum", "filter_sort_top100", "value_counts", "inner_join"];
const competitorSystems = ["bun_panda", "arquero", "danfojs", "nodejs_polars", "duckdb_wasm"];
const competitor50k = Object.fromEntries(["operation", "load_and_operation"].map((scope) => [
  scope,
  Object.fromEntries(competitorWorkloads.map((workload) => {
    const cell = competitor.cells.find((entry: any) =>
      entry.rows === 50000 && entry.scope === scope && entry.workload === workload
    );
    const medians = Object.fromEntries(competitorSystems.map((system) =>
      [system, cell.systems[system].medianOfProcessMeansMs]
    ));
    return [workload, {
      mediansMs: medians,
      speedupVsBunPanda: Object.fromEntries(competitorSystems.slice(1).map((system) =>
        [system, medians[system] / medians.bun_panda]
      )),
    }];
  })),
]));

const public45211 = Object.fromEntries(["operation", "load_and_operation"].map((scope) => [
  scope,
  Object.fromEntries(competitorWorkloads.map((workload) => {
    const cell = publicCompetitor.cells.find((entry: any) =>
      entry.rows === 45211 && entry.scope === scope && entry.workload === workload
    );
    return [workload, {
      mediansMs: Object.fromEntries(competitorSystems.map((system) =>
        [system, cell.systems[system].medianOfProcessMeansMs]
      )),
      equivalent: cell.equivalent,
    }];
  })),
]));

const browserNames = ["chromium", "firefox", "webkit"];
const browserWorkloads = ["stable_argsort", "filter_indices", "grouped_sum"];
const browser100k = Object.fromEntries(browserNames.map((browserName) => {
  const selected = browser.raw.filter((entry: any) => entry.browser === browserName && entry.rows === 100000);
  return [browserName, {
    version: selected[0].browserVersion,
    medianWasmInitMs: median(selected.map((entry: any) => entry.wasmInitMs)),
    capabilities: selected[0].capabilities,
    workloads: Object.fromEntries(browserWorkloads.map((workload, workloadIndex) => {
      const processSpeedups = selected.map((entry: any) =>
        mean(entry.results[workload].jsSamplesMs) / mean(entry.results[workload].wasmSamplesMs)
      );
      return [workload, {
        medianSpeedup: median(processSpeedups),
        ...bootstrapMedianInterval(
          processSpeedups,
          20260826 + browserNames.indexOf(browserName) * 100 + workloadIndex,
        ),
        processSpeedups,
      }];
    })),
  }];
}));

const payload = {
  generatedAt: new Date().toISOString(),
  competitor: {
    processes: competitor.raw.length,
    timedIterations: competitor.raw.reduce((sum: number, entry: any) => sum + entry.samplesMs.length, 0),
    cells: competitor.cells.length,
    rows50000: competitor50k,
  },
  publicWorkload: {
    dataset: "UCI Bank Marketing",
    rows: 45211,
    processes: publicCompetitor.raw.length,
    timedIterations: publicCompetitor.raw.reduce(
      (sum: number, entry: any) => sum + entry.samplesMs.length,
      0,
    ),
    cells: publicCompetitor.cells.length,
    results: public45211,
  },
  browser: {
    contexts: browser.raw.length,
    timedCalls: browser.raw.reduce((sum: number, entry: any) => sum +
      Object.values(entry.results).reduce((subtotal: number, result: any) =>
        subtotal + result.jsSamplesMs.length + result.wasmSamplesMs.length, 0), 0),
    rows100000: browser100k,
  },
};
writeFileSync("paper/data/expanded-summary.json", `${JSON.stringify(payload, null, 2)}\n`, "utf8");

function macro(name: string, value: string | number): string {
  return `\\newcommand{\\${name}}{${value}}`;
}

const op = competitor50k.operation;
const load = competitor50k.load_and_operation;
const labels: Record<string, string> = {
  bun_panda: "BunPanda",
  arquero: "Arquero",
  danfojs: "Danfo",
  nodejs_polars: "Polars",
  duckdb_wasm: "DuckDB",
};
const workloadLabels: Record<string, string> = {
  groupby_sum: "Group",
  filter_sort_top100: "Filter",
  value_counts: "Counts",
  inner_join: "Join",
};
const lines = [
  "% Generated by paper/artifact/summarize-expanded-study.ts; do not edit manually.",
  macro("CompetitorProcesses", payload.competitor.processes.toLocaleString("en-US")),
  macro("CompetitorIterations", payload.competitor.timedIterations.toLocaleString("en-US")),
  macro("CompetitorCells", payload.competitor.cells),
  macro("PublicProcesses", payload.publicWorkload.processes.toLocaleString("en-US")),
  macro("PublicIterations", payload.publicWorkload.timedIterations.toLocaleString("en-US")),
  macro("PublicCells", payload.publicWorkload.cells),
  macro("BrowserContexts", payload.browser.contexts),
  macro("BrowserTimedCalls", payload.browser.timedCalls.toLocaleString("en-US")),
];
for (const workload of competitorWorkloads) {
  for (const system of competitorSystems) {
    lines.push(macro(`${workloadLabels[workload]}${labels[system]}OpMs`, op[workload].mediansMs[system].toFixed(2)));
    lines.push(macro(`${workloadLabels[workload]}${labels[system]}LoadMs`, load[workload].mediansMs[system].toFixed(2)));
  }
}
for (const workload of competitorWorkloads) {
  for (const system of competitorSystems) {
    lines.push(macro(
      `Public${workloadLabels[workload]}${labels[system]}OpMs`,
      public45211.operation[workload].mediansMs[system].toFixed(2),
    ));
    lines.push(macro(
      `Public${workloadLabels[workload]}${labels[system]}LoadMs`,
      public45211.load_and_operation[workload].mediansMs[system].toFixed(2),
    ));
  }
}
for (const browserName of browserNames) {
  const label = browserName[0].toUpperCase() + browserName.slice(1);
  lines.push(macro(`${label}Version`, browser100k[browserName].version));
  lines.push(macro(`${label}InitMs`, browser100k[browserName].medianWasmInitMs.toFixed(2)));
  for (const workload of browserWorkloads) {
    const workloadLabel = workload === "stable_argsort" ? "Sort" : workload === "filter_indices" ? "Filter" : "Group";
    lines.push(macro(`${label}${workloadLabel}Speedup`, browser100k[browserName].workloads[workload].medianSpeedup.toFixed(2)));
    lines.push(macro(`${label}${workloadLabel}Lower`, browser100k[browserName].workloads[workload].lower95.toFixed(2)));
    lines.push(macro(`${label}${workloadLabel}Upper`, browser100k[browserName].workloads[workload].upper95.toFixed(2)));
  }
}
writeFileSync("paper/manuscript/generated-expanded-results.tex", `${lines.join("\n")}\n`, "utf8");
console.log(JSON.stringify(payload, null, 2));
