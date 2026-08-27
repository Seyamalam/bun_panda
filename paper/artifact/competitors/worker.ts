import { fileURLToPath } from "node:url";
import { DataFrame } from "../../../index";
import { buildDataset, canonicalize, digestRows, loadUciBankDataset, referenceOutput } from "./dataset";
import type { CanonicalRow, Dataset, DatasetSource, Scope, SystemName, Workload } from "./dataset";

function argument(name: string, fallback: string): string {
  const position = process.argv.indexOf(name);
  return position >= 0 ? process.argv[position + 1] ?? fallback : fallback;
}

const system = argument("--system", "bun_panda") as SystemName;
const workload = argument("--workload", "groupby_sum") as Workload;
const scope = argument("--scope", "operation") as Scope;
const rows = Number(argument("--rows", "10000"));
const seed = Number(argument("--seed", "20260826"));
const warmups = Number(argument("--warmups", "3"));
const iterations = Number(argument("--iterations", "10"));
const datasetSource = argument("--dataset", "synthetic") as DatasetSource;
const datasetPath = argument("--dataset-path", "paper/data/workloads/uci-bank/bank-full.csv");

interface BuiltSystem {
  run(workloadName: Workload): CanonicalRow[];
  dispose(): void;
}

type Builder = (dataset: Dataset) => BuiltSystem;

async function prepareBuilder(name: SystemName): Promise<{ builder: Builder; version: string }> {
  if (name === "bun_panda") {
    return {
      version: "0.4.0",
      builder: (dataset) => {
        const frame = new DataFrame(dataset.facts);
        const right = new DataFrame(dataset.right);
        return {
          run: (nextWorkload) => {
            if (nextWorkload === "groupby_sum") {
              return canonicalize(nextWorkload, frame.groupby("group").agg({ value: "sum" }).to_records()
                .map((row) => ({ group: String(row.group), value_sum: Number(row.value) })));
            }
            if (nextWorkload === "filter_sort_top100") {
              return canonicalize(nextWorkload, frame.query((row) => Number(row.value) >= 500)
                .sort_values("score", false, 100).to_records()
                .map((row) => ({ id: Number(row.id), score: Number(row.score) })));
            }
            if (nextWorkload === "value_counts") {
              return canonicalize(nextWorkload, frame.value_counts({ subset: ["group"] }).to_records()
                .map((row) => ({ group: String(row.group), count: Number(row.count) })));
            }
            return canonicalize(nextWorkload, frame.merge(right, { on: "id", how: "inner" }).to_records()
              .map((row) => ({ id: Number(row.id), rhs_value: Number(row.rhs_value) })));
          },
          dispose: () => {},
        };
      },
    };
  }

  if (name === "arquero") {
    const aq = await import("arquero");
    return {
      version: "8.0.3",
      builder: (dataset) => {
        const table = aq.from(dataset.facts);
        const right = aq.from(dataset.right);
        return {
          run: (nextWorkload) => {
            if (nextWorkload === "groupby_sum") {
              return canonicalize(nextWorkload, table.groupby("group")
                .rollup({ value_sum: (row: any) => aq.op.sum(row.value) }).objects() as CanonicalRow[]);
            }
            if (nextWorkload === "filter_sort_top100") {
              return canonicalize(nextWorkload, table
                .filter(aq.escape((row: any) => row.value >= 500))
                .orderby(aq.desc("score")).slice(0, 100).select("id", "score").objects() as CanonicalRow[]);
            }
            if (nextWorkload === "value_counts") {
              return canonicalize(nextWorkload, table.groupby("group")
                .rollup({ count: () => aq.op.count() }).objects() as CanonicalRow[]);
            }
            return canonicalize(nextWorkload, table.join(right, "id").select("id", "rhs_value").objects() as CanonicalRow[]);
          },
          dispose: () => {},
        };
      },
    };
  }

  if (name === "danfojs") {
    const danfo = await import("danfojs");
    return {
      version: "1.2.0",
      builder: (dataset) => {
        const frame = new danfo.DataFrame(dataset.facts);
        const right = new danfo.DataFrame(dataset.right);
        return {
          run: (nextWorkload) => {
            let output: any;
            if (nextWorkload === "groupby_sum") {
              output = frame.groupby(["group"]).col(["value"]).sum();
              return canonicalize(nextWorkload, output.values.map((row: any[]) => ({ group: row[0], value_sum: row[1] })));
            }
            if (nextWorkload === "filter_sort_top100") {
              output = frame.query(frame.column("value").ge(500)).sortValues("score", { ascending: false }).head(100);
              const idIndex = output.columns.indexOf("id");
              const scoreIndex = output.columns.indexOf("score");
              return canonicalize(nextWorkload, output.values.map((row: any[]) => ({ id: row[idIndex], score: row[scoreIndex] })));
            }
            if (nextWorkload === "value_counts") {
              output = frame.groupby(["group"]).col(["id"]).count();
              return canonicalize(nextWorkload, output.values.map((row: any[]) => ({ group: row[0], count: row[1] })));
            }
            output = danfo.merge({ left: frame, right, on: ["id"], how: "inner" });
            const idIndex = output.columns.indexOf("id");
            const rhsIndex = output.columns.indexOf("rhs_value");
            return canonicalize(nextWorkload, output.values.map((row: any[]) => ({ id: row[idIndex], rhs_value: row[rhsIndex] })));
          },
          dispose: () => {},
        };
      },
    };
  }

  if (name === "nodejs_polars") {
    const polars = await import("nodejs-polars");
    return {
      version: "0.26.0",
      builder: (dataset) => {
        const frame = polars.DataFrame(dataset.columns);
        const right = polars.DataFrame(dataset.rightColumns);
        return {
          run: (nextWorkload) => {
            let output: any;
            if (nextWorkload === "groupby_sum") {
              output = frame.groupBy("group").agg(polars.col("value").sum().alias("value_sum"));
            } else if (nextWorkload === "filter_sort_top100") {
              output = frame.filter(polars.col("value").gtEq(500)).sort({ by: "score", descending: true }).head(100)
                .select("id", "score");
            } else if (nextWorkload === "value_counts") {
              output = frame.groupBy("group").agg(polars.len().alias("count"));
            } else {
              output = frame.join(right, { on: "id", how: "inner" }).select("id", "rhs_value");
            }
            return canonicalize(nextWorkload, output.toRecords() as CanonicalRow[]);
          },
          dispose: () => {},
        };
      },
    };
  }

  const duckdb = await import("@duckdb/duckdb-wasm/blocking");
  const pathFor = (specifier: string) => fileURLToPath(import.meta.resolve(specifier));
  const bundles = {
    mvp: {
      mainModule: pathFor("@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm"),
      mainWorker: pathFor("@duckdb/duckdb-wasm/dist/duckdb-node-mvp.worker.cjs"),
    },
    eh: {
      mainModule: pathFor("@duckdb/duckdb-wasm/dist/duckdb-eh.wasm"),
      mainWorker: pathFor("@duckdb/duckdb-wasm/dist/duckdb-node-eh.worker.cjs"),
    },
  };
  const db = await duckdb.createDuckDB(bundles, new duckdb.VoidLogger(), duckdb.NODE_RUNTIME);
  await db.instantiate(() => {});
  db.open({});
  let sequence = 0;
  return {
    version: db.getVersion(),
    builder: (dataset) => {
      const suffix = sequence++;
      const factsFile = `facts_${suffix}.json`;
      const rightFile = `right_${suffix}.json`;
      const factsTable = `facts_${suffix}`;
      const rightTable = `right_${suffix}`;
      db.registerFileText(factsFile, JSON.stringify(dataset.facts));
      db.registerFileText(rightFile, JSON.stringify(dataset.right));
      const connection = db.connect();
      connection.query(`CREATE TABLE ${factsTable} AS SELECT * FROM read_json_auto('${factsFile}')`);
      connection.query(`CREATE TABLE ${rightTable} AS SELECT * FROM read_json_auto('${rightFile}')`);
      return {
        run: (nextWorkload) => {
          let query: string;
          if (nextWorkload === "groupby_sum") {
            query = `SELECT "group", SUM(value)::DOUBLE AS value_sum FROM ${factsTable} GROUP BY "group"`;
          } else if (nextWorkload === "filter_sort_top100") {
            query = `SELECT id, score FROM ${factsTable} WHERE value >= 500 ORDER BY score DESC LIMIT 100`;
          } else if (nextWorkload === "value_counts") {
            query = `SELECT "group", COUNT(*)::DOUBLE AS count FROM ${factsTable} GROUP BY "group"`;
          } else {
            query = `SELECT f.id, r.rhs_value FROM ${factsTable} f INNER JOIN ${rightTable} r USING (id)`;
          }
          return canonicalize(nextWorkload, connection.query(query).toArray().map((row: any) => row.toJSON()) as CanonicalRow[]);
        },
        dispose: () => {
          connection.query(`DROP TABLE ${factsTable}`);
          connection.query(`DROP TABLE ${rightTable}`);
          connection.close();
          db.dropFiles([factsFile, rightFile]);
        },
      };
    },
  };
}

const dataset = datasetSource === "uci_bank"
  ? await loadUciBankDataset(datasetPath, rows)
  : buildDataset(rows, seed);
const expectedDigest = digestRows(referenceOutput(dataset, workload));
const prepared = await prepareBuilder(system);

function executeOnce(existing?: BuiltSystem): { output: CanonicalRow[]; built: BuiltSystem | null } {
  if (scope === "operation") {
    if (!existing) throw new Error("operation scope requires a constructed system");
    return { output: existing.run(workload), built: null };
  }
  const built = prepared.builder(dataset);
  const output = built.run(workload);
  return { output, built };
}

const persistent = scope === "operation" ? prepared.builder(dataset) : undefined;
let output: CanonicalRow[] = [];
for (let index = 0; index < warmups; index += 1) {
  const result = executeOnce(persistent);
  output = result.output;
  result.built?.dispose();
}

const samplesMs: number[] = [];
let observedRssPeak = process.memoryUsage().rss;
for (let index = 0; index < iterations; index += 1) {
  const started = Bun.nanoseconds();
  const result = executeOnce(persistent);
  samplesMs.push((Bun.nanoseconds() - started) / 1_000_000);
  output = result.output;
  result.built?.dispose();
  observedRssPeak = Math.max(observedRssPeak, process.memoryUsage().rss);
}
persistent?.dispose();

const digest = digestRows(output);
if (digest !== expectedDigest) {
  throw new Error(`${system} produced ${digest}; reference is ${expectedDigest} for ${workload}`);
}

process.stdout.write(`${JSON.stringify({
  schemaVersion: "1.0.0",
  system,
  systemVersion: prepared.version,
  workload,
  scope,
  rows,
  seed,
  datasetSource,
  warmups,
  iterations,
  samplesMs,
  digest,
  outputRows: output.length,
  memory: {
    observedRssPeak,
    finalRss: process.memoryUsage().rss,
    maxRssKilobytes: process.resourceUsage().maxRSS,
  },
})}\n`);
