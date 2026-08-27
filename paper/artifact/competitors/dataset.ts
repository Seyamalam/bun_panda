export type Workload = "groupby_sum" | "filter_sort_top100" | "value_counts" | "inner_join";
export type SystemName = "bun_panda" | "arquero" | "danfojs" | "nodejs_polars" | "duckdb_wasm";
export type Scope = "operation" | "load_and_operation";

export interface FactRow {
  id: number;
  group: string;
  value: number;
  score: number;
  weight: number;
}

export interface RightRow {
  id: number;
  rhs_value: number;
}

export interface Dataset {
  rows: number;
  seed: number;
  facts: FactRow[];
  right: RightRow[];
  columns: {
    id: number[];
    group: string[];
    value: number[];
    score: number[];
    weight: number[];
  };
  rightColumns: { id: number[]; rhs_value: number[] };
}

export type DatasetSource = "synthetic" | "uci_bank";

export type CanonicalRow = Record<string, number | string>;

function lcg(initialSeed: number): () => number {
  let state = initialSeed >>> 0;
  return () => {
    state = (Math.imul(1664525, state) + 1013904223) >>> 0;
    return state / 2 ** 32;
  };
}

export function buildDataset(rows: number, seed: number): Dataset {
  const random = lcg(seed);
  const facts = new Array<FactRow>(rows);
  const id = new Array<number>(rows);
  const group = new Array<string>(rows);
  const value = new Array<number>(rows);
  const score = new Array<number>(rows);
  const weight = new Array<number>(rows);
  const groups = ["A", "B", "C", "D", "E", "F", "G", "H"];

  for (let index = 0; index < rows; index += 1) {
    const nextValue = Math.floor(random() * 1_000);
    const nextScore = nextValue * 1_000_000 + index;
    const nextWeight = Math.floor(random() * 500) + 50;
    const nextGroup = groups[(index * 17 + nextValue) % groups.length]!;
    const row = { id: index, group: nextGroup, value: nextValue, score: nextScore, weight: nextWeight };
    facts[index] = row;
    id[index] = row.id;
    group[index] = row.group;
    value[index] = row.value;
    score[index] = row.score;
    weight[index] = row.weight;
  }

  const right: RightRow[] = [];
  for (let index = 0; index < rows; index += 2) right.push({ id: index, rhs_value: index * 3 + 7 });

  return {
    rows,
    seed,
    facts,
    right,
    columns: { id, group, value, score, weight },
    rightColumns: {
      id: right.map((row) => row.id),
      rhs_value: right.map((row) => row.rhs_value),
    },
  };
}

function unquote(value: string): string {
  return value.startsWith('"') && value.endsWith('"') ? value.slice(1, -1).replaceAll('""', '"') : value;
}

export async function loadUciBankDataset(path: string, rowLimit?: number): Promise<Dataset> {
  const lines = (await Bun.file(path).text()).trim().split(/\r?\n/);
  const headers = lines[0]!.split(";").map(unquote);
  const index = Object.fromEntries(headers.map((name, position) => [name, position])) as Record<string, number>;
  const available = lines.length - 1;
  const rows = Math.min(rowLimit ?? available, available);
  const facts = new Array<FactRow>(rows);
  const id = new Array<number>(rows);
  const group = new Array<string>(rows);
  const value = new Array<number>(rows);
  const score = new Array<number>(rows);
  const weight = new Array<number>(rows);
  for (let rowIndex = 0; rowIndex < rows; rowIndex += 1) {
    const fields = lines[rowIndex + 1]!.split(";").map(unquote);
    const nextValue = Number(fields[index.balance]);
    const nextScore = Number(fields[index.duration]) * 1_000_000 + rowIndex;
    const row = {
      id: rowIndex,
      group: fields[index.job]!,
      value: nextValue,
      score: nextScore,
      weight: Number(fields[index.age]),
    };
    facts[rowIndex] = row;
    id[rowIndex] = row.id;
    group[rowIndex] = row.group;
    value[rowIndex] = row.value;
    score[rowIndex] = row.score;
    weight[rowIndex] = row.weight;
  }
  const right: RightRow[] = [];
  for (let rowIndex = 0; rowIndex < rows; rowIndex += 2) right.push({ id: rowIndex, rhs_value: rowIndex * 3 + 7 });
  return {
    rows,
    seed: 0,
    facts,
    right,
    columns: { id, group, value, score, weight },
    rightColumns: {
      id: right.map((row) => row.id),
      rhs_value: right.map((row) => row.rhs_value),
    },
  };
}

export function canonicalize(workload: Workload, rows: CanonicalRow[]): CanonicalRow[] {
  const normalized = rows.map((row) => {
    if (workload === "groupby_sum") return { group: String(row.group), value_sum: Number(row.value_sum) };
    if (workload === "filter_sort_top100") return { id: Number(row.id), score: Number(row.score) };
    if (workload === "value_counts") return { group: String(row.group), count: Number(row.count) };
    return { id: Number(row.id), rhs_value: Number(row.rhs_value) };
  });

  if (workload === "groupby_sum") {
    return normalized.sort((left, right) => String(left.group).localeCompare(String(right.group)));
  }
  if (workload === "filter_sort_top100") {
    return normalized.sort((left, right) => Number(right.score) - Number(left.score));
  }
  if (workload === "value_counts") {
    return normalized.sort((left, right) =>
      Number(right.count) - Number(left.count) || String(left.group).localeCompare(String(right.group))
    );
  }
  return normalized.sort((left, right) => Number(left.id) - Number(right.id));
}

export function referenceOutput(dataset: Dataset, workload: Workload): CanonicalRow[] {
  if (workload === "groupby_sum") {
    const sums = new Map<string, number>();
    for (const row of dataset.facts) sums.set(row.group, (sums.get(row.group) ?? 0) + row.value);
    return canonicalize(workload, [...sums].map(([group, value_sum]) => ({ group, value_sum })));
  }
  if (workload === "filter_sort_top100") {
    return canonicalize(workload, dataset.facts
      .filter((row) => row.value >= 500)
      .sort((left, right) => right.score - left.score)
      .slice(0, 100)
      .map(({ id, score }) => ({ id, score })));
  }
  if (workload === "value_counts") {
    const counts = new Map<string, number>();
    for (const row of dataset.facts) counts.set(row.group, (counts.get(row.group) ?? 0) + 1);
    return canonicalize(workload, [...counts].map(([group, count]) => ({ group, count })));
  }
  return canonicalize(workload, dataset.right.map(({ id, rhs_value }) => ({ id, rhs_value })));
}

export function digestRows(rows: CanonicalRow[]): string {
  return new Bun.CryptoHasher("sha256").update(JSON.stringify(rows)).digest("hex");
}
