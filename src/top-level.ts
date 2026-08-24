// Top-level pandas module surface: options registry, scalar/index types,
// range builders, and reshape/merge wrappers.
import { DataFrame } from "./dataframe";
import type { CellValue, IndexLabel, Row } from "./types";

// ---------------------------------------------------------------------------
// Options registry
// ---------------------------------------------------------------------------

const options = new Map<string, CellValue>([
  ["mode.chained_assignment", "warn"],
  ["mode.copy_on_write", false],
  ["display.max_rows", 60],
  ["display.max_columns", 0],
  ["display.precision", 6],
]);

export function get_option(key: string): CellValue {
  if (!options.has(key)) {
    throw new Error(`Option '${key}' does not exist.`);
  }
  return options.get(key)!;
}

export function set_option(key: string, value: CellValue): void {
  if (!options.has(key)) {
    throw new Error(`Option '${key}' does not exist.`);
  }
  options.set(key, value);
}

export function reset_option(key: string): void {
  if (!options.has(key)) {
    throw new Error(`Option '${key}' does not exist.`);
  }
  // Re-running the registry seed for a single key is enough for our defaults.
  const defaults = new Map<string, CellValue>([
    ["mode.chained_assignment", "warn"],
    ["mode.copy_on_write", false],
    ["display.max_rows", 60],
    ["display.max_columns", 0],
    ["display.precision", 6],
  ]);
  options.set(key, defaults.get(key)!);
}

export function describe_option(key?: string): string {
  if (key !== undefined) {
    if (!options.has(key)) throw new Error(`Option '${key}' does not exist.`);
    return `${key} = ${String(options.get(key))}`;
  }
  return [...options.keys()].map((k) => describe_option(k)).join("\n");
}

export function option_context(
  updates: Record<string, CellValue>,
  fn: () => void
): void {
  const previous = new Map<string, CellValue>();
  for (const [k, v] of Object.entries(updates)) {
    previous.set(k, get_option(k));
    set_option(k, v);
  }
  try {
    fn();
  } finally {
    for (const [k, v] of previous) options.set(k, v);
  }
}

// ---------------------------------------------------------------------------
// Missing-value sentinels
// ---------------------------------------------------------------------------

/** Singleton NA marker (pandas pd.NA). */
export const NA = Symbol.for("bun_panda.NA");
/** Singleton NaT marker (pandas pd.NaT). */
export const NaT = Symbol.for("bun_panda.NaT");

// ---------------------------------------------------------------------------
// Minimal scalar / index types
// ---------------------------------------------------------------------------

export class Timestamp {
  readonly date: Date;

  constructor(value: Date | string | number) {
    this.date = value instanceof Date ? new Date(value.getTime()) : new Date(value);
  }

  static now(): Timestamp {
    return new Timestamp(new Date());
  }

  add(other: Timedelta | number): Timestamp {
    const ms = other instanceof Timedelta ? other.ms : other;
    return new Timestamp(this.date.getTime() + ms);
  }

  sub(other: Timedelta | number | Timestamp): Timestamp | Timedelta {
    if (other instanceof Timestamp) {
      return new Timedelta(this.date.getTime() - other.date.getTime());
    }
    const ms = other instanceof Timedelta ? other.ms : other;
    return new Timestamp(this.date.getTime() - ms);
  }

  valueOf(): number {
    return this.date.getTime();
  }

  toString(): string {
    return this.date.toISOString();
  }
}

export class Timedelta {
  readonly ms: number;

  constructor(value: number | string) {
    if (typeof value === "number") {
      this.ms = value;
    } else {
      // Accept the pandas-style "1 days 02:03:04" / "02:03:04" strings.
      let total = 0;
      const dayMatch = value.match(/^(\d+)\s*days?\s*/);
      let rest = value;
      if (dayMatch) {
        total += Number(dayMatch[1]) * 86_400_000;
        rest = value.slice(dayMatch[0].length);
      }
      const parts = rest.split(":").map(Number);
      if (parts.length === 3) {
        total += ((parts[0]! * 3600 + parts[1]! * 60 + parts[2]!) * 1000);
      } else if (parts.length === 2) {
        total += (parts[0]! * 3600 + parts[1]! * 60) * 1000;
      } else if (parts.length === 1 && !Number.isNaN(parts[0])) {
        total += parts[0]!;
      }
      this.ms = total;
    }
  }

  static fromDays(days: number): Timedelta {
    return new Timedelta(days * 86_400_000);
  }

  add(other: Timedelta | number): Timedelta {
    return new Timedelta(this.ms + (other instanceof Timedelta ? other.ms : other));
  }

  sub(other: Timedelta | number): Timedelta {
    return new Timedelta(this.ms - (other instanceof Timedelta ? other.ms : other));
  }

  toString(): string {
    return `Timedelta('${this.ms} ms')`;
  }
}

export class Period {
  constructor(
    readonly start: Date,
    /** Frequency in milliseconds; keeps Period honest without full freq algebra. */
    readonly freqMs: number = 86_400_000
  ) {}

  get end(): Date {
    return new Date(this.start.getTime() + this.freqMs);
  }

  contains(t: Date | Timestamp): boolean {
    const time = t instanceof Timestamp ? t.date.getTime() : t.getTime();
    return time >= this.start.getTime() && time < this.end.getTime();
  }

  toString(): string {
    return `[${this.start.toISOString()}, ${this.end.toISOString()})`;
  }
}

export class Index {
  constructor(readonly labels: IndexLabel[]) {}

  get length(): number {
    return this.labels.length;
  }

  [Symbol.iterator]() {
    return this.labels[Symbol.iterator]();
  }

  at(position: number): IndexLabel | undefined {
    return this.labels.at(position);
  }

  equals(other: Index): boolean {
    return (
      this.labels.length === other.labels.length &&
      this.labels.every((l, i) => String(l) === String(other.labels[i]))
    );
  }

  toString(): string {
    return `Index([${this.labels.map(String).join(", ")}])`;
  }
}

export class MultiIndex {
  constructor(readonly tuples: IndexLabel[][]) {}

  get length(): number {
    return this.tuples.length;
  }

  get_level_values(level: number): Index {
    return new Index(this.tuples.map((t) => t[level]!));
  }

  toString(): string {
    return `MultiIndex([${this.tuples.map((t) => `(${t.join(", ")})`).join(", ")}])`;
  }
}

export class DatetimeIndex extends Index {
  constructor(values: (Date | Timestamp | string | number)[]) {
    super(
      values.map((v) =>
        v instanceof Timestamp ? v.date : v instanceof Date ? v : new Date(v)
      ) as unknown as IndexLabel[]
    );
  }

  get as_i8(): number[] {
    return (this.labels as unknown as Date[]).map((d) => d.getTime());
  }
}

export class TimedeltaIndex extends Index {
  constructor(values: (Timedelta | number)[]) {
    super(values.map((v) => (v instanceof Timedelta ? v.ms : v)));
  }
}

export class PeriodIndex extends Index {
  constructor(readonly periods: Period[]) {
    super(periods.map((p) => p.start.getTime()));
  }
}

// ---------------------------------------------------------------------------
// Range builders
// ---------------------------------------------------------------------------

function parseFreqToMs(freq: string): number {
  const match = freq.match(/^(\d+)?\s*(ms|s|sec|min|h|hour|D|day|W)$/);
  if (!match) throw new Error(`Unsupported frequency '${freq}'.`);
  const n = match[1] ? Number(match[1]) : 1;
  const unit = match[2]!;
  switch (unit) {
    case "ms": return n;
    case "s":
    case "sec": return n * 1000;
    case "min": return n * 60_000;
    case "h":
    case "hour": return n * 3_600_000;
    case "D":
    case "day": return n * 86_400_000;
    case "W": return n * 7 * 86_400_000;
  }
  throw new Error(`Unsupported frequency '${freq}'.`);
}

export function date_range(
  start: Date | string | Timestamp,
  end: Date | string | Timestamp,
  periods?: number,
  freq = "D"
): DatetimeIndex {
  const step = parseFreqToMs(freq);
  const startMs = start instanceof Timestamp ? start.date.getTime() : new Date(start as Date).getTime();
  const endMs = end instanceof Timestamp ? end.date.getTime() : new Date(end as Date).getTime();
  const values: number[] = [];
  if (periods !== undefined) {
    for (let i = 0; i < periods; i += 1) values.push(startMs + i * step);
  } else {
    for (let t = startMs; t <= endMs; t += step) values.push(t);
  }
  return new DatetimeIndex(values.map((v) => new Date(v)));
}

export function timedelta_range(
  start: Timedelta | number,
  end: Timedelta | number,
  periods?: number,
  freq = "D"
): TimedeltaIndex {
  const step = parseFreqToMs(freq);
  const startMs = start instanceof Timedelta ? start.ms : start;
  const endMs = end instanceof Timedelta ? end.ms : end;
  const values: number[] = [];
  if (periods !== undefined) {
    for (let i = 0; i < periods; i += 1) values.push(startMs + i * step);
  } else {
    for (let t = startMs; t <= endMs; t += step) values.push(t);
  }
  return new TimedeltaIndex(values);
}

export function interval_range(
  start: number,
  end: number,
  periods: number
): { left: number; right: number }[] {
  if (periods < 2) throw new Error("interval_range needs at least 2 periods.");
  const step = (end - start) / (periods - 1);
  const out: { left: number; right: number }[] = [];
  for (let i = 0; i < periods - 1; i += 1) {
    out.push({ left: start + i * step, right: start + (i + 1) * step });
  }
  return out;
}

export function period_range(
  start: Date | string | Timestamp,
  periods: number,
  freq = "D"
): PeriodIndex {
  const step = parseFreqToMs(freq);
  const startMs = start instanceof Timestamp ? start.date.getTime() : new Date(start as Date).getTime();
  const out: Period[] = [];
  for (let i = 0; i < periods; i += 1) {
    out.push(new Period(new Date(startMs + i * step), step));
  }
  return new PeriodIndex(out);
}

// ---------------------------------------------------------------------------
// Reshape / merge wrappers
// ---------------------------------------------------------------------------

export function melt(
  frame: DataFrame,
  options: { id_vars?: string | string[]; value_vars?: string | string[] } = {}
): DataFrame {
  return frame.melt(options);
}

export function pivot(
  frame: DataFrame,
  options: { index: string; columns: string; values: string }
): DataFrame {
  return frame.pivot(options.index, options.columns, options.values);
}

/** Long reshape of two value-column sets (minimal pandas lreshape). */
export function lreshape(
  frame: DataFrame,
  groups: Record<string, string[]>
): DataFrame {
  const groupEntries = Object.entries(groups);
  const idColumns = frame.columns.filter(
    (c) => !groupEntries.some(([, cols]) => cols.includes(c))
  );
  const rows: Row[] = [];
  const sourceRows = frame.to_records();
  const maxLen = Math.max(
    ...groupEntries.map(([, cols]) =>
      Math.max(...sourceRows.map((r) => cols.filter((c) => r[c] != null).length))
    )
  );
  for (const row of sourceRows) {
    for (let i = 0; i < maxLen; i += 1) {
      const next: Row = {};
      for (const c of idColumns) next[c] = row[c] as CellValue;
      let keep = false;
      for (const [name, cols] of groupEntries) {
        const present = cols.map((c) => row[c]).filter((v) => v != null);
        const value = present[i];
        next[name] = (value ?? null) as CellValue;
        if (value != null) keep = true;
      }
      if (keep) rows.push(next);
    }
  }
  return DataFrame.createInternal(rows, [...new Set(rows.flatMap((r) => Object.keys(r)))], rows.map((_, i) => i));
}

/** Wide-to-long melt keyed by stubname (minimal pandas wide_to_long). */
export function wide_to_long(
  frame: DataFrame,
  stubnames: string[],
  i: string,
  j: string
): DataFrame {
  const sourceRows = frame.to_records();
  const rows: Row[] = [];
  const suffixes = new Set<string>();
  for (const row of sourceRows) {
    for (const stub of stubnames) {
      for (const key of Object.keys(row)) {
        if (key.startsWith(stub) && key.length > stub.length) {
          suffixes.add(key.slice(stub.length));
        }
      }
    }
  }
  const sortedSuffixes = [...suffixes].sort();
  for (const row of sourceRows) {
    for (const suffix of sortedSuffixes) {
      const next: Row = { [i]: row[i] as CellValue, [j]: suffix };
      let any = false;
      for (const stub of stubnames) {
        const col = `${stub}${suffix}`;
        next[stub] = (row[col] ?? null) as CellValue;
        if (row[col] != null) any = true;
      }
      if (any) rows.push(next);
    }
  }
  const columns = [i, j, ...stubnames];
  return DataFrame.createInternal(rows, columns, rows.map((_, idx) => idx));
}

/** As-of merge: last right row with key <= each left key. */
export function merge_asof(
  left: DataFrame,
  right: DataFrame,
  on: string
): DataFrame {
  const leftRows = left.to_records().map((r) => Number(r[on]));
  const rightRows = right.to_records();
  const rightKeys = rightRows.map((r) => Number(r[on]));
  const outRows: Row[] = [];
  for (let li = 0; li < leftRows.length; li += 1) {
    const key = leftRows[li]!;
    let best = -1;
    for (let ri = 0; ri < rightKeys.length; ri += 1) {
      if (rightKeys[ri]! <= key) best = ri;
      else break;
    }
    const merged: Row = { ...left.iloc(li) as unknown as Row };
    if (best >= 0) {
      const matched = rightRows[best]!;
      for (const [k, v] of Object.entries(matched)) {
        if (!(k in merged) || k === on) merged[k] = v as CellValue;
      }
    }
    outRows.push(merged);
  }
  const columns = [...new Set(outRows.flatMap((r) => Object.keys(r)))];
  return DataFrame.createInternal(outRows, columns, leftRows.map((_, i) => i));
}

/** Outer merge restricted to ordered keys with forward/backward filling. */
export function merge_ordered(
  left: DataFrame,
  right: DataFrame,
  on: string
): DataFrame {
  const unionKeys = [
    ...new Set([
      ...left.to_records().map((r) => r[on]),
      ...right.to_records().map((r) => r[on]),
    ]),
  ].sort(compareScalars);
  const leftByKey = new Map(left.to_records().map((r) => [String(r[on]), r]));
  const rightByKey = new Map(right.to_records().map((r) => [String(r[on]), r]));
  const outRows: Row[] = unionKeys.map((key) => {
    const l = leftByKey.get(String(key));
    const r = rightByKey.get(String(key));
    const merged: Row = { ...(l ?? {}), ...(r ?? {}) };
    merged[on] = key as CellValue;
    return merged;
  });
  const columns = [...new Set([...left.columns, ...right.columns])];
  return DataFrame.createInternal(outRows, columns, outRows.map((_, i) => i));
}

function compareScalars(a: unknown, b: unknown): number {
  if (typeof a === "number" && typeof b === "number") return a - b;
  return String(a).localeCompare(String(b));
}
