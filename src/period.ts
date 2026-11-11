// pandas `Period` scalar and the `period[<freq>]` dtype family (Phase 1).
//
// A Period is a calendar interval addressed by a pandas-style ordinal: the
// count of freq units since the 1970-01-01 epoch, exactly as pandas computes
// it (`Period('2024-01', 'M').ordinal === 648`, `Period('1970-01-01','W')
// .ordinal === 1` for W-SUN weeks ending Sunday). Period start/end instants
// are epoch-millisecond boundaries in UTC, matching the library's naive
// datetime64 storage; nanosecond-era fields are out of scope for a JS Date.
//
// Supported freqs and their pandas spellings:
//   'Y' (aliases 'A', 'Y-DEC', 'A-DEC'), 'Q' ('Q-DEC'), 'M',
//   'W' ('W-SUN'; weeks ending Sunday), 'D', 'H' ('h').
// Finer freqs (min/s/ms/us/ns) and non-default anchors (Q-MAR, W-MON, …)
// raise ValueError.

import { ValueError } from "./errors";
import type { DType } from "./types";

export type PeriodFreq = "Y" | "Q" | "M" | "W" | "D" | "H";

const DAY_MS = 86_400_000;
const HOUR_MS = 3_600_000;

/** Coarseness rank (larger span sorts first in mixed-freq comparisons). */
const FREQ_GRAIN: Record<PeriodFreq, number> = { Y: 5, Q: 4, M: 3, W: 2, D: 1, H: 0 };

/** Accepted spellings → canonical code. */
const FREQ_ALIASES: Record<string, PeriodFreq> = {
  Y: "Y", A: "Y", "Y-DEC": "Y", "A-DEC": "Y",
  Q: "Q", "Q-DEC": "Q",
  M: "M",
  W: "W", "W-SUN": "W",
  D: "D",
  H: "H", h: "H",
};

/** pandas `freqstr` spelling per canonical freq. */
const PANDAS_FREQSTR: Record<PeriodFreq, string> = {
  Y: "Y-DEC", Q: "Q-DEC", M: "M", W: "W-SUN", D: "D", H: "h",
};

/** True when the token names a supported period freq ('M' but not 'min'). */
export function isPeriodFreq(freq: string): boolean {
  return FREQ_ALIASES[freq.trim()] !== undefined;
}

/** Canonical code for a freq spelling; ValueError on unsupported freqs. */
export function normalizePeriodFreq(freq: string): PeriodFreq {
  const trimmed = freq.trim();
  const canonical = FREQ_ALIASES[trimmed];
  if (canonical === undefined) {
    throw new ValueError(`Unsupported period freq '${freq}'; supported: Y/A, Q, M, W, D, H`);
  }
  return canonical;
}

const PERIOD_DTYPE_PATTERN = /^period\[([^\]]+)\]$/;

/** Freq carried by a `period[<freq>]` dtype spelling; null for other dtypes. */
export function periodFreqOf(dtype: string | undefined | null): PeriodFreq | null {
  const inner = dtype == null ? null : PERIOD_DTYPE_PATTERN.exec(dtype)?.[1];
  if (inner == null) return null;
  return isPeriodFreq(inner) ? normalizePeriodFreq(inner) : null;
}

/** True for `period[M]`/`period[Q-DEC]`/`period[h]`-style dtype spellings. */
export function isPeriodDType(dtype: string | undefined | null): boolean {
  return periodFreqOf(dtype) !== null;
}

/** Canonical `period[<code>]` dtype spelling for a freq. */
export function periodDTypeName(freq: PeriodFreq | string): DType {
  return `period[${normalizePeriodFreq(freq)}]` as DType;
}

// ---------------------------------------------------------------------------
// Ordinal ↔ instant math (UTC calendar fields, matching pandas ordinals)
// ---------------------------------------------------------------------------

/** Milliseconds at the inclusive start of the period (UTC midnight basis). */
export function periodStartMs(freq: PeriodFreq, ordinal: number): number {
  switch (freq) {
    case "Y": return Date.UTC(1970 + ordinal, 0, 1);
    case "Q": {
      const year = 1970 + Math.floor(ordinal / 4);
      const quarter = ordinal - 4 * Math.floor(ordinal / 4);
      return Date.UTC(year, quarter * 3, 1);
    }
    case "M": {
      const year = 1970 + Math.floor(ordinal / 12);
      const month = ordinal - 12 * Math.floor(ordinal / 12);
      return Date.UTC(year, month, 1);
    }
    // W-SUN periods end on Sunday: ordinal 1 covers 1969-12-29..1970-01-04.
    case "W": return (7 * (ordinal - 1) - 3) * DAY_MS;
    case "D": return ordinal * DAY_MS;
    case "H": return ordinal * HOUR_MS;
  }
}

/** Exclusive end boundary = start of the next ordinal. */
export function periodEndExclusiveMs(freq: PeriodFreq, ordinal: number): number {
  return periodStartMs(freq, ordinal + 1);
}

export interface PeriodFields {
  year: number;
  month: number;   // 1-12
  day: number;     // 1-31
  hour?: number;   // 0-23
  minute?: number; // 0-59
  second?: number; // 0-59
}

/** pandas ordinal for the period containing the given UTC calendar fields. */
export function periodOrdinalFromFields(freq: PeriodFreq, f: PeriodFields): number {
  switch (freq) {
    case "Y": return f.year - 1970;
    case "Q": return (f.year - 1970) * 4 + Math.floor((f.month - 1) / 3);
    case "M": return (f.year - 1970) * 12 + (f.month - 1);
    case "W": {
      const days = Math.floor(Date.UTC(f.year, f.month - 1, f.day) / DAY_MS);
      return Math.floor((days + 3) / 7) + 1;
    }
    case "D": return Math.floor(Date.UTC(f.year, f.month - 1, f.day) / DAY_MS);
    case "H":
      return Math.floor(
        Date.UTC(f.year, f.month - 1, f.day, f.hour ?? 0, f.minute ?? 0, f.second ?? 0) / HOUR_MS
      );
  }
}

/** Ordinal of the `freq` period containing the UTC instant `ms`. */
export function periodOrdinalAtMs(freq: PeriodFreq, ms: number): number {
  const d = new Date(ms);
  return periodOrdinalFromFields(freq, {
    year: d.getUTCFullYear(),
    month: d.getUTCMonth() + 1,
    day: d.getUTCDate(),
    hour: d.getUTCHours(),
    minute: d.getUTCMinutes(),
    second: d.getUTCSeconds(),
  });
}

// ---------------------------------------------------------------------------
// Value parsing
// ---------------------------------------------------------------------------

type Granularity = PeriodFreq | "MIN" | "S";

interface ParsedPeriod {
  fields: Required<PeriodFields>;
  /** Granularity expressed by the literal, used for freq inference. */
  granularity: Granularity;
}

const YEAR_RE = /^([+-]?\d{4,6})$/;
const YEAR_MONTH_RE = /^([+-]?\d{4,6})[-/](\d{1,2})$/;
const QUARTER_RE = /^([+-]?\d{4,6})[-\s]?[Qq]([1-4])$/;
const DATE_RE = /^([+-]?\d{4,6})[-/](\d{1,2})[-/](\d{1,2})$/;
const DATETIME_RE = /^([+-]?\d{4,6})[-/](\d{1,2})[-/](\d{1,2})(?:[T\s](\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?/;

function parsePeriodLiteral(text: string): ParsedPeriod {
  const trimmed = text.trim();
  let m = YEAR_RE.exec(trimmed);
  if (m) return { fields: fieldsOf(m[1]!), granularity: "Y" };
  m = QUARTER_RE.exec(trimmed);
  if (m) {
    const quarter = Number(m[2]);
    return { fields: fieldsOf(m[1]!, 3 * quarter - 2), granularity: "Q" };
  }
  m = YEAR_MONTH_RE.exec(trimmed);
  if (m) return { fields: fieldsOf(m[1]!, m[2]!), granularity: "M" };
  m = DATE_RE.exec(trimmed);
  if (m) return { fields: fieldsOf(m[1]!, m[2]!, m[3]!), granularity: "D" };
  m = DATETIME_RE.exec(trimmed);
  if (m) {
    const granularity: Granularity =
      m[6] !== undefined && Number(m[6]) !== 0 ? "S"
        : m[5] !== undefined && Number(m[5]) !== 0 ? "MIN"
          : m[4] !== undefined ? "H" : "D";
    return { fields: fieldsOf(m[1]!, m[2]!, m[3]!, m[4], m[5], m[6]), granularity };
  }
  throw new ValueError(`Cannot parse '${text}' as a period`);
}

function fieldsOf(
  year: string | number, month: string | number = "1", day: string | number = "1",
  hour?: string, minute?: string, second?: string
): Required<PeriodFields> {
  const fields = {
    year: Number(year), month: Number(month), day: Number(day),
    hour: hour === undefined ? 0 : Number(hour),
    minute: minute === undefined ? 0 : Number(minute),
    second: second === undefined ? 0 : Number(second),
  };
  if (fields.month < 1 || fields.month > 12 || fields.day < 1 || fields.day > 31 ||
      fields.hour > 23 || fields.minute > 59 || fields.second > 59) {
    throw new ValueError(`Period fields out of range: ${year}-${month}-${day} ${hour ?? 0}:${minute ?? 0}:${second ?? 0}`);
  }
  return fields;
}

function utcFieldsOf(ms: number): Required<PeriodFields> {
  const d = new Date(ms);
  return {
    year: d.getUTCFullYear(), month: d.getUTCMonth() + 1, day: d.getUTCDate(),
    hour: d.getUTCHours(), minute: d.getUTCMinutes(), second: d.getUTCSeconds(),
  };
}

/** Duck-typed Timestamp-like (has `.date`/`wallClock`); avoids a top-level import. */
function dateLikeOf(value: unknown): Date | null {
  if (value instanceof Date) return value;
  if (value !== null && typeof value === "object") {
    const inner = (value as { date?: unknown }).date;
    if (inner instanceof Date) {
      const wall = (value as { wallClock?: unknown }).wallClock;
      return typeof wall === "function" ? (wall as () => Date).call(value) : inner;
    }
  }
  return null;
}

/** Raw (ordinal, freq) resolution shared by the Period constructor and
 * `parsePeriodValue`; kept constructor-free so `new Period(ord, freq)` can
 * never recurse. */
function resolvePeriod(
  value: string | number | Date | Period | { date?: unknown; wallClock?: unknown },
  freq?: PeriodFreq | string
): { ordinal: number; freq: PeriodFreq } {
  const target = freq === undefined ? undefined : normalizePeriodFreq(freq);
  if (value instanceof Period) {
    if (target === undefined || target === value.freq) {
      return { ordinal: value.ordinal, freq: value.freq };
    }
    // pandas `Period(otherPeriod, freq)` raises IncompatibleFrequency;
    // explicit conversion goes through `asfreq`.
    throw new ValueError(`Input has different freq=${value.freqstr} from Period(freq=${PANDAS_FREQSTR[target]})`);
  }
  if (typeof value === "number") {
    if (!Number.isInteger(value)) throw new ValueError(`Period ordinal must be an integer, got ${value}`);
    if (target === undefined) throw new ValueError("Period: freq is required for ordinal input");
    return { ordinal: value, freq: target };
  }
  const date = dateLikeOf(value);
  if (date !== null) {
    if (Number.isNaN(date.getTime())) throw new ValueError(`Cannot parse '${String(value)}' as a period`);
    const resolved = target ?? "D";
    return { ordinal: periodOrdinalFromFields(resolved, utcFieldsOf(date.getTime())), freq: resolved };
  }
  if (typeof value === "string") {
    const parsed = parsePeriodLiteral(value);
    const resolved = target ?? parsed.granularity;
    if (resolved === "MIN" || resolved === "S") {
      throw new ValueError(`Unsupported period granularity '${resolved}' in '${value}'; finest supported freq is H`);
    }
    return { ordinal: periodOrdinalFromFields(resolved, parsed.fields), freq: resolved };
  }
  throw new ValueError(`Cannot construct a Period from ${String(value)}`);
}

/** Period wrapping the value, honoring a target freq (floors finer input). */
export function parsePeriodValue(
  value: string | number | Date | Period | { date?: unknown; wallClock?: unknown },
  freq?: PeriodFreq | string
): Period {
  const resolved = resolvePeriod(value, freq);
  return new Period(resolved.ordinal, resolved.freq);
}

// ---------------------------------------------------------------------------
// Period
// ---------------------------------------------------------------------------

function pad2(n: number): string {
  return n < 10 ? `0${n}` : String(n);
}

function isoDate(ms: number): string {
  const d = new Date(ms);
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

/** ISO-8601 week number (1-53) of the UTC instant. */
function isoWeekOfYear(d: Date): number {
  const target = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const dayNumber = (target.getUTCDay() + 6) % 7;
  target.setUTCDate(target.getUTCDate() - dayNumber + 3);
  const firstThursday = new Date(Date.UTC(target.getUTCFullYear(), 0, 4));
  const firstDayNumber = (firstThursday.getUTCDay() + 6) % 7;
  firstThursday.setUTCDate(firstThursday.getUTCDate() - firstDayNumber + 3);
  return 1 + Math.round((target.getTime() - firstThursday.getTime()) / (7 * DAY_MS));
}

function integerOperand(value: number | bigint, op: string): number {
  const n = typeof value === "bigint" ? Number(value) : value;
  if (!Number.isInteger(n)) {
    throw new TypeError(`unsupported operand type(s) for ${op}: 'Period' and '${typeof value === "bigint" ? "int" : "float"}'`);
  }
  return n;
}

/**
 * pandas `Period`: a single calendar span (e.g. the month of January 2024).
 * Identity is `(ordinal, freq)`; `str(p)` follows pandas ('2024-01',
 * '2024Q1', '2024-01-15/2024-01-21' for weeks, '2024-01-15 05:00' for hours).
 *
 * Constructor shapes:
 *   new Period("2024-01", "M")   — parse at a freq (finer input floors)
 *   new Period("2024-01")        — freq inferred from the literal
 *   new Period(648, "M")         — raw pandas ordinal
 *   new Period(dateOrTimestamp, "D") — period containing the instant (UTC fields)
 *   new Period(otherPeriod)      — copy (mismatched freq raises, like pandas)
 */
export class Period {
  readonly ordinal: number;
  readonly freq: PeriodFreq;

  constructor(
    value: string | number | Date | Period | { date?: unknown; wallClock?: unknown },
    freq?: PeriodFreq | string
  ) {
    const resolved = resolvePeriod(value, freq);
    this.ordinal = resolved.ordinal;
    this.freq = resolved.freq;
  }

  static fromOrdinal(ordinal: number, freq: PeriodFreq | string): Period {
    return new Period(ordinal, freq);
  }

  /** pandas `freqstr` spelling ('M', 'Q-DEC', 'Y-DEC', 'W-SUN', 'D', 'h'). */
  get freqstr(): string {
    return PANDAS_FREQSTR[this.freq];
  }

  private get startMs(): number {
    return periodStartMs(this.freq, this.ordinal);
  }

  private get endExclusiveMs(): number {
    return periodEndExclusiveMs(this.freq, this.ordinal);
  }

  /** Inclusive start instant (pandas `Period.start_time`). */
  get start_time(): Date {
    return new Date(this.startMs);
  }

  /**
   * pandas `Period.end_time`: the last representable instant. At ms
   * resolution this is `endExclusive - 1ms` (pandas reports 23:59:59.999999
   * at microsecond storage).
   */
  get end_time(): Date {
    return new Date(this.endExclusiveMs - 1);
  }

  /** Compat aliases for the pre-ordinal Period surface. */
  get start(): Date {
    return this.start_time;
  }

  /** Exclusive end boundary (the next period's start). */
  get end(): Date {
    return new Date(this.endExclusiveMs);
  }

  /** Millisecond span of this concrete period (calendar-exact for M/Q/Y). */
  get freqMs(): number {
    return this.endExclusiveMs - this.startMs;
  }

  private get endDate(): Date {
    return this.end_time;
  }

  // pandas derives Period calendar fields from the END of the span
  // (Period('2024-01','M').day === 31, Period('2024Q1','Q').month === 3).
  get year(): number {
    return this.endDate.getUTCFullYear();
  }
  get month(): number {
    return this.endDate.getUTCMonth() + 1;
  }
  get day(): number {
    return this.endDate.getUTCDate();
  }
  get quarter(): number {
    return Math.floor(this.endDate.getUTCMonth() / 3) + 1;
  }
  get weekofyear(): number {
    return isoWeekOfYear(this.endDate);
  }
  get hour(): number {
    return this.endDate.getUTCHours();
  }
  get minute(): number {
    return this.endDate.getUTCMinutes();
  }
  get second(): number {
    return this.endDate.getUTCSeconds();
  }
  /** 0 = Monday ... 6 = Sunday (pandas `dayofweek`). */
  get dayofweek(): number {
    return (this.endDate.getUTCDay() + 6) % 7;
  }
  /** 1-based day of year of the period's end date. */
  get dayofyear(): number {
    const start = Date.UTC(this.endDate.getUTCFullYear(), 0, 1);
    return Math.floor((this.endDate.getTime() - start) / DAY_MS) + 1;
  }

  /** True when the instant (Date or Timestamp-like `.date`) lies in this period. */
  contains(t: Date | { date: Date }): boolean {
    const ms = t instanceof Date ? t.getTime() : t.date.getTime();
    return ms >= this.startMs && ms < this.endExclusiveMs;
  }

  /** pandas `Period.to_timestamp`: boundary instant ('S' start, 'E' end). */
  to_timestamp(how: "S" | "E" | "start" | "end" = "S"): Date {
    const end = how === "E" || how === "end";
    return end ? this.end_time : this.start_time;
  }

  /**
   * pandas `Period.asfreq`: re-base to another freq. `how='S'` anchors on the
   * period containing the start instant, `how='E'` (default) on the period
   * containing the end instant.
   */
  asfreq(target: PeriodFreq | string, how: "S" | "E" | "start" | "end" = "E"): Period {
    const freq = normalizePeriodFreq(target);
    if (freq === this.freq) return new Period(this.ordinal, this.freq);
    const anchor = how === "S" || how === "start" ? this.startMs : this.endExclusiveMs - 1;
    return new Period(periodOrdinalAtMs(freq, anchor), freq);
  }

  /** pandas `Period + n`: shift by whole freq units. */
  add(n: number | bigint): Period {
    return new Period(this.ordinal + integerOperand(n, "+"), this.freq);
  }

  /**
   * `Period - int` shifts back `n` units. `Period - Period` returns the
   * ordinal difference in freq steps (pandas returns an offset object such
   * as `2 * MonthEnd`; the integer count is the honest scalar answer here).
   */
  sub(other: number | bigint | Period): Period | number {
    if (other instanceof Period) {
      if (other.freq !== this.freq) {
        throw new ValueError(`Input has different freq=${other.freqstr} from Period(freq=${this.freqstr})`);
      }
      return this.ordinal - other.ordinal;
    }
    return new Period(this.ordinal - integerOperand(other, "-"), this.freq);
  }

  /** Same `(freq, ordinal)` identity (pandas `==`; cross-freq is False). */
  equals(other: unknown): boolean {
    return other instanceof Period && other.freq === this.freq && other.ordinal === this.ordinal;
  }

  /**
   * Ordinal comparison; cross-freq ordering raises the pandas
   * `IncompatibleFrequency` analogue (a ValueError here).
   */
  compareTo(other: Period): number {
    if (other.freq !== this.freq) {
      throw new ValueError(`Input has different freq=${other.freqstr} from Period(freq=${this.freqstr})`);
    }
    return this.ordinal - other.ordinal;
  }

  lt(other: Period): boolean { return this.compareTo(other) < 0; }
  le(other: Period): boolean { return this.compareTo(other) <= 0; }
  gt(other: Period): boolean { return this.compareTo(other) > 0; }
  ge(other: Period): boolean { return this.compareTo(other) >= 0; }

  toString(): string {
    switch (this.freq) {
      case "Y": return `${this.year}`;
      case "Q": return `${this.year}Q${this.quarter}`;
      case "M": return `${this.year}-${pad2(this.month)}`;
      case "W": return `${isoDate(this.startMs)}/${isoDate(this.end_time.getTime())}`;
      case "D": return isoDate(this.startMs);
      case "H": {
        const start = new Date(this.startMs);
        return `${isoDate(this.startMs)} ${pad2(start.getUTCHours())}:00`;
      }
    }
  }

  /** pandas `repr(Period)`: Period('2024-01', 'M'). */
  repr(): string {
    return `Period('${this.toString()}', '${this.freqstr}')`;
  }

  /** JSON.stringify emits the pandas str form ("2024-01"). */
  toJSON(): string {
    return this.toString();
  }
}

/**
 * Deterministic sort key for Periods: `(freq grain, ordinal)` — coarser
 * freqs first, then pandas ordinal order. Same-freq pairs order by ordinal,
 * matching pandas; pandas raises on cross-freq ORDER comparisons, while
 * this key keeps sorts/merges total over mixed-freq object columns.
 */
export function comparePeriods(left: Period, right: Period): number {
  if (left.freq === right.freq) return left.ordinal - right.ordinal;
  const grain = FREQ_GRAIN[right.freq] - FREQ_GRAIN[left.freq];
  return grain !== 0 ? grain : left.ordinal - right.ordinal;
}
