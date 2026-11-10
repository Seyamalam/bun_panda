// Timezone metadata for pandas-style datetime64 storage.
//
// Representation: datetime cells are JS `Date` values holding the UTC
// instant (epoch milliseconds; the `[ns]` unit is declared in the dtype
// name only — JavaScript Date cannot retain nanosecond precision). The
// timezone itself lives in the dtype string, e.g. `datetime64[ns,
// US/Eastern]`, mirroring how pandas attaches tzinfo to the dtype rather
// than to each stored value. `NaT` is the missing marker for these
// columns, kept distinct from null/NaN/NA.
//
// Zone resolution is a FIXED-OFFSET approximation: named zones resolve to
// their standard-time offset and do not model DST transitions (US/Eastern
// is always UTC-05:00, never EDT). Asia/Tokyo and Asia/Kolkata observe no
// DST, so their offsets are exact. Explicit offset spellings such as
// "UTC+05:30" or "-08:00" are always exact.

import { ValueError } from "../../errors";
import type { DType } from "../../types";

const MINUTE_MS = 60_000;

/**
 * Fixed standard-time offsets (minutes east of UTC) for supported named
 * zones. US/Europe entries pin the winter offset; Asia entries are exact.
 */
const NAMED_ZONE_OFFSET_MINUTES: Record<string, number> = {
  UTC: 0,
  GMT: 0,
  Z: 0,
  Zulu: 0,
  Universal: 0,
  "Etc/UTC": 0,
  "Etc/GMT": 0,
  "Etc/Universal": 0,
  "Etc/Zulu": 0,
  "US/Eastern": -300,
  "US/Central": -360,
  "US/Mountain": -420,
  "US/Pacific": -480,
  "America/New_York": -300,
  "America/Chicago": -360,
  "America/Denver": -420,
  "America/Los_Angeles": -480,
  "Europe/London": 0,
  "Europe/Berlin": 60,
  "Europe/Paris": 60,
  "Europe/Madrid": 60,
  "Asia/Tokyo": 540,
  "Asia/Kolkata": 330,
  "Asia/Calcutta": 330,
};

// "UTC+05:30", "GMT-8", "+05:30", "-0800".
const OFFSET_PATTERN = /^(?:UTC|GMT)?([+-])(\d{1,2})(?::?(\d{2}))?$/i;

/**
 * Fixed offset in minutes east of UTC for a zone label. Accepts "UTC",
 * "GMT", "Z", "UTC±HH[:MM]", "GMT±HH[:MM]", bare "±HH[:MM]", and the
 * named zones in NAMED_ZONE_OFFSET_MINUTES. Throws ValueError for unknown
 * zones (the pandas `UnknownTimeZoneError` analogue).
 */
export function zoneOffsetMinutes(tz: string): number {
  const trimmed = tz.trim();
  const named = NAMED_ZONE_OFFSET_MINUTES[trimmed];
  if (named !== undefined) {
    return named;
  }
  const match = OFFSET_PATTERN.exec(trimmed);
  if (match) {
    const sign = match[1] === "-" ? -1 : 1;
    const hours = Number(match[2]);
    const minutes = match[3] === undefined ? 0 : Number(match[3]);
    if (hours <= 23 && minutes <= 59) {
      return sign * (hours * 60 + minutes);
    }
  }
  throw new ValueError(`Unknown time zone '${tz}'`);
}

/** Fixed offset in milliseconds east of UTC for a zone label. */
export function zoneOffsetMs(tz: string): number {
  return zoneOffsetMinutes(tz) * MINUTE_MS;
}

// `datetime64[ns]`, `datetime64[us, US/Eastern]`, etc. Units match pandas:
// s, ms, us, ns (all stored as epoch ms; the unit is metadata).
const DATETIME64_RESOLVED = /^datetime64\[(ns|us|ms|s)(?:\s*,\s*([^\]]+))?\]$/i;
// `datetime64` / `Datetime64` (bun_panda compat spellings for naive).
const DATETIME64_BARE = /^datetime64$/i;

/** True for `datetime64`, `Datetime64`, `datetime64[<unit>]`, and `datetime64[<unit>, <zone>]`. */
export function isDatetime64DType(dtype: string): boolean {
  return DATETIME64_BARE.test(dtype) || DATETIME64_RESOLVED.test(dtype);
}

/** True for the whole datetime family: pandas datetime64 spellings plus the legacy `date` alias. */
export function isDatetimeDType(dtype: string | undefined): boolean {
  return dtype === "date" || (dtype !== undefined && isDatetime64DType(dtype));
}

/** Zone label embedded in an aware dtype (`datetime64[ns, X]`); null for naive and non-datetime dtypes. */
export function dtypeZone(dtype: string | undefined): string | null {
  if (dtype === undefined) {
    return null;
  }
  const zone = DATETIME64_RESOLVED.exec(dtype)?.[2]?.trim();
  return zone === undefined || zone === "" ? null : zone;
}

/** True when the dtype carries a zone label (`datetime64[ns, X]`). */
export function isDatetimeAwareDType(dtype: string | undefined): boolean {
  return isDatetime64DType(dtype ?? "") && dtypeZone(dtype) !== null;
}

/** Storage unit declared by a datetime dtype; "ns" for the `date` alias and bare spellings. */
export function dtypeUnit(dtype: string | undefined): string {
  if (dtype === undefined) {
    return "ns";
  }
  return DATETIME64_RESOLVED.exec(dtype)?.[1]?.toLowerCase() ?? "ns";
}

/** Canonical pandas dtype spelling for a datetime column with an optional zone. */
export function datetime64Name(unit: string, zone: string | null): DType {
  return (zone === null ? `datetime64[${unit}]` : `datetime64[${unit}, ${zone}]`) as DType;
}
