import type { CellValue } from "../../types";
import { dtypeZone, zoneOffsetMs, zoneOffsetMinutes } from "../shared/timezone";

export function stripBom(text: string): string {
  if (text.charCodeAt(0) === 0xfeff) {
    return text.slice(1);
  }
  return text;
}

export function coerceJsonCell(value: unknown): CellValue {
  if (
    value === null ||
    value === undefined ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  return JSON.stringify(value);
}

export function normalizeExternalCell(value: unknown): CellValue {
  if (
    value === null ||
    value === undefined ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    value instanceof Date
  ) {
    return value;
  }

  // bigint is a first-class CellValue (int64 storage); keep it exact.
  if (typeof value === "bigint") {
    return value;
  }

  return JSON.stringify(value);
}

// ---------------------------------------------------------------------------
// pandas-style scalar formatting for text exports (CSV).
//
// Datetime columns are formatted once per column at the finest resolution the
// data needs — exactly like pandas `to_csv`:
//   all values on a day boundary  -> "2020-01-01"
//   otherwise whole seconds       -> "2020-01-01 15:00:00"
//   otherwise                     -> "2020-01-01 15:00:00.123"
// Aware dtypes shift the stored UTC instant into the fixed zone offset and
// append "±HH:MM" — "2020-01-01 07:00:00-05:00" (pandas' rendering; the
// fixed-offset approximation in timezone.ts applies here too).
// ---------------------------------------------------------------------------

type CsvDatetimeResolution = "day" | "second" | "millis";

const DAY_MS = 86_400_000;

function pad(value: number, width: number): string {
  return String(Math.abs(value)).padStart(width, "0");
}

/** "+05:30" / "-08:00" / "+00:00" — pandas offset spelling. */
function csvUtcOffset(zone: string): string {
  const minutes = zoneOffsetMinutes(zone);
  const sign = minutes < 0 ? "-" : "+";
  const abs = Math.abs(minutes);
  return `${sign}${pad(Math.floor(abs / 60), 2)}:${pad(abs % 60, 2)}`;
}

function csvDatetimeText(instant: number, localOffset: number, resolution: CsvDatetimeResolution, zoneSuffix: string): string {
  const local = new Date(instant + localOffset);
  const date = `${pad(local.getUTCFullYear(), 4)}-${pad(local.getUTCMonth() + 1, 2)}-${pad(local.getUTCDate(), 2)}`;
  if (resolution === "day") {
    return date;
  }
  const time = `${pad(local.getUTCHours(), 2)}:${pad(local.getUTCMinutes(), 2)}:${pad(local.getUTCSeconds(), 2)}`;
  const fraction = resolution === "millis" ? `.${pad(local.getUTCMilliseconds(), 3)}` : "";
  return `${date} ${time}${fraction}${zoneSuffix}`;
}

/**
 * Builds a pandas-`to_csv` datetime formatter for one datetime-family column
 * (`date`, `datetime64[...]`, `datetime64[..., zone]`). The scan picks the
 * finest resolution any non-missing value needs, matching pandas'
 * per-column format choice. Returns undefined for cells that are not
 * finite Dates so callers can fall back to the generic cell escape.
 */
export function csvDatetimeFormatter(
  values: readonly CellValue[],
  dtype: string
): (value: CellValue) => string | undefined {
  const zone = dtypeZone(dtype);
  const offset = zone === null ? 0 : zoneOffsetMs(zone);
  const zoneSuffix = zone === null ? "" : csvUtcOffset(zone);

  let resolution: CsvDatetimeResolution = "day";
  for (const value of values) {
    if (!(value instanceof Date) || !Number.isFinite(value.getTime())) {
      continue;
    }
    const local = value.getTime() + offset;
    if (local % DAY_MS !== 0) {
      resolution = local % 1000 === 0 ? "second" : "millis";
      if (resolution === "millis") {
        break;
      }
    }
  }

  return (value) => {
    if (value instanceof Date && Number.isFinite(value.getTime())) {
      return csvDatetimeText(value.getTime(), offset, resolution, zoneSuffix);
    }
    return undefined;
  };
}
