import type { CellValue } from "../../types";
import { isMissing } from "../../utils";

function padBoth(s: string, width: number, fillChar: string): string {
  const total = width - s.length;
  if (total <= 0) {
    return s;
  }
  const left = Math.floor(total / 2);
  const right = total - left;
  return fillChar.repeat(left) + s + fillChar.repeat(right);
}

/**
 * pandas-style `.str` accessor for Series: applies string methods
 * element-wise. Missing values propagate as null; non-string values
 * are converted with String() first, matching pandas' loose behavior
 * for object columns.
 */
export class StringMethods {
  private readonly values: CellValue[];

  constructor(values: CellValue[]) {
    this.values = values;
  }

  private mapStrings(fn: (s: string) => string): CellValue[] {
    return this.values.map((value) => {
      if (isMissing(value)) {
        return null;
      }
      return fn(String(value));
    });
  }

  private mapBooleans(fn: (s: string) => boolean): CellValue[] {
    return this.values.map((value) => {
      if (isMissing(value)) {
        return null;
      }
      return fn(String(value));
    });
  }

  upper(): CellValue[] {
    return this.mapStrings((s) => s.toUpperCase());
  }

  lower(): CellValue[] {
    return this.mapStrings((s) => s.toLowerCase());
  }

  capitalize(): CellValue[] {
    return this.mapStrings((s) => s.charAt(0).toUpperCase() + s.slice(1));
  }

  title(): CellValue[] {
    return this.mapStrings((s) =>
      s.replace(/\w\S*/g, (word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    );
  }

  strip(): CellValue[] {
    return this.mapStrings((s) => s.trim());
  }

  lstrip(): CellValue[] {
    return this.mapStrings((s) => s.replace(/^\s+/, ""));
  }

  rstrip(): CellValue[] {
    return this.mapStrings((s) => s.replace(/\s+$/, ""));
  }

  zfill(width: number): CellValue[] {
    return this.mapStrings((s) => s.padStart(width, "0"));
  }

  pad(width: number, side: "left" | "right" | "both" = "left", fillChar = " "): CellValue[] {
    return this.mapStrings((s) => {
      if (side === "left") {
        return s.padStart(width, fillChar);
      }
      if (side === "right") {
        return s.padEnd(width, fillChar);
      }
      return padBoth(s, width, fillChar);
    });
  }

  slice(start?: number, stop?: number): CellValue[] {
    return this.mapStrings((s) => {
      const from = start ?? 0;
      if (stop === undefined) {
        return s.slice(from);
      }
      const to = stop < 0 ? s.length + stop : stop;
      return s.slice(from, to);
    });
  }

  replace(pat: string | RegExp, repl: string, regex = false): CellValue[] {
    return this.mapStrings((s) => {
      if (typeof pat === "string" && !regex) {
        return s.split(pat).join(repl);
      }
      return s.replace(pat instanceof RegExp ? pat : new RegExp(pat, "g"), repl);
    });
  }

  contains(pat: string | RegExp, regex = false): CellValue[] {
    return this.mapBooleans((s) => {
      if (regex || pat instanceof RegExp) {
        return new RegExp(pat as string | RegExp).test(s);
      }
      return s.includes(pat as string);
    });
  }

  startswith(pat: string): CellValue[] {
    return this.mapBooleans((s) => s.startsWith(pat));
  }

  endswith(pat: string): CellValue[] {
    return this.mapBooleans((s) => s.endsWith(pat));
  }

  match(pat: string | RegExp): CellValue[] {
    const re = pat instanceof RegExp ? pat : new RegExp(pat);
    return this.mapBooleans((s) => re.test(s));
  }

  find(pat: string): CellValue[] {
    return this.values.map((value) => {
      if (isMissing(value)) {
        return null;
      }
      return String(value).indexOf(pat);
    });
  }

  len(): CellValue[] {
    return this.values.map((value) => {
      if (isMissing(value)) {
        return null;
      }
      return String(value).length;
    });
  }

  get(position: number): CellValue[] {
    return this.values.map((value) => {
      if (isMissing(value)) {
        return null;
      }
      const s = String(value);
      const index = position < 0 ? s.length + position : position;
      return index >= 0 && index < s.length ? s[index]! : "";
    });
  }

  count(pat: string): CellValue[] {
    return this.values.map((value) => {
      if (isMissing(value)) {
        return null;
      }
      return String(value).split(pat).length - 1;
    });
  }

  /** Returns arrays of components; with expand=false returns them per cell. */
  split(sep?: string): CellValue[][] {
    return this.values.map((value) => {
      if (isMissing(value)) {
        return [];
      }
      const s = String(value);
      return sep === undefined ? s.split(/\s+/).filter(Boolean) : s.split(sep);
    }) as CellValue[][];
  }

  cat(other?: string | CellValue[], sep = "", naRep?: string): CellValue[] {
    return this.values.map((value, i) => {
      const left = isMissing(value) ? (naRep !== undefined ? naRep : null) : String(value);
      if (left === null) {
        return null;
      }
      if (other === undefined) {
        return left;
      }
      let right: string | null;
      if (typeof other === "string") {
        right = other;
      } else {
        right = isMissing(other[i]) ? (naRep !== undefined ? naRep : null) : String(other[i]);
      }
      if (right === null) {
        return null;
      }
      return `${left}${sep}${right}`;
    });
  }

  // ---- character-class predicates ----

  private mapCharClass(fn: (s: string) => boolean): CellValue[] {
    return this.mapBooleans(fn);
  }

  isalpha(): CellValue[] {
    return this.mapCharClass((s) => s.length > 0 && /^[a-zA-Z]+$/.test(s));
  }

  isdigit(): CellValue[] {
    return this.mapCharClass((s) => s.length > 0 && /^[0-9]+$/.test(s));
  }

  isnumeric(): CellValue[] {
    return this.mapCharClass((s) => s.length > 0 && /^[0-9+\-.,\s]+$/.test(s));
  }

  isspace(): CellValue[] {
    return this.mapCharClass((s) => s.length > 0 && /^\s+$/.test(s));
  }

  isalnum(): CellValue[] {
    return this.mapCharClass((s) => s.length > 0 && /^[a-zA-Z0-9]+$/.test(s));
  }

  islower(): CellValue[] {
    return this.mapCharClass(
      (s) => s === s.toLowerCase() && s !== s.toUpperCase()
    );
  }

  isupper(): CellValue[] {
    return this.mapCharClass(
      (s) => s === s.toUpperCase() && s !== s.toLowerCase()
    );
  }

  /** Casefold (more aggressive than lower for some locales). */
  casefold(): CellValue[] {
    return this.mapStrings((s) => s.toLowerCase());
  }

  swapcase(): CellValue[] {
    return this.mapStrings((s) =>
      [...s]
        .map((ch) => (ch === ch.toUpperCase() ? ch.toLowerCase() : ch.toUpperCase()))
        .join("")
    );
  }

  center(width: number, fillChar = " "): CellValue[] {
    return this.mapStrings((s) => {
      const total = width - s.length;
      if (total <= 0) return s;
      const left = Math.floor(total / 2);
      return fillChar.repeat(left) + s + fillChar.repeat(total - left);
    });
  }

  ljust(width: number, fillChar = " "): CellValue[] {
    return this.mapStrings((s) => s.padEnd(width, fillChar));
  }

  rjust(width: number, fillChar = " "): CellValue[] {
    return this.mapStrings((s) => s.padStart(width, fillChar));
  }

  /** Removes a prefix from each string when present. */
  removeprefix(prefix: string): CellValue[] {
    return this.mapStrings((s) => (s.startsWith(prefix) ? s.slice(prefix.length) : s));
  }

  /** Removes a suffix from each string when present. */
  removesuffix(suffix: string): CellValue[] {
    return this.mapStrings((s) => (s.endsWith(suffix) ? s.slice(0, -suffix.length) : s));
  }

  /** Repeats each string n times. */
  repeat(n: number): CellValue[] {
    return this.mapStrings((s) => s.repeat(Math.max(0, n)));
  }

  /** All non-overlapping matches of `pat` per cell; empty array when no match. */
  findall(pat: string | RegExp): CellValue[][] {
    return this.values.map((value) => {
      if (isMissing(value)) {
        return [];
      }
      const source = String(value);
      const re = pat instanceof RegExp ? new RegExp(pat.source, pat.flags.includes("g") ? pat.flags : pat.flags + "g") : new RegExp(pat, "g");
      return [...source.matchAll(re)].map((m) => m[0]);
    });
  }

  /**
   * Extracts the first capture group match per cell; null when no
   * match. With multiple groups, returns the group arrays.
   */
  extract(pat: string | RegExp): CellValue[] | CellValue[][] {
    const re = pat instanceof RegExp ? new RegExp(pat.source, pat.flags.replace("g", "")) : new RegExp(pat);
    const groupCount = new RegExp(re.source + "|").exec("")!.length - 1;
    if (groupCount > 1) {
      return this.values.map((value) => {
        if (isMissing(value)) {
          return [];
        }
        const match = String(value).match(re);
        return match ? match.slice(1) : [];
      }) as CellValue[][];
    }
    return this.values.map((value) => {
      if (isMissing(value)) {
        return null;
      }
      const match = String(value).match(re);
      return match ? (match[1] ?? match[0]) : null;
    });
  }

  /** Splits on the first occurrence; returns [before, sep, after]. */
  partition(sep: string): CellValue[][] {
    return this.values.map((value) => {
      if (isMissing(value)) {
        return ["", "", ""];
      }
      const s = String(value);
      const index = s.indexOf(sep);
      if (index < 0) {
        return [s, "", ""];
      }
      return [s.slice(0, index), sep, s.slice(index + sep.length)];
    });
  }

  /** Splits on the last occurrence; returns [before, sep, after]. */
  rpartition(sep: string): CellValue[][] {
    return this.values.map((value) => {
      if (isMissing(value)) {
        return ["", "", ""];
      }
      const s = String(value);
      const index = s.lastIndexOf(sep);
      if (index < 0) {
        return ["", "", s];
      }
      return [s.slice(0, index), sep, s.slice(index + sep.length)];
    });
  }

  /** Splits from the right, at most `n` times. */
  rsplit(sep?: string, n?: number): CellValue[][] {
    return this.values.map((value) => {
      if (isMissing(value)) {
        return [];
      }
      const s = String(value);
      if (n === undefined) {
        return sep === undefined ? s.split(/\s+/).filter(Boolean) : s.split(sep);
      }
      if (sep === undefined) {
        return s.split(/\s+/).filter(Boolean).reduceRight<string[][]>(
          (acc, part, i, arr) => (i >= arr.length - n ? [[part], ...acc] as string[][] : acc.length ? [[arr.slice(0, arr.length - n).join(" ")], ...acc] : [[part], ...acc]),
          [] as string[][]
        ).flat() as unknown as string[];
      }
      const parts = s.split(sep);
      const head = parts.slice(0, parts.length - n);
      const tail = parts.slice(parts.length - n);
      return n > 0 && parts.length > n ? [head.join(sep), ...tail] : parts;
    });
  }
}
