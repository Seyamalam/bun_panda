// Top-level pandas-style IO surface beyond CSV/JSON/Excel/Parquet.
// Everything here is dependency-free: JSON-encoded buffers are decoded when
// they look like our own writer output, everything genuinely binary throws a
// descriptive NotSupportedError instead of failing mysteriously.
import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { DataFrame } from "./dataframe";
import { NotSupportedError, BunPandaValidationError } from "./errors";
import type { CellValue, Row } from "./types";

// ---------------------------------------------------------------------------
// Shared plumbing
// ---------------------------------------------------------------------------

function assertNonEmpty(value: unknown, what: string): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new BunPandaValidationError(`Expected a non-empty ${what}.`);
  }
}

/** Treat a short token without newlines as a file path, otherwise as raw text. */
function resolveTextOrPath(
  input: string,
  readOptions: { encoding?: BufferEncoding }
): string {
  if (!input.includes("\n") && !input.startsWith("{") && !input.startsWith("[")) {
    const encoding = readOptions.encoding ?? "utf8";
    return readFileSync(input, encoding);
  }
  return input;
}

function decodeBuffer(payload: Buffer | Uint8Array): string {
  return Buffer.from(payload.buffer, payload.byteOffset, payload.byteLength).toString("utf8");
}

/**
 * Best-effort decode of a JSON-encoded tabular payload (the shape our own
 * to_* writers emit). Returns null when the bytes are not decodable JSON.
 */
function tryParseJsonRecords(text: string): { rows: Row[]; columns?: string[] } | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  if (Array.isArray(parsed)) {
    const rows = parsed.filter(
      (entry): entry is Row => typeof entry === "object" && entry !== null && !Array.isArray(entry)
    );
    return { rows };
  }
  if (typeof parsed === "object" && parsed !== null) {
    const obj = parsed as Record<string, unknown>;
    // Column-oriented shape: { columns: [...], data: [[...], ...] }
    if (Array.isArray(obj.columns) && Array.isArray(obj.data)) {
      const columns = obj.columns.map(String);
      const rows = (obj.data as CellValue[][]).map((values) => {
        const row: Row = {};
        columns.forEach((column, i) => {
          row[column] = (values[i] ?? null) as CellValue;
        });
        return row;
      });
      return { rows, columns };
    }
    // Single-column-oriented plain object: { a: [1,2], b: [3,4] }
    const entries = Object.entries(obj).filter(([, v]) => Array.isArray(v));
    if (entries.length > 0 && entries.every(([, v]) => Array.isArray(v))) {
      const length = Math.max(...entries.map(([, v]) => (v as CellValue[]).length));
      const rows = Array.from({ length }, (_, i) => {
        const row: Row = {};
        for (const [key, value] of entries) {
          row[key] = ((value as CellValue[])[i] ?? null) as CellValue;
        }
        return row;
      });
      return { rows };
    }
  }
  return null;
}

function frameFromJsonPayload(
  payload: Buffer | Uint8Array,
  formatName: string,
  options: { names?: string[]; index_col?: string | number } = {}
): DataFrame {
  const parsed = tryParseJsonRecords(decodeBuffer(payload));
  if (parsed === null || parsed.rows.length === 0) {
    throw new NotSupportedError(
      `${formatName} support requires a JSON-encoded tabular payload (as written by our to_* writers); ` +
        `binary ${formatName} decoding is not implemented. Encode the data as JSON records ` +
        `or use a dedicated ${formatName} parser.`
    );
  }
  const columns =
    parsed.columns ??
    (options.names ??
      [...new Set(parsed.rows.flatMap((row) => Object.keys(row)))]);
  const frame = new DataFrame(parsed.rows, { columns });
  if (options.index_col === undefined) return frame;
  const name =
    typeof options.index_col === "number"
      ? columns[options.index_col]
      : options.index_col;
  if (name === undefined || !columns.includes(name)) {
    throw new BunPandaValidationError(`index_col ${String(options.index_col)} not found in columns.`);
  }
  return frame.set_index(name);
}

// ---------------------------------------------------------------------------
// JSON Lines
// ---------------------------------------------------------------------------

export function read_json_lines(input: string): DataFrame {
  assertNonEmpty(input, "JSON-lines document or path");
  const text = resolveTextOrPath(input, {});
  const rows: Row[] = [];
  for (const line of text.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (trimmed.length === 0) continue;
    rows.push(JSON.parse(trimmed) as Row);
  }
  const columns = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  return new DataFrame(rows, { columns });
}

// ---------------------------------------------------------------------------
// Fixed-width files
// ---------------------------------------------------------------------------

export interface ReadFwfOptions {
  /** Inclusive/exclusive [start, end) character spans, pandas-style. */
  colspecs?: [number, number][];
  /** Uniform column widths (alternative to colspecs). */
  widths?: number[];
  names?: string[];
  /** Treat the first line as a header row supplying column names. */
  header?: boolean;
}

export function read_fwf(input: string, options: ReadFwfOptions = {}): DataFrame {
  assertNonEmpty(input, "fixed-width document or path");
  const text = resolveTextOrPath(input, {});
  const lines = text.split(/\r?\n/).filter((line) => line.trim().length > 0);
  if (lines.length === 0) return new DataFrame([]);

  let colspecs = options.colspecs;
  if (colspecs === undefined && options.widths !== undefined) {
    let offset = 0;
    colspecs = options.widths.map((width) => {
      const span: [number, number] = [offset, offset + width];
      offset += width;
      return span;
    });
  }

  let names = options.names;
  if (names !== undefined && options.header) {
    throw new BunPandaValidationError("Pass either `names` or `header: true`, not both.");
  }
  let bodyLines = lines;
  if (names === undefined && options.header) {
    const headerLine = bodyLines[0]!;
    bodyLines = bodyLines.slice(1);
    names = splitFixedWidth(headerLine, colspecs).map((cell) => cell.trim());
  }
  if (names === undefined && colspecs === undefined) {
    // No explicit layout at all: fall back to whitespace-run splitting so the
    // call still succeeds on well-aligned simple tables.
    names = splitOnWhitespaceRuns(bodyLines[0]!).map((_, i) => `col_${i}`);
  }

  const rows: Row[] = [];
  for (const line of bodyLines) {
    const cells = splitFixedWidth(line, colspecs);
    rows.push(
      Object.fromEntries(
        cells.map((cell, i) => [names?.[i] ?? `col_${i}`, coerceScalar(cell.trim())])
      )
    );
  }
  const columns = rows.length > 0 ? Object.keys(rows[0]!) : (names ?? []);
  return new DataFrame(rows, { columns });
}

function splitFixedWidth(line: string, colspecs?: [number, number][]): string[] {
  if (colspecs !== undefined) {
    return colspecs.map(([start, end]) => line.slice(start, end));
  }
  return splitOnWhitespaceRuns(line);
}

function splitOnWhitespaceRuns(line: string): string[] {
  return line.split(/\s{2,}|\t+/).filter((cell) => cell.length > 0);
}

function coerceScalar(cell: string): CellValue {
  if (cell === "" || cell.toLowerCase() === "null" || cell.toLowerCase() === "nan") return null;
  if (cell === "true") return true;
  if (cell === "false") return false;
  const num = Number(cell);
  if (cell !== "" && !Number.isNaN(num)) return num;
  return cell;
}

// ---------------------------------------------------------------------------
// HTML tables
// ---------------------------------------------------------------------------

export interface ReadHtmlOptions {
  /** Only consider tables whose text contains this substring (pandas `match`). */
  match?: string;
  /** Treat the first <tr> as a header row (default true when it uses <th>). */
  header?: boolean;
}

const NAMED_ENTITIES: Record<string, string> = {
  amp: "&",
  lt: "<",
  gt: ">",
  quot: '"',
  apos: "'",
  nbsp: " ",
};

function decodeEntities(text: string): string {
  return text.replace(/&(#x?[0-9a-fA-F]+|[a-zA-Z]+);/g, (entity, body: string) => {
    if (body.startsWith("#x") || body.startsWith("#X")) {
      return String.fromCodePoint(parseInt(body.slice(2), 16));
    }
    if (body.startsWith("#")) {
      return String.fromCodePoint(parseInt(body.slice(1), 10));
    }
    return NAMED_ENTITIES[body.toLowerCase()] ?? entity;
  });
}

function stripTags(htmlFragment: string): string {
  return decodeEntities(htmlFragment.replace(/<[^>]*>/g, "")).trim();
}

/** Parse the first (or `match`ed) HTML table into a DataFrame. DOM-free. */
export function read_html(input: string, options: ReadHtmlOptions = {}): DataFrame {
  assertNonEmpty(input, "HTML document");
  const tables = input.match(/<table[\s\S]*?<\/table\s*>/gi) ?? [];
  const candidates =
    options.match !== undefined
      ? tables.filter((table) => stripTags(table).includes(options.match!))
      : tables;
  if (candidates.length === 0) {
    throw new BunPandaValidationError(
      options.match !== undefined
        ? `No table matching '${options.match}' found in HTML.`
        : "No <table> found in HTML."
    );
  }

  const rowsHtml = candidates[0]!.match(/<tr[\s\S]*?<\/tr\s*>/gi) ?? [];
  const grid = rowsHtml.map((rowHtml) =>
    (rowHtml.match(/<t[hd][^>]*>[\s\S]*?<\/t[hd]\s*>/gi) ?? []).map(stripTags)
  );
  if (grid.length === 0) {
    throw new BunPandaValidationError("Table found but it contains no rows.");
  }

  const firstRowIsHeader =
    options.header ?? /<th[\s>]/i.test(rowsHtml[0]!);
  let columns: string[];
  let bodyRows: string[][];
  if (firstRowIsHeader) {
    columns = grid[0]!.map((cell, i) => (cell === "" ? `col_${i}` : cell));
    bodyRows = grid.slice(1);
  } else {
    bodyRows = grid;
    const width = Math.max(...grid.map((row) => row.length));
    columns = Array.from({ length: width }, (_, i) => `col_${i}`);
  }

  const records: Row[] = bodyRows.map((cells) =>
    Object.fromEntries(columns.map((column, i) => [column, coerceScalar(cells[i] ?? "")]))
  );
  return new DataFrame(records, { columns });
}

// ---------------------------------------------------------------------------
// Clipboard
// ---------------------------------------------------------------------------

function readClipboardText(): string {
  const platform = process.platform;
  if (platform === "darwin") {
    return runCapture("pbpaste", []);
  }
  if (platform === "win32") {
    return runCapture(
      "powershell",
      ["-NoProfile", "-Command", "Get-Clipboard -Raw"]
    );
  }
  // Linux/BSD: prefer xclip, fall back to xsel.
  try {
    return runCapture("xclip", ["-selection", "clipboard", "-o"], true);
  } catch {
    return runCapture("xsel", ["--clipboard", "--output"]);
  }
}

function runCapture(command: string, args: string[], quietFallback = false): string {
  const result = spawnSync(command, args, { encoding: "utf8" });
  if (result.error !== undefined || result.status !== 0) {
    if (quietFallback) throw new Error(`'${command}' failed`);
    throw new NotSupportedError(
      `Could not read the clipboard via '${command}'. ` +
        (result.stderr?.toString().trim() || result.error?.message || "Command failed.") +
        " On Linux install xclip or xsel; paste manually and use read_csv otherwise."
    );
  }
  return result.stdout.toString();
}

/** Read the system clipboard and parse it as delimited text (default TSV). */
export function read_clipboard(options: { sep?: string; header?: boolean } = {}): DataFrame {
  const text = readClipboardText();
  const sep = options.sep ?? "\t";
  const lines = text.split(/\r?\n/).filter((line) => line.length > 0);
  if (lines.length === 0) return new DataFrame([]);
  const delimiter = sep === "\\t" ? "\t" : sep;

  const splitLine = (line: string): string[] => {
    if (delimiter === ",") {
      // Minimal quoted-CSV awareness.
      return line.match(/("([^"]|"")*"|[^,]*)(,|$)/g)?.slice(0, -1)
        .map((cell) => cell.replace(/,$/, "").replace(/^"([\s\S]*)"$/, "$1").replace(/""/g, '"')) ?? [];
    }
    return line.split(delimiter);
  };

  const grid = lines.map(splitLine);
  const hasHeader = options.header ?? true;
  const columns = hasHeader
    ? grid[0]!.map((cell, i) => cell.trim() === "" ? `col_${i}` : cell.trim())
    : Array.from({ length: grid[0]?.length ?? 0 }, (_, i) => `col_${i}`);
  const bodyRows = hasHeader ? grid.slice(1) : grid;
  const records: Row[] = bodyRows.map((cells) =>
    Object.fromEntries(columns.map((column, i) => [column, coerceScalar((cells[i] ?? "").trim())]))
  );
  return new DataFrame(records, { columns });
}

// ---------------------------------------------------------------------------
// Binary formats we do not truly implement
// ---------------------------------------------------------------------------

export function read_pickle(
  path_or_buffer: string | Buffer | Uint8Array,
  options: { index_col?: string | number } = {}
): DataFrame {
  return readStructuredBinary(path_or_buffer, "pickle", options);
}

export function read_feather(
  path_or_buffer: string | Buffer | Uint8Array,
  options: { index_col?: string | number } = {}
): DataFrame {
  return readStructuredBinary(path_or_buffer, "feather", options);
}

export function read_orc(
  path_or_buffer: string | Buffer | Uint8Array,
  options: { index_col?: string | number } = {}
): DataFrame {
  return readStructuredBinary(path_or_buffer, "orc", options);
}

export function read_hdf(
  path_or_buffer: string | Buffer | Uint8Array,
  options: { index_col?: string | number } = {}
): DataFrame {
  return readStructuredBinary(path_or_buffer, "hdf5", options);
}

export function read_sas(
  path_or_buffer: string | Buffer | Uint8Array,
  options: { index_col?: string | number } = {}
): DataFrame {
  return readStructuredBinary(path_or_buffer, "sas", options);
}

export function read_spss(
  path_or_buffer: string | Buffer | Uint8Array,
  options: { index_col?: string | number } = {}
): DataFrame {
  return readStructuredBinary(path_or_buffer, "spss", options);
}

export function read_stata(
  path_or_buffer: string | Buffer | Uint8Array,
  options: { index_col?: string | number } = {}
): DataFrame {
  return readStructuredBinary(path_or_buffer, "stata", options);
}

function readStructuredBinary(
  path_or_buffer: string | Buffer | Uint8Array,
  formatName: string,
  options: { index_col?: string | number }
): DataFrame {
  const payload = typeof path_or_buffer === "string"
    ? readFileSync(path_or_buffer)
    : Buffer.isBuffer(path_or_buffer)
      ? path_or_buffer
      : Buffer.from(
          path_or_buffer.buffer,
          path_or_buffer.byteOffset,
          path_or_buffer.byteLength
        );
  return frameFromJsonPayload(payload, formatName, options);
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

export type SqlQueryFunction = (sql: string, params?: unknown[]) => Row[] | Promise<Row[]>;

export interface SqlConnectionLike {
  query?(sql: string, params?: unknown[]): Row[] | Promise<Row[]> | unknown;
  all?(sql: string, params?: unknown[]): Row[] | Promise<Row[]> | unknown;
  run?(sql: string, params?: unknown[]): unknown;
}

export type SqlEngine = SqlQueryFunction | SqlConnectionLike;

function looksLikeSql(identifier: string): boolean {
  return /\s/.test(identifier) || /^(select|with|pragma|explain)\b/i.test(identifier);
}

async function executeSql(engine: SqlEngine, sql: string, params?: unknown[]): Promise<Row[]> {
  const queryFn =
    typeof engine === "function"
      ? engine
      : (engine.query ?? engine.all ?? engine.run);
  if (typeof queryFn !== "function") {
    throw new NotSupportedError(
      "Unsupported SQL connection: pass a query function `(sql, params?) => rows` " +
        "or an object exposing `.query(sql)` / `.all(sql)`."
    );
  }
  const result = await queryFn.call(engine, sql, params);
  if (!Array.isArray(result)) {
    throw new NotSupportedError(
      "The SQL engine returned a non-array result; expected an array of row objects " +
        "(e.g. `.all()` results from better-sqlite3 / bun:sqlite)."
    );
  }
  return result as Row[];
}

function frameFromRows(rows: Row[]): DataFrame {
  if (rows.length === 0) return new DataFrame([]);
  const columns = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  return new DataFrame(rows, { columns });
}

export async function read_sql_query(
  sql: string,
  con: SqlEngine,
  params?: unknown[]
): Promise<DataFrame> {
  assertNonEmpty(sql, "SQL query");
  return frameFromRows(await executeSql(con, sql, params));
}

export async function read_sql_table(
  table_name: string,
  con: SqlEngine,
  params?: unknown[]
): Promise<DataFrame> {
  assertNonEmpty(table_name, "table name");
  return frameFromRows(await executeSql(con, `SELECT * FROM ${table_name}`, params));
}

export async function read_sql(
  sql_or_table: string,
  con: SqlEngine,
  params?: unknown[]
): Promise<DataFrame> {
  if (looksLikeSql(sql_or_table)) {
    return read_sql_query(sql_or_table, con, params);
  }
  return read_sql_table(sql_or_table, con, params);
}

// ---------------------------------------------------------------------------
// BigQuery stub + XML
// ---------------------------------------------------------------------------

export async function read_gbq(
  _query: string,
  _options: Record<string, unknown> = {}
): Promise<never> {
  throw new NotSupportedError(
    "read_gbq is a stub: Google BigQuery access needs credentials and a client SDK. " +
      "Run your query with @google-cloud/bigquery and build a DataFrame from the returned rows."
  );
}

/** Parse repeated XML elements into rows: attributes plus simple child text nodes. */
export function read_xml(input: string, options: { xpath?: string } = {}): DataFrame {
  assertNonEmpty(input, "XML document");
  const withoutComments = input.replace(/<!--[\s\S]*?-->/g, "");

  // Determine which element repeats: either the caller-provided tag or the
  // most frequent non-root tag name.
  let tagName = options.xpath?.split("/").pop()?.replace(/\[.*\]/, "");
  if (tagName === undefined) {
    const counts = new Map<string, number>();
    for (const open of withoutComments.matchAll(/<([a-zA-Z_][\w.:-]*)\s*[/>]/g)) {
      counts.set(open[1]!, (counts.get(open[1]!) ?? 0) + 1);
    }
    if (counts.size === 0) {
      throw new BunPandaValidationError("No XML elements found.");
    }
    tagName = [...counts.entries()].sort((a, b) => b[1] - a[1])[0]![0];
  }

  const pattern = new RegExp(`<${tagName}\\b([^>]*)>([\\s\\S]*?)</${tagName}\\s*>`, "gi");
  const rows: Row[] = [];
  for (const match of withoutComments.matchAll(pattern)) {
    const attributes = match[1] ?? "";
    const inner = match[2] ?? "";
    const row: Row = {};
    for (const attr of attributes.matchAll(/([\w.:-]+)\s*=\s*["']([^"']*)["']/g)) {
      row[`@${attr[1]}`] = decodeEntities(attr[2]!);
    }
    for (const child of inner.matchAll(/<([\w.:-]+)[^>]*>([\s\S]*?)<\/\1\s*>/g)) {
      row[child[1]!] = coerceScalar(decodeEntities(child[2]!.replace(/<[^>]*>/g, "").trim()));
    }
    rows.push(row);
  }
  if (rows.length === 0) {
    throw new BunPandaValidationError(`No <${tagName}> elements found in XML.`);
  }
  const columns = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  return new DataFrame(rows, { columns });
}
