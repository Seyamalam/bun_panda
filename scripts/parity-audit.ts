// Parity audit: diff the pandas public API (scraped from the official
// reference docs) against bun_panda's implemented surface.
import { readFileSync, writeFileSync } from "node:fs";

// Baseline is committed (scripts/pandas-api-baseline.txt) so the audit runs
// offline; refresh it with scripts/refresh-pandas-baseline.sh.
const methodLines = readFileSync(
  new URL("./pandas-api-baseline.txt", import.meta.url),
  "utf8"
)
  .split("\n")
  .filter(Boolean)
  .map((line) => line.replace("pandas.", ""));

const frameMethods = methodLines
  .filter((m) => m.startsWith("DataFrame."))
  .map((m) => m.slice("DataFrame.".length))
  .filter((m) => !m.startsWith("__"));

const seriesMethods = methodLines
  .filter((m) => m.startsWith("Series."))
  .map((m) => m.slice("Series.".length))
  .filter((m) => !m.startsWith("__"));

// Top-level pandas functions (curated from the reference index — the
// frame/series pages don't list these).
const topLevel = [
  "read_csv", "read_table", "read_fwf", "read_clipboard", "read_excel",
  "read_feather", "read_gbq", "read_hdf", "read_html", "read_json",
  "read_json_lines", "read_orc", "read_parquet", "read_pickle", "read_sas",
  "read_spss", "read_sql", "read_sql_query", "read_sql_table", "read_stata",
  "read_xml", "DataFrame", "Series", "to_datetime", "to_timedelta",
  "to_numeric", "date_range", "bdate_range", "period_range", "timedelta_range",
  "interval_range", "Index", "MultiIndex", "Categorical", "CategoricalDtype",
  "Interval", "Timestamp", "DatetimeIndex", "Timedelta", "TimedeltaIndex",
  "Period", "PeriodIndex", "NaT", "NA", "isna", "isnull", "notna", "notnull",
  "concat", "merge", "merge_ordered", "merge_asof", "pivot", "pivot_table",
  "crosstab", "cut", "qcut", "get_dummies", "melt", "lreshape", "wide_to_long",
  "factorize", "unique", "value_counts", "isin", "map", "align", "array",
  "test", "describe_option", "reset_option", "get_option", "set_option",
  "option_context", "show_versions",
];

// ---- Extract bun_panda surface from source ----
function extractClassMethods(path: string, className: string) {
  const src = readFileSync(path, "utf8");
  const classStart = src.indexOf(`class ${className}`);
  if (classStart < 0) return [];
  // Find the matching closing brace by brace counting.
  let depth = 0;
  let start = -1;
  for (let i = classStart; i < src.length; i += 1) {
    if (src[i] === "{") {
      if (depth === 0) start = i;
      depth += 1;
    } else if (src[i] === "}") {
      depth -= 1;
      if (depth === 0) {
        return extractMethodNames(src.slice(start, i));
      }
    }
  }
  return extractMethodNames(src.slice(classStart));
}

function extractMethodNames(body: string) {
  const names = new Set();
  const re = /^\s{2}([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:<[^>]*>)?\(/gm;
  let match;
  while ((match = re.exec(body)) !== null) {
    names.add(match[1]!);
  }
  // getters
  const getterRe = /^\s{2}(?:get|set)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(/gm;
  while ((match = getterRe.exec(body)) !== null) {
    names.add(match[1]!);
  }
  return [...names];
}

const dfMethods = extractClassMethods("src/dataframe.ts", "DataFrame");
const serMethods = extractClassMethods("src/series.ts", "Series");

const groupbySrc = readFileSync("src/groupby.ts", "utf8");
const groupbyMethods = extractMethodNames(groupbySrc);

const indexSrc = readFileSync("src/index.ts", "utf8");
const ioSrc = readFileSync("src/io.ts", "utf8");
const exportedTopLevel = new Set<string>();
for (const m of indexSrc.matchAll(/^\s{2}([a-zA-Z_][a-zA-Z0-9_]*)\s*,?\s*$/gm)) {
  exportedTopLevel.add(m[1]!);
}
for (const m of ioSrc.matchAll(/^export (?:async )?function ([a-zA-Z_][a-zA-Z0-9_]*)/gm)) {
  exportedTopLevel.add(m[1]!);
}

// pandas name -> bun_panda name aliases
const aliases = new Map<string, string[]>([
  ["to_records", ["to_records", "toRecords"]],
  ["to_dict", ["to_dict", "toDict"]],
  ["dtypes", ["dtypes", "dtypes"]],
  ["isin", ["isin"]],
  ["rename", ["rename"]],
  ["aggregate", ["agg", "aggregate"]],
  ["__iter__", []],
]);

function hasAny(names: string[], candidate: string): boolean {
  const alts = aliases.get(candidate) ?? [candidate];
  return alts.some((alt) => names.includes(alt));
}

function auditSection(pandasNames: string[], ours: string[], label: string) {
  const have = pandasNames.filter((name: string) => hasAny(ours, name));
  const missing = pandasNames.filter((name: string) => !hasAny(ours, name));
  const pct = pandasNames.length ? Math.round((have.length / pandasNames.length) * 100) : 0;
  return { label, total: pandasNames.length, have: have.length, missing, pct };
}

const sections = [
  auditSection(frameMethods, dfMethods, "DataFrame"),
  auditSection(seriesMethods, serMethods, "Series"),
  auditSection(topLevel, [...exportedTopLevel] as string[], "Top-level"),
  auditSection(
    ["agg", "transform", "filter", "apply", "size", "count", "sum", "mean",
     "median", "std", "var", "min", "max", "first", "last", "nunique",
     "value_counts", "rolling", "resample", "pipe", "idxmin", "idxmax",
     "skew", "kurt", "sem", "quantile", "cumsum", "cumprod", "cummin", "cummax",
     "shift", "diff", "pct_change", "rank", "corr", "cov", "describe", "ohlc"],
    groupbyMethods,
    "GroupBy"
  ),
];

const totalPandas = sections.reduce((sum, s) => sum + s.total, 0);
const totalHave = sections.reduce((sum, s) => sum + s.have, 0);

const lines: string[] = [];
lines.push("# pandas API Parity Audit");
lines.push("");
lines.push(`Generated ${new Date().toISOString().slice(0, 10)} — pandas methods scraped from the official API reference (frame.html, series.html), bun_panda surface extracted from \`src/\`.`);
lines.push("");
lines.push("| Surface | pandas API | bun_panda | Parity |");
lines.push("| --- | ---: | ---: | ---: |");
for (const s of sections) {
  lines.push(`| ${s.label} | ${s.total} | ${s.have} | ${s.pct}% |`);
}
lines.push(`| **Total** | **${totalPandas}** | **${totalHave}** | **${Math.round((totalHave / totalPandas) * 100)}%** |`);
lines.push("");

for (const s of sections) {
  lines.push(`## ${s.label} — missing (${s.missing.length})`);
  lines.push("");
  // Group into rough families for readability.
  const families = new Map<string, string[]>();
  for (const name of s.missing.sort()) {
    const family =
      name.startsWith("plot") ? "plotting" :
      name.startsWith("to_") ? "export" :
      name.startsWith("read_") ? "io" :
      name.match(/^(cum|rolling|expanding)/) ? "window/cumulative" :
      name.match(/^(add|sub|mul|div|mod|pow|radd|rsub|rmul|rdiv|rmod|rpow|truediv|floordiv|neg|abs|round|clip)/) ? "arithmetic" :
      name.match(/^(eq|ne|lt|le|gt|ge)/) ? "comparison" :
      name.match(/^(str|dt|cat)_/) || name === "str" || name === "dt" ? "accessors" :
      name.match(/^(isna|notna|isnull|notnull|fillna|dropna|interpolate|ffill|bfill)/) ? "missing data" :
      name.match(/^(idx|nlargest|nsmallest|sample)/) ? "selection" :
      "other";
    if (!families.has(family)) families.set(family, []);
    families.get(family)!.push(name);
  }
  for (const [family, names] of [...families.entries()].sort()) {
    lines.push(`### ${family} (${names.length})`);
    lines.push("");
    lines.push(names.map((n) => `\`${n}\``).join(", "));
    lines.push("");
  }
}

writeFileSync("docs/PARITY.md", `${lines.join("\n")}\n`);

console.log(lines.slice(0, 16).join("\n"));
console.log(`\nFull detail written to docs/PARITY.md`);
