import { readFileSync } from "node:fs";

const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8")) as {
  name: string;
  version: string;
};

/** Prints runtime and library versions (pandas show_versions). */
export function show_versions(): string {
  const lines = [
    `${pkg.name}: ${pkg.version}`,
    "",
    "INSTALLED VERSIONS",
    "------------------",
    `bun        : ${typeof Bun !== "undefined" ? Bun.version : "unknown"}`,
    `typescript : ${process.env.TS_VERSION ?? "(runtime-supplied)"}`,
    `python     : not required`,
  ];
  const text = lines.join("\n");
  console.log(text);
  return text;
}

/**
 * Minimal pandas.test(): delegates to the repo test suite. In-library use
 * just reports how to run the suite; CI runs `bun test` directly.
 */
export function test(): never {
  throw new Error(
    "test(): run the suite directly with `bun test` (see README Development section)."
  );
}
