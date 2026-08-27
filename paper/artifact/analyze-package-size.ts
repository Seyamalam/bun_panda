import {
  cpSync,
  mkdtempSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join, relative } from "node:path";
import { brotliCompressSync, constants, gzipSync } from "node:zlib";

const ROOT = process.cwd();
const OUTPUT = process.env.BUN_PANDA_PACKAGE_SIZE_OUTPUT ??
  join(ROOT, "paper/data/package-size.json");

function walk(path: string): string[] {
  const out: string[] = [];
  for (const name of readdirSync(path)) {
    const child = join(path, name);
    const stat = statSync(child);
    if (stat.isDirectory()) out.push(...walk(child));
    else out.push(child);
  }
  return out;
}

function bytes(path: string): number {
  return statSync(path).size;
}

function sum(paths: string[]): number {
  return paths.reduce((total, path) => total + bytes(path), 0);
}

function run(command: string[], cwd = ROOT): string {
  const result = Bun.spawnSync(command, { cwd, stdout: "pipe", stderr: "pipe" });
  if (result.exitCode !== 0) {
    throw new Error(
      `${command.join(" ")} failed:\n${result.stderr.toString()}\n${result.stdout.toString()}`
    );
  }
  return result.stdout.toString();
}

const temporary = mkdtempSync(join(tmpdir(), "bun-panda-paper-size-"));
try {
  const bundle = join(temporary, "bun-panda.min.js");
  const browserBundle = join(temporary, "bun-panda-browser.min.js");
  run([
    "bun",
    "build",
    "index.ts",
    "--target=bun",
    "--minify",
    "--external=xlsx",
    "--external=parquetjs-lite",
    `--outfile=${bundle}`,
  ]);
  run([
    "bun",
    "build",
    "src/browser.ts",
    "--target=browser",
    "--format=esm",
    "--minify",
    `--outfile=${browserBundle}`,
  ]);

  const packOutput = run(["npm", "pack", "--json", "--ignore-scripts"], ROOT);
  const pack = JSON.parse(packOutput)[0] as {
    filename: string;
    size: number;
    unpackedSize: number;
    entryCount: number;
    files: { path: string; size: number }[];
  };
  const tarballPath = join(ROOT, pack.filename);
  const storedTarball = join(temporary, basename(pack.filename));
  cpSync(tarballPath, storedTarball);
  rmSync(tarballPath);

  const sourceFiles = walk(join(ROOT, "src"));
  const categories = ["dataframe", "series", "groupby", "io", "wasm", "other"];
  const sourceByCategory = Object.fromEntries(categories.map((category) => [category, 0]));
  for (const file of sourceFiles) {
    const path = relative(join(ROOT, "src"), file).replaceAll("\\", "/");
    const category = path.includes("wasm/")
      ? "wasm"
      : path.includes("internal/dataframe/") || path === "dataframe.ts"
        ? "dataframe"
        : path.includes("internal/series/") || path === "series.ts"
          ? "series"
          : path.includes("groupby") || path === "groupby.ts"
            ? "groupby"
            : path.includes("/io") || path.startsWith("io/")
              ? "io"
              : "other";
    sourceByCategory[category] += bytes(file);
  }

  const bundleBuffer = readFileSync(bundle);
  const browserBundleBuffer = readFileSync(browserBundle);
  const wasmPath = join(ROOT, "src/wasm/bun_panda_core.wasm");
  const wasmBuffer = readFileSync(wasmPath);
  const brotli = (buffer: Buffer): number => brotliCompressSync(buffer, {
    params: { [constants.BROTLI_PARAM_QUALITY]: 11 },
  }).byteLength;
  const payload = {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    package: JSON.parse(readFileSync(join(ROOT, "package.json"), "utf8")),
    artifacts: {
      npmTarballBytes: pack.size,
      npmUnpackedBytes: pack.unpackedSize,
      npmEntryCount: pack.entryCount,
      coreMinifiedBundleBytes: bundleBuffer.byteLength,
      coreMinifiedBundleGzipBytes: gzipSync(bundleBuffer, { level: 9 }).byteLength,
      browserKernelMinifiedBytes: browserBundleBuffer.byteLength,
      browserKernelGzipBytes: gzipSync(browserBundleBuffer, { level: 9 }).byteLength,
      browserKernelBrotliBytes: brotli(browserBundleBuffer),
      wasmBinaryBytes: bytes(wasmPath),
      wasmBinaryGzipBytes: gzipSync(wasmBuffer, { level: 9 }).byteLength,
      wasmBinaryBrotliBytes: brotli(wasmBuffer),
      sourceBytes: sum(sourceFiles),
      sourceFileCount: sourceFiles.length,
    },
    method: {
      bundleCommand:
        "bun build index.ts --target=bun --minify --external=xlsx --external=parquetjs-lite",
      bundleScope:
        "library code with optional XLSX and Parquet packages externalized; WASM is a separate runtime asset",
      browserBundleCommand:
        "bun build src/browser.ts --target=browser --format=esm --minify",
      browserBundleScope:
        "async typed-kernel entry only; no DataFrame API or Node/Bun imports",
      gzipLevel: 9,
      brotliQuality: 11,
      packCommand: "npm pack --json --ignore-scripts",
    },
    sourceByCategory,
    largestPackageFiles: [...pack.files]
      .sort((left, right) => right.size - left.size)
      .slice(0, 15),
  };

  mkdirSync(dirname(OUTPUT), { recursive: true });
  writeFileSync(OUTPUT, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  console.log(JSON.stringify(payload.artifacts, null, 2));
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
