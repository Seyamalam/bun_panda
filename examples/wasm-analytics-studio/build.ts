import { mkdir, rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const exampleDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = join(exampleDirectory, "../..");
export const outputDirectory = join(exampleDirectory, "dist");
const assetsDirectory = join(outputDirectory, "assets");

export interface BuildMetadata {
  builtAt: string;
  assets: {
    mainJavaScript: number;
    workerJavaScript: number;
    styles: number;
    wasm: number;
    total: number;
  };
}

async function copy(source: string, destination: string): Promise<number> {
  const sourceFile = Bun.file(source);
  await Bun.write(destination, sourceFile);
  return sourceFile.size;
}

export async function buildShowcase(): Promise<BuildMetadata> {
  await rm(outputDirectory, { recursive: true, force: true });
  await mkdir(assetsDirectory, { recursive: true });

  const result = await Bun.build({
    entrypoints: [
      join(exampleDirectory, "src/main.ts"),
      join(exampleDirectory, "src/analytics.worker.ts"),
    ],
    outdir: assetsDirectory,
    target: "browser",
    format: "esm",
    sourcemap: "linked",
    naming: "[name].[ext]",
  });

  if (!result.success) {
    for (const log of result.logs) console.error(log);
    throw new Error("The browser showcase bundle did not compile.");
  }

  const [htmlBytes, styleBytes, wasmBytes] = await Promise.all([
    copy(join(exampleDirectory, "src/index.html"), join(outputDirectory, "index.html")),
    copy(join(exampleDirectory, "src/styles.css"), join(assetsDirectory, "styles.css")),
    copy(
      join(repositoryRoot, "src/wasm/bun_panda_core.wasm"),
      join(assetsDirectory, "bun_panda_core.wasm"),
    ),
  ]);

  const mainBytes = Bun.file(join(assetsDirectory, "main.js")).size;
  const workerBytes = Bun.file(join(assetsDirectory, "analytics.worker.js")).size;
  const metadata: BuildMetadata = {
    builtAt: new Date().toISOString(),
    assets: {
      mainJavaScript: mainBytes,
      workerJavaScript: workerBytes,
      styles: styleBytes,
      wasm: wasmBytes,
      total: mainBytes + workerBytes + styleBytes + wasmBytes + htmlBytes,
    },
  };
  await Bun.write(join(outputDirectory, "build-meta.json"), JSON.stringify(metadata, null, 2));
  return metadata;
}

if (import.meta.main) {
  const metadata = await buildShowcase();
  console.log(`Built Wasm Analytics Studio (${metadata.assets.total.toLocaleString()} bytes).`);
}
