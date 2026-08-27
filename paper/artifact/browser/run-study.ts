import { mkdirSync, writeFileSync } from "node:fs";
import { chromium, firefox, webkit } from "@playwright/test";

const sizes = (process.env.BUN_PANDA_BROWSER_SIZES ?? "10000,100000").split(",").map(Number);
const replicates = Number(process.env.BUN_PANDA_BROWSER_REPLICATES ?? "30");
const warmups = Number(process.env.BUN_PANDA_BROWSER_WARMUPS ?? "3");
const iterations = Number(process.env.BUN_PANDA_BROWSER_ITERATIONS ?? "10");
const outputPath = process.env.BUN_PANDA_BROWSER_OUTPUT ?? "paper/data/browser-study.json";
const requestedBrowsers = (process.env.BUN_PANDA_BROWSERS ?? "chromium,firefox,webkit").split(",");
const baseSeed = 20260826;

const build = await Bun.build({
  entrypoints: ["paper/artifact/browser/worker.ts"],
  target: "browser",
  format: "esm",
  minify: true,
  sourcemap: "none",
  write: false,
});
if (!build.success || !build.outputs[0]) {
  throw new Error(`browser worker build failed: ${build.logs.map(String).join("\n")}`);
}
const workerJavaScript = await build.outputs[0].text();
const wasmBytes = await Bun.file("src/wasm/bun_panda_core.wasm").arrayBuffer();
const headers = {
  "Cross-Origin-Opener-Policy": "same-origin",
  "Cross-Origin-Embedder-Policy": "require-corp",
  "Cross-Origin-Resource-Policy": "same-origin",
};
const server = Bun.serve({
  port: 0,
  fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/worker.js") {
      return new Response(workerJavaScript, { headers: { ...headers, "Content-Type": "text/javascript" } });
    }
    if (url.pathname === "/bun_panda_core.wasm") {
      return new Response(wasmBytes, { headers: { ...headers, "Content-Type": "application/wasm" } });
    }
    return new Response("<!doctype html><meta charset=utf-8><title>bun_panda browser study</title>", {
      headers: { ...headers, "Content-Type": "text/html" },
    });
  },
});

const browserTypes = { chromium, firefox, webkit } as const;
const raw: Record<string, unknown>[] = [];
try {
  for (const browserName of requestedBrowsers) {
    const browserType = browserTypes[browserName as keyof typeof browserTypes];
    if (!browserType) throw new Error(`unknown browser ${browserName}`);
    for (const rows of sizes) {
      for (let replicate = 0; replicate < replicates; replicate += 1) {
        const browser = await browserType.launch({ headless: true });
        const page = await browser.newPage();
        await page.goto(`http://127.0.0.1:${server.port}/`, { waitUntil: "load" });
        const userAgent = await page.evaluate(() => navigator.userAgent);
        const started = Bun.nanoseconds();
        const result = await page.evaluate(({ rows: rowCount, seed, warmups: warmupCount, iterations: iterationCount }) =>
          new Promise<Record<string, unknown>>((resolve, reject) => {
            const worker = new Worker("/worker.js", { type: "module" });
            worker.onmessage = (event) => {
              worker.terminate();
              resolve(event.data);
            };
            worker.onerror = (event) => {
              worker.terminate();
              reject(new Error(event.message));
            };
            worker.postMessage({
              rows: rowCount,
              seed,
              warmups: warmupCount,
              iterations: iterationCount,
              wasmUrl: "/bun_panda_core.wasm",
            });
          }), { rows, seed: baseSeed + replicate, warmups, iterations });
        raw.push({
          browser: browserName,
          browserVersion: browser.version(),
          userAgent,
          replicate,
          pageToResultMs: (Bun.nanoseconds() - started) / 1_000_000,
          ...result,
        });
        await browser.close();
        console.log(`browser progress ${browserName}, n=${rows}, replicate=${replicate + 1}/${replicates}`);
      }
    }
  }
} finally {
  server.stop(true);
}

const payload = {
  schemaVersion: "1.0.0",
  generatedAt: new Date().toISOString(),
  design: {
    browsers: requestedBrowsers,
    sizes,
    replicates,
    warmups,
    iterations,
    baseSeed,
    execution: "dedicated module worker under cross-origin isolation",
    coldMetric: "Wasm fetch, compile, instantiate, and export validation inside a fresh worker",
    warmMetrics: "kernel calls include copies into and out of Wasm linear memory",
    baseline: "equivalent handwritten JavaScript over the same typed arrays",
    correctness: "SHA-256 digest equality for every JavaScript and Wasm output",
  },
  raw,
};
mkdirSync("paper/data", { recursive: true });
writeFileSync(outputPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
console.log(`wrote ${raw.length} browser observations to ${outputPath}`);
