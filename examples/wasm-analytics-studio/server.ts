import { extname, join } from "node:path";
import { buildShowcase, outputDirectory } from "./build";

await buildShowcase();

const contentTypes: Record<string, string> = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".wasm": "application/wasm",
};

const commonHeaders = {
  "Cache-Control": "no-store",
  "Cross-Origin-Embedder-Policy": "require-corp",
  "Cross-Origin-Opener-Policy": "same-origin",
  "Cross-Origin-Resource-Policy": "same-origin",
  "X-Content-Type-Options": "nosniff",
};

const requestedPort = Number(Bun.env.PORT ?? 4173);
const server = Bun.serve({
  port: Number.isInteger(requestedPort) ? requestedPort : 4173,
  async fetch(request) {
    const url = new URL(request.url);
    const relativePath = url.pathname === "/" ? "index.html" : decodeURIComponent(url.pathname.slice(1));
    if (relativePath.split("/").includes("..")) {
      return new Response("Bad request", { status: 400, headers: commonHeaders });
    }

    const asset = Bun.file(join(outputDirectory, relativePath));
    if (!(await asset.exists())) {
      return new Response("Not found", { status: 404, headers: commonHeaders });
    }

    return new Response(asset, {
      headers: {
        ...commonHeaders,
        "Content-Type": contentTypes[extname(relativePath)] ?? "application/octet-stream",
      },
    });
  },
});

console.log(`Wasm Analytics Studio running at ${server.url}`);
