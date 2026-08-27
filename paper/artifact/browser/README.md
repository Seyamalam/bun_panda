# Browser worker study

The study bundles the browser entry and runs it in a dedicated module worker.
The local test server sends cross-origin isolation headers so the result can
state whether each browser exposes shared memory and Wasm threads.

Install the pinned Playwright browsers once:

```sh
bunx playwright install chromium firefox webkit
```

Run the study:

```sh
bun run paper/artifact/browser/run-study.ts
```

Each fresh worker measures Wasm initialization and warm argsort, filtering,
and grouped-sum kernels. Equivalent handwritten JavaScript runs over the same
typed arrays. SHA-256 checks must match before the script writes a result.
The paper setting is the default: 30 fresh contexts for each browser and row
count. Set `BUN_PANDA_BROWSER_REPLICATES=1` only for a smoke check.
