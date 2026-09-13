import type {
  BuildMetadata,
  ExperimentConfig,
  ExperimentReport,
  WorkerRequest,
  WorkerResponse,
  WorkloadResult,
} from "./types";

const $ = <T extends Element>(selector: string): T => {
  const element = document.querySelector<T>(selector);
  if (!element) throw new Error(`Missing interface element: ${selector}`);
  return element;
};

const form = $("#experiment-form") as HTMLFormElement;
const runButton = $("#run-button") as HTMLButtonElement;
const cancelButton = $("#cancel-button") as HTMLButtonElement;
const progress = $("#progress") as HTMLProgressElement;
const progressValue = $("#progress-value") as HTMLOutputElement;
const progressStage = $("#progress-stage");
const workerLight = $("#worker-light");
const workerState = $("#worker-state");
const workloadGrid = $("#workload-grid");
const emptyState = $("#empty-state");
const downloadButton = $("#download-button") as HTMLButtonElement;
const rowsInput = $("#rows") as HTMLInputElement;
const selectivityInput = $("#selectivity") as HTMLInputElement;
let worker: Worker | undefined;
let latestReport: ExperimentReport | undefined;

const formatBytes = (bytes: number): string => {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)} KiB`;
  return `${(bytes / 1024 ** 2).toFixed(2)} MiB`;
};
const formatMs = (value: number): string => value < 1 ? `${(value * 1000).toFixed(0)} µs` : `${value.toFixed(2)} ms`;
const escapeHtml = (value: string): string => value.replace(/[&<>"]/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[character]!);

function sparkline(samples: number[], className: string): string {
  const width = 148;
  const height = 28;
  const minimum = Math.min(...samples);
  const maximum = Math.max(...samples);
  const span = maximum - minimum || 1;
  const points = samples.map((sample, index) => {
    const x = samples.length === 1 ? width / 2 : index / (samples.length - 1) * width;
    const y = height - 3 - (sample - minimum) / span * (height - 6);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return `<svg class="sparkline ${className}" viewBox="0 0 ${width} ${height}" role="img" aria-label="Timing sample distribution"><polyline points="${points}" /></svg>`;
}

function workloadCard(result: WorkloadResult, index: number): string {
  const longest = Math.max(result.typescript.medianMs, result.wasm.medianMs, Number.EPSILON);
  const tsWidth = Math.max(4, result.typescript.medianMs / longest * 100);
  const wasmWidth = Math.max(4, result.wasm.medianMs / longest * 100);
  const speedLabel = result.speedup >= 1
    ? `${result.speedup.toFixed(2)}× Wasm speedup`
    : `${(1 / result.speedup).toFixed(2)}× TypeScript speedup`;
  return `
    <article class="workload-card" style="--card-order:${index}">
      <header>
        <span class="kernel-number">K${String(index + 1).padStart(2, "0")}</span>
        <span class="parity ${result.equivalent ? "pass" : "fail"}">${result.equivalent ? "DIGEST MATCH" : "MISMATCH"}</span>
      </header>
      <h3>${escapeHtml(result.label)}</h3>
      <p>${escapeHtml(result.description)}</p>
      <div class="race-row ts">
        <div><span>TypeScript</span><strong>${formatMs(result.typescript.medianMs)}</strong></div>
        <i style="--bar-width:${tsWidth}%"></i>
        ${sparkline(result.typescript.samplesMs, "ts-line")}
      </div>
      <div class="race-row wasm">
        <div><span>Rust / Wasm</span><strong>${formatMs(result.wasm.medianMs)}</strong></div>
        <i style="--bar-width:${wasmWidth}%"></i>
        ${sparkline(result.wasm.samplesMs, "wasm-line")}
      </div>
      <footer>
        <strong class="winner ${result.winner}">${speedLabel}</strong>
        <span>p95 ${formatMs(result[result.winner].p95Ms)} · ${result.outputLength.toLocaleString()} outputs</span>
        <span class="dispatch">Policy: ${escapeHtml(result.dispatch.path)} / ${escapeHtml(result.dispatch.reason.replaceAll("-", " "))}</span>
      </footer>
    </article>`;
}

function setProgress(stage: string, value: number): void {
  progress.value = value;
  progress.textContent = `${value}%`;
  progressValue.value = `${value}%`;
  progressStage.textContent = stage;
}

function readConfig(): ExperimentConfig {
  const value = (name: string): string => {
    const control = form.elements.namedItem(name) as HTMLInputElement | HTMLSelectElement | null;
    if (!control) throw new Error(`Missing experiment control: ${name}`);
    return control.value;
  };
  return {
    rows: Number(value("rows")),
    groups: Number(value("groups")),
    selectivity: Number(value("selectivity")) / 100,
    iterations: Number(value("iterations")),
    warmups: 2,
    seed: Number(value("seed")),
    shape: value("shape") as ExperimentConfig["shape"],
  };
}

function setRunning(running: boolean): void {
  runButton.disabled = running;
  cancelButton.hidden = !running;
  form.querySelectorAll<HTMLInputElement | HTMLSelectElement>("input, select, .preset").forEach((control) => {
    control.disabled = running;
  });
  workerLight.classList.toggle("active", running);
}

function spawnWorker(): Worker {
  worker?.terminate();
  const next = new Worker("/assets/analytics.worker.js", { type: "module" });
  next.onmessage = (event: MessageEvent<WorkerResponse>) => {
    const message = event.data;
    if (message.type === "ready") {
      workerState.textContent = "Worker ready";
      workerLight.classList.add("ready");
    } else if (message.type === "progress") {
      setProgress(message.stage, message.percent);
      workerState.textContent = "Experiment running";
    } else if (message.type === "complete") {
      latestReport = message.report;
      renderReport(message.report);
      const saved = persistReport(message.report);
      setProgress(saved ? "Experiment complete" : "Experiment complete · browser history unavailable", 100);
      setRunning(false);
      workerState.textContent = "Worker ready";
      downloadButton.disabled = false;
    } else if (message.type === "error") {
      setProgress(`Run failed: ${message.message}`, 0);
      setRunning(false);
      workerState.textContent = "Worker fault";
      workerLight.classList.remove("active", "ready");
    }
  };
  next.onerror = (event) => {
    setProgress(`Worker failed: ${event.message}`, 0);
    setRunning(false);
    workerState.textContent = "Worker fault";
  };
  worker = next;
  return next;
}

function renderReport(report: ExperimentReport): void {
  workloadGrid.innerHTML = report.workloads.map(workloadCard).join("");
  workloadGrid.removeAttribute("hidden");
  emptyState.setAttribute("hidden", "");
  $("#wasm-size").textContent = formatBytes(report.wasmAssetBytes);
  $("#input-size").textContent = formatBytes(report.inputBytes);
  $("#wasm-init").textContent = formatMs(report.wasmInitMs);
  const allEquivalent = report.workloads.every((item) => item.equivalent);
  $("#integrity").textContent = allEquivalent ? "4 / 4" : "FAILED";
  $("#integrity").classList.toggle("warning", !allEquivalent);
  const wasmWins = report.workloads.filter((item) => item.winner === "wasm").length;
  $("#hero-decision").textContent = `${wasmWins} wasm · ${report.workloads.length - wasmWins} ts wins`;

  const basics: Array<[string, boolean]> = [
    ["WebAssembly", report.capabilities.webAssembly],
    ["Streaming", report.capabilities.instantiateStreaming],
    ["Cross-origin isolated", report.capabilities.crossOriginIsolated],
    ["SharedArrayBuffer", report.capabilities.sharedArrayBuffer],
  ];
  const capabilityMarkup = [...basics, ...Object.entries(report.capabilities.features)]
    .map(([name, supported]) => `<span class="capability ${supported ? "supported" : "unsupported"}"><i></i>${escapeHtml(name)}</span>`)
    .join("");
  $("#capabilities").innerHTML = capabilityMarkup
    + `<span class="capability hardware"><i></i>${report.capabilities.hardwareConcurrency} logical cores</span>`;
  $("#isolation-note").textContent = report.capabilities.crossOriginIsolated
    ? "COOP/COEP isolation is active."
    : "Cross-origin isolation is unavailable.";
}

const historyKey = "bun-panda-wasm-studio-history-v1";
function readHistory(): ExperimentReport[] {
  try {
    const history = JSON.parse(localStorage.getItem(historyKey) ?? "[]") as ExperimentReport[];
    return history.filter((report) =>
      typeof report?.runId === "string" &&
      typeof report.createdAt === "string" &&
      Number.isFinite(report.config?.rows) &&
      typeof report.config?.shape === "string" &&
      Array.isArray(report.workloads) && report.workloads.length > 0 &&
      report.workloads.every((item) => item &&
        (item.winner === "wasm" || item.winner === "typescript") &&
        Number.isFinite(item.speedup) && item.speedup > 0 &&
        typeof item.equivalent === "boolean")
    ).slice(0, 6);
  }
  catch { return []; }
}
function persistReport(report: ExperimentReport): boolean {
  const history = [report, ...readHistory().filter((item) => item.runId !== report.runId)].slice(0, 6);
  renderHistory(history);
  try {
    localStorage.setItem(historyKey, JSON.stringify(history));
    return true;
  } catch {
    return false;
  }
}
function renderHistory(history: ExperimentReport[]): void {
  const body = $("#history-body");
  if (history.length === 0) return;
  body.innerHTML = history.map((report) => {
    const wasmWins = report.workloads.filter((item) => item.winner === "wasm").length;
    const fastest = Math.max(...report.workloads.map((item) => Math.max(item.speedup, 1 / item.speedup)));
    const parity = report.workloads.every((item) => item.equivalent);
    return `<tr><td>${new Date(report.createdAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}</td><td>${report.config.rows.toLocaleString()}</td><td>${escapeHtml(report.config.shape)}</td><td>${wasmWins} / ${report.workloads.length}</td><td>${fastest.toFixed(2)}×</td><td><span class="table-status ${parity ? "pass" : "fail"}">${parity ? "pass" : "fail"}</span></td></tr>`;
  }).join("");
}

async function renderBuildMetadata(): Promise<void> {
  try {
    const response = await fetch("/build-meta.json");
    const metadata = await response.json() as BuildMetadata;
    const rows: Array<[string, number]> = [
      ["Main interface", metadata.assets.mainJavaScript],
      ["Benchmark worker", metadata.assets.workerJavaScript],
      ["Rust/Wasm kernel", metadata.assets.wasm],
      ["Visual system", metadata.assets.styles],
    ];
    const maximum = Math.max(...rows.map(([, bytes]) => bytes));
    $("#bundle-ledger").innerHTML = rows.map(([name, bytes]) => `
      <div><span>${name}</span><i style="--ledger-width:${bytes / maximum * 100}%"></i><strong>${formatBytes(bytes)}</strong></div>`).join("")
      + `<p>${formatBytes(metadata.assets.total)} total uncompressed transfer footprint</p>`;
  } catch {
    $("#bundle-ledger").innerHTML = "<p>Build manifest unavailable.</p>";
  }
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  const activeWorker = spawnWorker();
  setRunning(true);
  setProgress("Queueing worker experiment", 2);
  const request: WorkerRequest = { type: "run", config: readConfig(), wasmUrl: "/assets/bun_panda_core.wasm" };
  activeWorker.postMessage(request);
});

cancelButton.addEventListener("click", () => {
  worker?.terminate();
  worker = undefined;
  setRunning(false);
  setProgress("Experiment cancelled", 0);
  workerState.textContent = "Worker restarting";
  spawnWorker();
});

downloadButton.addEventListener("click", () => {
  if (!latestReport) return;
  const blob = new Blob([JSON.stringify(latestReport, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `bun-panda-browser-${latestReport.runId}.json`;
  anchor.click();
  URL.revokeObjectURL(url);
});

rowsInput.addEventListener("input", () => { ($("#rows-output") as HTMLOutputElement).value = Number(rowsInput.value).toLocaleString(); });
selectivityInput.addEventListener("input", () => { ($("#selectivity-output") as HTMLOutputElement).value = `${selectivityInput.value}%`; });
document.querySelectorAll<HTMLButtonElement>(".preset").forEach((button) => button.addEventListener("click", () => {
  const presets = {
    quick: { rows: "20000", iterations: "5", shape: "uniform", groups: "16", selectivity: "50" },
    stress: { rows: "250000", iterations: "10", shape: "nearly-sorted", groups: "256", selectivity: "75" },
    skew: { rows: "150000", iterations: "10", shape: "skewed", groups: "64", selectivity: "20" },
  } as const;
  const preset = presets[button.dataset.preset as keyof typeof presets];
  for (const [name, value] of Object.entries(preset)) {
    const control = form.elements.namedItem(name) as HTMLInputElement | HTMLSelectElement;
    control.value = value;
    control.dispatchEvent(new Event("input"));
  }
}));

renderHistory(readHistory());
void renderBuildMetadata();
spawnWorker();
