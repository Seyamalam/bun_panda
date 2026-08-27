# Cross-platform reproduction

The manuscript currently reports performance measured on one Apple M5 Pro
running macOS. Windows and Ubuntu reports collected with this procedure are
independent validation inputs. Do not combine them with the manuscript tables
or describe them as published results until the author has checked the machine,
versions, correctness gates, and raw measurements.

The reproduction runner works on macOS, Windows, and Linux. It runs the test
and benchmark protocol in a clean clone, then writes one dated JSON file. The
tester only needs to send that file back.

## What the full run does

The default `full` profile records the operating system, architecture, CPU,
memory, tool versions, Git commit, and initial working-tree state. It then runs:

1. the locked Bun dependency installation;
2. type checking, linting, and the complete Bun test suite;
3. npm package packing and import checks;
4. the API name census;
5. package, bundle, compression, and Wasm size measurements;
6. the 2,500-case pandas differential and backend conformance study;
7. the fresh-process TypeScript, Wasm, and adaptive ablation;
8. the five-system synthetic comparison;
9. the checksum-pinned UCI Bank Marketing comparison; and
10. the Chromium, Firefox, and WebKit worker study.

Each stage records its command, duration, exit status, and bounded logs. Raw
benchmark JSON, conformance summaries, mismatch entries, and SHA-256 digests
are embedded in the final report. Temporary files and the isolated Python
environment are removed after the report is written.

Expect the full run to take about one hour on a recent desktop, and possibly
longer on older machines. Playwright downloads three browser builds. Keep a
laptop plugged in, disable battery-saving mode, and close heavy applications.
These steps reduce avoidable noise but do not turn the machine into a
controlled laboratory environment.

## Required software

Install these tools before cloning:

- Git;
- Bun;
- Python 3.11 or newer with `venv` and `pip`; and
- Node.js with npm, used by the package checks.

The report records the exact versions it finds. Bun 1.4.0 and pandas 3.0.5
match the current macOS study. The runner creates a temporary Python virtual
environment and installs the pinned pandas version itself.

## macOS

Open Terminal and run:

```sh
git clone https://github.com/Seyamalam/bun_panda.git
cd bun_panda
./scripts/reproduce-platform.sh --tester "Your name"
```

If the shell says the launcher is not executable, use:

```sh
sh scripts/reproduce-platform.sh --tester "Your name"
```

## Ubuntu Linux

Install the usual command-line prerequisites if they are missing:

```sh
sudo apt-get update
sudo apt-get install -y git curl unzip python3 python3-venv python3-pip nodejs npm
```

Install Bun using its official instructions, then clone the repository and
install the browser system libraries once:

```sh
git clone https://github.com/Seyamalam/bun_panda.git
cd bun_panda
bun install --frozen-lockfile
sudo bunx playwright install-deps chromium firefox webkit
./scripts/reproduce-platform.sh --tester "Your name"
```

The full runner measures the host as it is. It does not invoke the separate
cgroup or Docker protocols and does not label an Ubuntu result as a controlled
memory experiment.

## Windows

Install Git, Bun, Python, and Node.js, then open PowerShell:

```powershell
git clone https://github.com/Seyamalam/bun_panda.git
Set-Location bun_panda
powershell -ExecutionPolicy Bypass -File .\scripts\reproduce-platform.ps1 --tester "Your name"
```

Command Prompt is also supported:

```bat
git clone https://github.com/Seyamalam/bun_panda.git
cd bun_panda
scripts\reproduce-platform.cmd --tester "Your name"
```

## File to return

At the end, the runner prints a path similar to:

```text
bun-panda-reproduction-windows-x64-2026-08-26T12-30-00Z.json
```

Send that JSON file to the author. Do not send `node_modules`, browser caches,
the temporary Python environment, or `paper/data`.

The process exits with a nonzero status if a required stage fails, but the JSON
report is still written and identifies the failed stage. Send the report even
after a failure. It contains no environment-variable dump, hostname, username,
or home-directory path. It does include the optional tester name, hardware and
software versions, commit, initial Git status, command logs, and measurements.

## Quick installation check

Before committing an hour to the full run, a tester may use the `quick`
profile:

```sh
./scripts/reproduce-platform.sh --profile quick --tester "Your name"
```

On Windows, pass the same arguments to the PowerShell or Command Prompt
launcher. Quick mode keeps the correctness checks but reduces benchmark scales,
process counts, warm-ups, iterations, bootstrap resamples, and browser
contexts. Its timing results are diagnostic only and must not enter the paper.

Use `--skip-browser` only when browser installation is impossible. The report
will mark the browser study as skipped, so it is not a complete reproduction.

To choose the report path explicitly:

```sh
./scripts/reproduce-platform.sh --output ../my-machine-report.json --tester "Your name"
```
