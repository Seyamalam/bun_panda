import { readFileSync } from "node:fs";

const THRESHOLD = 0.7;
const lcovPath = "coverage/lcov.info";
const lcov = readFileSync(lcovPath, "utf8");

let linesFound = 0;
let linesHit = 0;
let functionsFound = 0;
let functionsHit = 0;

for (const line of lcov.split("\n")) {
  if (line.startsWith("LF:")) linesFound += Number(line.slice(3));
  else if (line.startsWith("LH:")) linesHit += Number(line.slice(3));
  else if (line.startsWith("FNF:")) functionsFound += Number(line.slice(4));
  else if (line.startsWith("FNH:")) functionsHit += Number(line.slice(4));
}

if (linesFound === 0 || functionsFound === 0) {
  throw new Error(`No aggregate line/function totals found in ${lcovPath}.`);
}

const lineRatio = linesHit / linesFound;
const functionRatio = functionsHit / functionsFound;
console.log(
  `aggregate coverage: lines=${(lineRatio * 100).toFixed(2)}% ` +
    `(required ${(THRESHOLD * 100).toFixed(0)}%); ` +
    `functions=${(functionRatio * 100).toFixed(2)}% (reported, not gated)`
);

if (lineRatio < THRESHOLD) {
  process.exit(1);
}
