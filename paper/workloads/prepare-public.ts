import { mkdirSync } from "node:fs";

const outputDirectory = "paper/data/workloads/uci-bank";
const sourcePath = `${outputDirectory}/bank-marketing.zip`;
const nestedPath = `${outputDirectory}/bank.zip`;
const csvPath = `${outputDirectory}/bank-full.csv`;
const sourceUrl = "https://archive.ics.uci.edu/static/public/222/bank+marketing.zip";

function digest(bytes: ArrayBuffer | Uint8Array): string {
  return new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
}

function extract(archive: string, member: string): Uint8Array {
  const child = Bun.spawnSync(["unzip", "-p", archive, member], { stdout: "pipe", stderr: "pipe" });
  if (child.exitCode !== 0) throw new Error(`could not extract ${member}: ${child.stderr.toString()}`);
  return child.stdout;
}

mkdirSync(outputDirectory, { recursive: true });
const response = await fetch(sourceUrl);
if (!response.ok) throw new Error(`UCI download failed with HTTP ${response.status}`);
const sourceBytes = await response.arrayBuffer();
const sourceDigest = digest(sourceBytes);
if (sourceDigest !== "e0bf5f5de5b846e2f18e9d90606637267d46dfa260e0f17bb12e605db5efbeb4") {
  throw new Error(`UCI source checksum changed: ${sourceDigest}`);
}
await Bun.write(sourcePath, sourceBytes);

const nestedBytes = extract(sourcePath, "bank.zip");
const nestedDigest = digest(nestedBytes);
if (nestedDigest !== "99d7e8eb12401ed278b793984423915411ea8df099e1795f9fefe254f513fe5e") {
  throw new Error(`nested archive checksum changed: ${nestedDigest}`);
}
await Bun.write(nestedPath, nestedBytes);

const csvBytes = extract(nestedPath, "bank-full.csv");
const csvDigest = digest(csvBytes);
if (csvDigest !== "d1513ec63b385506f7cfce9f2c5caa9fe99e7ba4e8c3fa264b3aaf0f849ed32d") {
  throw new Error(`CSV checksum changed: ${csvDigest}`);
}
await Bun.write(csvPath, csvBytes);
console.log(`prepared ${csvPath} (${csvBytes.byteLength} bytes, sha256 ${csvDigest})`);
