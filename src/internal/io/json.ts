import { DataFrame } from "../../dataframe";
import type { CellValue, Row } from "../../types";
import type { ReadJSONOptions } from "../../io";
import { applyIndexColumn } from "./frame";
import { coerceJsonCell, stripBom } from "./shared";

export function parseJsonText(text: string, options: ReadJSONOptions = {}): DataFrame {
  if (options.lines) {
    return parseJsonLines(text, options);
  }

  const parsed = JSON.parse(stripBom(text)) as unknown;
  const orient = options.orient ?? inferJsonOrient(parsed);
  let frame: DataFrame;

  if (orient === "records") {
    if (!Array.isArray(parsed)) {
      throw new Error("JSON orient 'records' expects an array of objects.");
    }
    frame = dataframeFromRecords(parsed);
  } else {
    frame = dataframeFromColumnar(parsed);
  }

  return applyIndexColumn(frame, options.index_col);
}

function parseJsonLines(text: string, options: ReadJSONOptions): DataFrame {
  if (options.orient && options.orient !== "records") {
    throw new Error("JSON lines format only supports orient='records'.");
  }

  const records = stripBom(text)
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line, position) => {
      const parsed = JSON.parse(line) as unknown;
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new Error(`JSON lines entry at line ${position + 1} is not an object.`);
      }
      const row: Row = {};
      for (const [column, value] of Object.entries(parsed)) {
        row[column] = coerceJsonCell(value);
      }
      return row;
    });

  const frame = new DataFrame(records);
  return applyIndexColumn(frame, options.index_col);
}

function dataframeFromRecords(parsed: unknown): DataFrame {
  if (!Array.isArray(parsed)) {
    throw new Error("JSON orient 'records' expects an array of objects.");
  }
  const records: Row[] = parsed.map((entry, position) => {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      throw new Error(`JSON records entry at position ${position} is not an object.`);
    }
    const row: Row = {};
    for (const [column, value] of Object.entries(entry)) {
      row[column] = coerceJsonCell(value);
    }
    return row;
  });
  return new DataFrame(records);
}

function dataframeFromColumnar(parsed: unknown): DataFrame {
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("JSON orient 'list' expects a column-object with array values.");
  }
  const columnar: Record<string, CellValue[]> = {};
  for (const [column, values] of Object.entries(parsed as Record<string, unknown>)) {
    if (!Array.isArray(values)) {
      throw new Error(`JSON column '${column}' is not an array.`);
    }
    columnar[column] = values.map((value) => coerceJsonCell(value));
  }
  return new DataFrame(columnar);
}

function inferJsonOrient(input: unknown): "records" | "list" {
  if (Array.isArray(input)) {
    return "records";
  }
  if (input && typeof input === "object") {
    return "list";
  }
  throw new Error("Unable to infer JSON orient; expected array or object root.");
}
