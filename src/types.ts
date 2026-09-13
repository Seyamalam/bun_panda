import type { MissingScalar } from "./missing";

export type IndexLabel = string | number;

export type CellValue = string | number | boolean | MissingScalar | Date;

export type Row = Record<string, CellValue>;

export type AggName =
  | "sum"
  | "mean"
  | "min"
  | "max"
  | "count"
  | "median"
  | "std"
  | "var"
  | "first"
  | "last"
  | "nunique";

export type AggFn = (values: CellValue[], rows: Row[]) => CellValue;

export type AggSpec = Record<string, AggName | AggFn>;

export type IntegerDType = `${"int" | "uint" | "Int" | "UInt"}${8 | 16 | 32 | 64}`;
export type DType = "number" | "string" | "boolean" | "date" | "bool" | "object"
  | "float32" | "float64" | "Float32" | "Float64" | IntegerDType;

export type InferredDType = DType | "mixed" | "unknown";
