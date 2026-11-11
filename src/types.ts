import type { MissingScalar } from "./missing";

/** A scalar index label or a MultiIndex tuple (one scalar per level). */
export type IndexLabel = string | number | readonly (string | number)[];

export type CellValue = string | number | bigint | boolean | MissingScalar | Date;

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
/** pandas datetime spellings: `datetime64`, `Datetime64`, `datetime64[ns]`, `datetime64[ns, <zone>]`. */
export type DatetimeDType = "datetime64" | "Datetime64" | `datetime64[${string}]`;
/** pandas `category` / `Categorical` dtype spellings (both normalize to `category`). */
export type CategoricalDType = "category" | "Categorical";
export type DType = "number" | "string" | "str" | "boolean" | "date" | "bool" | "object"
  | "float32" | "float64" | "Float32" | "Float64" | IntegerDType | DatetimeDType | CategoricalDType;

export type InferredDType = DType | "mixed" | "unknown";
