export type IndexLabel = string | number;

export type CellValue = string | number | boolean | null | undefined | Date;

export type Row = Record<string, CellValue>;

export type AggName = "sum" | "mean" | "min" | "max" | "count";

export type AggFn = (values: CellValue[], rows: Row[]) => CellValue;

export type AggSpec = Record<string, AggName | AggFn>;
