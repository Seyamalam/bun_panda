declare module "parquetjs-lite" {
  export class ParquetSchema {
    constructor(schema: Record<string, unknown>);
  }

  export class ParquetWriter {
    static openFile(schema: ParquetSchema, path: string): Promise<ParquetWriter>;
    appendRow(row: Record<string, unknown>): Promise<void>;
    close(): Promise<void>;
  }

  export class ParquetReader {
    static openFile(path: string, options?: { rawStatistics?: boolean }): Promise<ParquetReader>;
    getCursor(): { next(): Promise<Record<string, unknown> | null> };
    close(): Promise<void>;
  }
}

declare module "parquetjs-lite/lib/types.js" {
  export const PARQUET_LOGICAL_TYPES: Record<
    string,
    { toPrimitive(value: unknown): unknown }
  >;
}
