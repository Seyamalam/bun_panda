import { DataFrame } from "../../dataframe";
import type { Row } from "../../types";
import type { ReadParquetOptions } from "../../io";
import { applyIndexColumn } from "./frame";
import { buildTypedColumns, typedColumnsToRecords } from "./parquetTyped";

interface ShreddedRowBuffer {
  rowCount: number;
  columnData: Record<string, unknown>;
}

export async function readParquetFile(
  path: string,
  options: ReadParquetOptions = {}
): Promise<DataFrame> {
  const parquet = await import("parquetjs-lite");
  const reader = (await parquet.ParquetReader.openFile(path)) as unknown as {
    metadata: { row_groups: unknown[] };
    envelopeReader: {
      readRowGroup(
        schema: never,
        rowGroup: never,
        columnList?: string[]
      ): Promise<ShreddedRowBuffer>;
    };
    schema: never;
    close(): Promise<void>;
  };

  try {
    const allRecords: Row[] = [];

    // Row-group-at-a-time via the envelope reader: each group arrives
    // as shredded per-column buffers. Numeric columns are decoded
    // straight into Float64Arrays (NaN = missing) and converted to
    // rows once at the boundary — this skips the per-cell
    // materializeRecords dispatch that dominates read time.
    const groups = reader.metadata.row_groups ?? [];
    for (const group of groups) {
      const buffer = await reader.envelopeReader.readRowGroup(
        reader.schema,
        group as never,
        []
      );
      const rowCount = buffer.rowCount ?? 0;
      if (!buffer.columnData || rowCount === 0) {
        continue;
      }
      const typedColumns = buildTypedColumns(
        reader.schema,
        buffer.columnData as never,
        rowCount
      );
      for (const record of typedColumnsToRecords(typedColumns, rowCount)) {
        allRecords.push(record);
      }
    }

    let frame = new DataFrame(allRecords);
    if (options.columns && options.columns.length > 0) {
      frame = frame.select(options.columns);
    }

    return applyIndexColumn(frame, options.index_col);
  } finally {
    await reader.close();
  }
}
