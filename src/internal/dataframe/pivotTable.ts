import { runAggregation, safeMarginsColumnName, sortRowsByColumns, uniqueColumnValues } from "./core";
import { normalizeKeyCell } from "./keys";
import type { AggFn, AggName, CellValue, Row } from "../../types";
import { isMissing } from "../../utils";

export interface ComputePivotTableOptions {
  sourceRows: Row[];
  index: string[];
  values: string[];
  columns?: string;
  aggfunc: AggName | AggFn;
  fillValue?: CellValue;
  margins: boolean;
  marginsName: string;
  dropna: boolean;
  sort: boolean;
}

export interface PivotTableResult {
  rows: Row[];
  columns: string[];
}

export function computePivotTable(options: ComputePivotTableOptions): PivotTableResult {
  const {
    sourceRows,
    index,
    values,
    columns,
    aggfunc,
    fillValue,
    margins,
    marginsName,
    dropna,
    sort,
  } = options;

  const filteredRows = sourceRows.filter((row) => {
    if (!dropna) {
      return true;
    }
    const requiredColumns = [...index, ...values, ...(columns ? [columns] : [])];
    return requiredColumns.every((column) => !isMissing(row[column]));
  });

  if (!columns) {
    const grouped = aggregateRows(index, values, aggfunc, filteredRows);
    const sortedGrouped = sort ? sortRowsByColumns(grouped, index) : grouped;

    if (margins) {
      const totalRow: Row = {};
      for (let i = 0; i < index.length; i += 1) {
        totalRow[index[i]!] = i === 0 ? marginsName : "";
      }
      for (const valueColumn of values) {
        const valueSeries = filteredRows.map((row) => row[valueColumn]);
        totalRow[valueColumn] = runAggregation(valueSeries, filteredRows, aggfunc);
      }
      sortedGrouped.push(totalRow);
    }

    if (fillValue !== undefined) {
      for (const row of sortedGrouped) {
        for (const valueColumn of values) {
          if (row[valueColumn] === undefined) {
            row[valueColumn] = fillValue;
          }
        }
      }
    }

    return {
      rows: sortedGrouped,
      columns: [...index, ...values],
    };
  }

  const grouped = aggregateRows([...index, columns], values, aggfunc, filteredRows);
  const pivotColumnValues = uniqueColumnValues(filteredRows.map((row) => row[columns]), {
    sort,
    includeMissing: !dropna,
  });

  const valueColumnsOut =
    values.length === 1
      ? pivotColumnValues.map((value) => String(value))
      : values.flatMap((valueColumn) =>
          pivotColumnValues.map((value) => `${valueColumn}_${String(value)}`)
        );

  const marginsColumnsOut = margins
    ? values.length === 1
      ? [safeMarginsColumnName(marginsName, valueColumnsOut)]
      : values.map((valueColumn) =>
          safeMarginsColumnName(`${valueColumn}_${marginsName}`, valueColumnsOut)
        )
    : [];

  const tableRows = new Map<string, Row>();
  const orderedKeys: string[] = [];

  for (const row of grouped) {
    const indexValues = index.map((column) => row[column]);
    const tableKey = JSON.stringify(indexValues.map((value) => normalizeKeyCell(value)));
    const columnValue = row[columns];

    let tableRow = tableRows.get(tableKey);
    if (!tableRow) {
      tableRow = {};
      for (let i = 0; i < index.length; i += 1) {
        tableRow[index[i]!] = indexValues[i];
      }
      tableRows.set(tableKey, tableRow);
      orderedKeys.push(tableKey);
    }

    for (const valueColumn of values) {
      const outputColumn = values.length === 1 ? String(columnValue) : `${valueColumn}_${String(columnValue)}`;
      tableRow[outputColumn] = row[valueColumn];
    }
  }

  const outputRows = orderedKeys.map((key) => {
    const row = tableRows.get(key)!;

    if (margins) {
      for (let valueIndex = 0; valueIndex < values.length; valueIndex += 1) {
        const valueColumn = values[valueIndex]!;
        const matchingRows = filteredRows.filter((sourceRow) =>
          index.every((indexColumn) => sourceRow[indexColumn] === row[indexColumn])
        );
        const sourceValues = matchingRows.map((sourceRow) => sourceRow[valueColumn]);
        row[marginsColumnsOut[valueIndex]!] = runAggregation(sourceValues, matchingRows, aggfunc);
      }
    }

    for (const valueColumn of valueColumnsOut) {
      if (row[valueColumn] === undefined && fillValue !== undefined) {
        row[valueColumn] = fillValue;
      }
    }

    for (const marginsColumn of marginsColumnsOut) {
      if (row[marginsColumn] === undefined && fillValue !== undefined) {
        row[marginsColumn] = fillValue;
      }
    }

    return row;
  });

  if (margins) {
    const totalRow: Row = {};
    for (let i = 0; i < index.length; i += 1) {
      totalRow[index[i]!] = i === 0 ? marginsName : "";
    }

    for (const pivotColumn of pivotColumnValues) {
      const pivotKey = normalizeKeyCell(pivotColumn);
      for (let valueIndex = 0; valueIndex < values.length; valueIndex += 1) {
        const valueColumn = values[valueIndex]!;
        const outputColumn =
          values.length === 1 ? String(pivotColumn) : `${valueColumn}_${String(pivotColumn)}`;

        const matchingRows = filteredRows.filter(
          (sourceRow) => normalizeKeyCell(sourceRow[columns]) === pivotKey
        );
        const sourceValues = matchingRows.map((sourceRow) => sourceRow[valueColumn]);
        totalRow[outputColumn] = runAggregation(sourceValues, matchingRows, aggfunc);
      }
    }

    for (let valueIndex = 0; valueIndex < values.length; valueIndex += 1) {
      const valueColumn = values[valueIndex]!;
      const sourceValues = filteredRows.map((row) => row[valueColumn]);
      totalRow[marginsColumnsOut[valueIndex]!] = runAggregation(sourceValues, filteredRows, aggfunc);
    }

    outputRows.push(totalRow);
  }

  const rows = sort ? sortRowsByColumns(outputRows, index, margins ? marginsName : undefined) : outputRows;

  return {
    rows,
    columns: [...index, ...valueColumnsOut, ...marginsColumnsOut],
  };
}

function aggregateRows(
  groupColumns: string[],
  valueColumns: string[],
  aggfunc: AggName | AggFn,
  sourceRows: Row[]
): Row[] {
  const groups = new Map<string, { groupValues: CellValue[]; rows: Row[] }>();

  for (const sourceRow of sourceRows) {
    const groupValues = groupColumns.map((column) => sourceRow[column]);
    const key = JSON.stringify(groupValues.map((value) => normalizeKeyCell(value)));
    const group = groups.get(key);
    if (!group) {
      groups.set(key, { groupValues, rows: [sourceRow] });
    } else {
      group.rows.push(sourceRow);
    }
  }

  const rows: Row[] = [];
  for (const group of groups.values()) {
    const row: Row = {};
    for (let i = 0; i < groupColumns.length; i += 1) {
      row[groupColumns[i]!] = group.groupValues[i];
    }

    for (const valueColumn of valueColumns) {
      const values = group.rows.map((entry) => entry[valueColumn]);
      row[valueColumn] = runAggregation(values, group.rows, aggfunc);
    }
    rows.push(row);
  }

  return rows;
}
