import { DataFrame } from "../../dataframe";

export function applyIndexColumn(
  frame: DataFrame,
  indexCol?: string | number
): DataFrame {
  if (indexCol === undefined) {
    return frame;
  }

  const indexColumn =
    typeof indexCol === "number"
      ? frame.columns[indexCol]
      : indexCol;

  if (!indexColumn || !frame.columns.includes(indexColumn)) {
    throw new Error("index_col does not match any column.");
  }

  return frame.set_index(indexColumn);
}
