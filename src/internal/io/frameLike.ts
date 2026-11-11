import type { IndexLabel, InferredDType, Row } from "../../types";

export interface FrameLike {
  columns: IndexLabel[];
  /** Storage keys backing `columns` — used for record lookups. */
  storageColumns: string[];
  index: IndexLabel[];
  to_records(): Row[];
  readonly dtypes: Record<string, InferredDType>;
}
