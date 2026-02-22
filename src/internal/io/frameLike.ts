import type { IndexLabel, InferredDType, Row } from "../../types";

export interface FrameLike {
  columns: string[];
  index: IndexLabel[];
  to_records(): Row[];
  dtypes(): Record<string, InferredDType>;
}
