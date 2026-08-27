export interface DispatchCalibration {
  version: string;
  fullSortMinRows: number;
  topKMaxRows: number;
  filterWasmEnabled: boolean;
  groupByWasmEnabledWithoutReuse: boolean;
  groupByWasmEnabledWithReuse: boolean;
  groupByMinRowsWithReuse: number;
}

/**
 * Static policy derived from the 2026-08-26 ablation.
 *
 * The benchmark artifact may replace these values after a fresh-process
 * calibration. Keeping the values in one record makes the active policy
 * inspectable and versioned.
 */
export const DEFAULT_DISPATCH_CALIBRATION: DispatchCalibration = Object.freeze({
  version: "fresh-process-m5-pro-2026-08-26-v2",
  fullSortMinRows: 10_000,
  topKMaxRows: 10_000,
  filterWasmEnabled: false,
  groupByWasmEnabledWithoutReuse: false,
  groupByWasmEnabledWithReuse: true,
  groupByMinRowsWithReuse: 10_000,
});
