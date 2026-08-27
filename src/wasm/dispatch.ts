import {
  DEFAULT_DISPATCH_CALIBRATION,
  type DispatchCalibration,
} from "./dispatch-calibration";

export type ExecutionPath = "typescript" | "wasm";

export type DispatchReason =
  | "forced-typescript"
  | "forced-wasm"
  | "below-measured-sort-range"
  | "measured-full-sort-win"
  | "bounded-selection-preferred"
  | "measured-top-k-win"
  | "measured-filter-regression"
  | "filter-enabled-by-calibration"
  | "groupby-dropna-unsupported"
  | "groupby-awaiting-reuse-evidence"
  | "groupby-enabled-by-calibration";

export type DispatchRequest =
  | {
      operation: "sort";
      rowCount: number;
      limit?: number;
    }
  | {
      operation: "filter-mask";
      rowCount: number;
    }
  | {
      operation: "groupby-fused";
      rowCount: number;
      planCount: number;
      dropna: boolean;
      typedColumnsReused: boolean;
    };

export interface DispatchDecision {
  path: ExecutionPath;
  reason: DispatchReason;
  calibrationVersion: string;
}

function configuredMode(): "adaptive" | ExecutionPath {
  const root = globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  };
  const value = root.process?.env?.BUN_PANDA_WASM;
  if (value === "0") return "typescript";
  if (value === "1") return "wasm";
  return "adaptive";
}

function decideSort(
  request: Extract<DispatchRequest, { operation: "sort" }>,
  calibration: DispatchCalibration
): DispatchDecision {
  if (request.rowCount < calibration.fullSortMinRows) {
    return {
      path: "typescript",
      reason: "below-measured-sort-range",
      calibrationVersion: calibration.version,
    };
  }

  if (request.limit === undefined || request.limit >= request.rowCount) {
    return {
      path: "wasm",
      reason: "measured-full-sort-win",
      calibrationVersion: calibration.version,
    };
  }

  if (request.rowCount > calibration.topKMaxRows) {
    return {
      path: "typescript",
      reason: "bounded-selection-preferred",
      calibrationVersion: calibration.version,
    };
  }

  return {
    path: "wasm",
    reason: "measured-top-k-win",
    calibrationVersion: calibration.version,
  };
}

function decideFilter(calibration: DispatchCalibration): DispatchDecision {
  return calibration.filterWasmEnabled
    ? {
        path: "wasm",
        reason: "filter-enabled-by-calibration",
        calibrationVersion: calibration.version,
      }
    : {
        path: "typescript",
        reason: "measured-filter-regression",
        calibrationVersion: calibration.version,
      };
}

function decideGroupBy(
  request: Extract<DispatchRequest, { operation: "groupby-fused" }>,
  calibration: DispatchCalibration
): DispatchDecision {
  if (!request.dropna) {
    return {
      path: "typescript",
      reason: "groupby-dropna-unsupported",
      calibrationVersion: calibration.version,
    };
  }

  const enabled = request.typedColumnsReused
    ? calibration.groupByWasmEnabledWithReuse &&
      request.rowCount >= calibration.groupByMinRowsWithReuse
    : calibration.groupByWasmEnabledWithoutReuse;

  return enabled
    ? {
        path: "wasm",
        reason: "groupby-enabled-by-calibration",
        calibrationVersion: calibration.version,
      }
    : {
        path: "typescript",
        reason: "groupby-awaiting-reuse-evidence",
        calibrationVersion: calibration.version,
      };
}

/**
 * Chooses an execution path from operation facts and versioned calibration.
 * Callers still retain a correctness-preserving TypeScript fallback when a
 * selected Wasm kernel cannot load or rejects the call shape.
 */
export function chooseExecutionPath(
  request: DispatchRequest,
  calibration: DispatchCalibration = DEFAULT_DISPATCH_CALIBRATION
): DispatchDecision {
  const mode = configuredMode();
  if (mode === "typescript") {
    return {
      path: "typescript",
      reason: "forced-typescript",
      calibrationVersion: calibration.version,
    };
  }
  // Forced Wasm is still restricted to semantically supported call shapes.
  // The current grouping kernel always drops missing keys, so routing a
  // dropna=false request through it would silently remove a group.
  if (request.operation === "groupby-fused" && !request.dropna) {
    return {
      path: "typescript",
      reason: "groupby-dropna-unsupported",
      calibrationVersion: calibration.version,
    };
  }
  if (mode === "wasm") {
    return {
      path: "wasm",
      reason: "forced-wasm",
      calibrationVersion: calibration.version,
    };
  }

  if (request.operation === "sort") return decideSort(request, calibration);
  if (request.operation === "filter-mask") return decideFilter(calibration);
  return decideGroupBy(request, calibration);
}
