/** Distinct missing markers for nullable and datetime data. */
export const NA = Symbol.for("bun_panda.NA");
export const NaT = Symbol.for("bun_panda.NaT");
export type MissingScalar = typeof NA | typeof NaT | null | undefined;

/** Includes floating NaN without narrowing all numbers to a missing subtype. */
export function isMissingScalar(value: unknown): boolean {
  return value == null || value === NA || value === NaT ||
    (typeof value === "number" && Number.isNaN(value)) ||
    (value instanceof Date && Number.isNaN(value.getTime()));
}
