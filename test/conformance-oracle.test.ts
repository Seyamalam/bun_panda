import { describe, expect, test } from "bun:test";
import { mergeRowsEquivalent } from "../paper/artifact/conformance/equivalence";

const cellEqual = (left: unknown, right: unknown): boolean => Object.is(left, right);
const rowEqual = (left: unknown[], right: unknown[]): boolean =>
  left.length === right.length && left.every((cell, index) => cellEqual(cell, right[index]));

describe("merge conformance ordering", () => {
  test("allows duplicate match permutations within one unchanged key group", () => {
    expect(mergeRowsEquivalent(
      [["A", 5.45], ["A", 3.76]],
      [["A", 3.76], ["A", 5.45]],
      [0],
      cellEqual,
      rowEqual,
    )).toBe(true);
  });

  test("rejects a changed join-key sequence", () => {
    expect(mergeRowsEquivalent(
      [["A", 1], ["B", 2]],
      [["B", 2], ["A", 1]],
      [0],
      cellEqual,
      rowEqual,
    )).toBe(false);
  });

  test("rejects changed values within a key group", () => {
    expect(mergeRowsEquivalent(
      [["A", 1], ["A", 2]],
      [["A", 1], ["A", 3]],
      [0],
      cellEqual,
      rowEqual,
    )).toBe(false);
  });
});
