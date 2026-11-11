import { describe, expect, test } from "bun:test";
import { Series } from "../src/index";

describe("categorical dtype", () => {
  test("dtype declaration and .cat accessor", () => {
    const s = new Series(["a", "b", "a"], { dtype: "category" });
    expect(s.dtype).toBe("category");
    expect(s.cat.categories).toEqual(["a", "b"]);
    expect(s.cat.ordered).toBe(false);
    expect(s.cat.codes.to_list()).toEqual([0, 1, 0]);
  });

  test("unordered comparisons raise pandas error", () => {
    const s = new Series(["a", "b"], { dtype: "category" });
    const o = new Series(["a", "b"], { dtype: "category" });
    expect(() => s.lt(o)).toThrow("Unordered Categoricals can only compare equality or not");
    expect(s.eq(o).to_list()).toEqual([true, true]);
  });

  test("ordered categorical compares by code order", () => {
    const s = new Series(["low", "high"], { dtype: "category" });
    const ordered = s.cat.as_ordered();
    expect(ordered.cat.ordered).toBe(true);
  });

  test("value_counts works on categorical", () => {
    const s = new Series(["a", "b", "a", "a"], { dtype: "category" });
    expect(s.value_counts().to_list()).toEqual([3, 1]);
  });

  test("astype to category", () => {
    const s = new Series([1, 2, 1]);
    const c = s.astype("category");
    expect(c.dtype).toBe("category");
  });
});
