import type { CellValue, DType, IndexLabel, Row } from "../../types";
import { isMissing, pythonOperandTypeName } from "../../utils";
import { scalarEquals, scalarKey } from "../shared/valueSemantics";
import {
  AttributeError,
  NotImplementedError,
  UndefinedVariableError,
  ValueError,
} from "../../errors";

/**
 * Safe recursive-descent parser for pandas `DataFrame.query()` /
 * `DataFrame.eval()` expressions. No `eval()` / `new Function()` —
 * expressions are tokenized and compiled into a small AST evaluated per row.
 *
 * Supported grammar (a pandas-compatible subset):
 *
 *   - literals: numbers, 'strings', "strings", True, False, None
 *   - column-name identifiers, `backtick-quoted` names, `index` and
 *     named index levels, @external variables (local_dict/resolvers)
 *   - arithmetic: +  -  *  /  //  %  ** and unary +/-
 *   - comparisons: ==  !=  <  <=  >  >=  (chained: `1 < a < 5`)
 *   - membership: `in` / `not in` over lists `[1, 2]`, tuples `(1, 2)`,
 *     @lists, and columns (pandas `x.isin(y)` semantics)
 *   - boolean ops: `and`/`&`, `or`/`|`, `not`/`~` with pandas precedence
 *     (`&`/`|` bind like `and`/`or`, below comparisons — unlike Python)
 *   - method calls: `.isna()/.isnull()/.notna()/.notnull()/.isin([..])`
 *     and `.str.contains()/.str.startswith()/.str.endswith()`
 *   - `col = expr` top-level assignment for eval()
 *
 * pandas-faithful errors: UndefinedVariableError for unknown names,
 * SyntaxError for malformed input, TypeError for bad operands,
 * NotImplementedError for `is`/`if`/scalar-only bool ops, and
 * AttributeError for `>>`, `<<`, `^` (nodes the pandas visitor lacks).
 */

type ExprValue = CellValue | CellValue[];

/** Per-row evaluation context supplied by the caller (one per frame pass). */
interface ExprEvalCtx {
  /** 0-based position of the row being evaluated. */
  position: number;
  /** scalarKey sets for `col in col` membership, keyed by column name. */
  columnValueSets?: Map<string, Set<string>>;
}

interface ExprNode {
  eval(row: Row, ctx: ExprEvalCtx): ExprValue;
  /**
   * True when the subtree evaluates identically for every row — the pandas
   * `Term.is_scalar` notion used by the scalar-only-bool-op check and by
   * scalar `in` (`2 in qty` → membership on the *index*).
   */
  isScalar: boolean;
  /** Literal `None` operand: `x == None` is False for every cell in pandas. */
  isNone?: boolean;
  /** Set when the node is a bare column reference. */
  columnName?: string;
  /** Statically boolean-producing node (literal bool, comparison, bool op). */
  isBool?: boolean;
}

/** Resolution scope handed to the compiler by DataFrame.eval/query. */
export interface FrameExprScope {
  /** Valid column names — when provided, unknown identifiers raise. */
  columns?: string[];
  /** Index labels (for the `index` term and scalar `x in column` checks). */
  indexLabels?: IndexLabel[];
  /** Index level names resolvable as expression identifiers. */
  indexNames?: (string | undefined)[];
  /** Column dtypes, used for pandas-faithful type checks and error names. */
  dtypes?: Record<string, DType | undefined>;
  /** @name lookup — pandas local_dict. */
  localDict?: Record<string, unknown>;
  /** @name lookup — pandas global_dict. */
  globalDict?: Record<string, unknown>;
  /** @name lookup — pandas resolvers list. */
  resolvers?: Record<string, unknown>[];
  /**
   * eval() propagates missing through comparisons (library contract);
   * query() uses pandas mask semantics (NaN comparisons → False, != → True).
   */
  mode?: "eval" | "query";
}

/** Public per-row context for `CompiledFrameExpr.evaluate`. */
export interface FrameExprEvalContext {
  position?: number;
  columnValueSets?: Map<string, Set<string>>;
}

export interface CompiledFrameExpr {
  /** Evaluates the expression for one row; null where operands are missing. */
  evaluate(row: Row, ctx?: FrameExprEvalContext): CellValue;
  /** Column names referenced by the expression, in order of appearance. */
  referencedColumns: string[];
  /** True when the expression is a constant (no column/index references). */
  isScalar: boolean;
  /** `name = expr` assignment target when the expression is an assignment. */
  assignmentTarget?: string;
  /** Columns whose full value-set is needed (`lhs in rhs_column`). */
  columnSetRefs: string[];
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

type TokenType = "number" | "string" | "identifier" | "backtick" | "at" | "operator";

interface Token {
  type: TokenType;
  value: string;
}

const TWO_CHAR_OPERATORS = ["**", "//", ">=", "<=", "==", "!=", ">>", "<<"];
const ONE_CHAR_OPERATORS = "+-*/%><()[]{},.&|^~=".split("");

const STRING_ESCAPES: Record<string, string> = {
  n: "\n",
  t: "\t",
  r: "\r",
  b: "\b",
  f: "\f",
  v: "\v",
  "0": "\0",
  "\\": "\\",
  "'": "'",
  '"': '"',
};

function syntaxError(message = "invalid syntax"): SyntaxError {
  return new SyntaxError(`${message} (<unknown>, line 1)`);
}

function tokenize(expr: string): Token[] {
  const tokens: Token[] = [];
  let i = 0;
  while (i < expr.length) {
    const char = expr[i]!;
    if (char === " " || char === "\t" || char === "\n" || char === "\r") {
      i += 1;
      continue;
    }
    if (char === "'" || char === '"') {
      const quote = char;
      let j = i + 1;
      let value = "";
      let closed = false;
      while (j < expr.length) {
        const c = expr[j]!;
        if (c === "\\") {
          const next = expr[j + 1];
          if (next === undefined) break;
          if (next in STRING_ESCAPES) {
            value += STRING_ESCAPES[next];
          } else {
            value += `\\${next}`;
          }
          j += 2;
          continue;
        }
        if (c === quote) {
          closed = true;
          j += 1;
          break;
        }
        value += c;
        j += 1;
      }
      if (!closed) {
        throw syntaxError("unterminated string literal");
      }
      tokens.push({ type: "string", value });
      i = j;
      continue;
    }
    if (char === "`") {
      const end = expr.indexOf("`", i + 1);
      if (end === -1) {
        throw syntaxError("unterminated backtick-quoted name");
      }
      const name = expr.slice(i + 1, end);
      if (name.length === 0) {
        throw syntaxError("empty backtick-quoted name");
      }
      tokens.push({ type: "backtick", value: name });
      i = end + 1;
      continue;
    }
    if (char === "@") {
      const next = expr[i + 1] ?? "";
      if (!/[A-Za-z_]/.test(next)) {
        throw syntaxError("invalid syntax");
      }
      let j = i + 1;
      while (j < expr.length && /[A-Za-z0-9_]/.test(expr[j]!)) {
        j += 1;
      }
      tokens.push({ type: "at", value: expr.slice(i + 1, j) });
      i = j;
      continue;
    }
    const twoChar = expr.slice(i, i + 2);
    if (TWO_CHAR_OPERATORS.includes(twoChar)) {
      tokens.push({ type: "operator", value: twoChar });
      i += 2;
      continue;
    }
    if (char === "!" && expr[i + 1] !== "=") {
      // Python has no bare `!`; pandas raises SyntaxError too.
      throw syntaxError("invalid syntax");
    }
    if (ONE_CHAR_OPERATORS.includes(char)) {
      tokens.push({ type: "operator", value: char });
      i += 1;
      continue;
    }
    if (/[0-9]/.test(char) || (char === "." && /[0-9]/.test(expr[i + 1] ?? ""))) {
      const rest = expr.slice(i);
      const match = /^[0-9]*\.?[0-9]+(?:[eE][+-]?[0-9]+)?/.exec(rest);
      const text = match?.[0] ?? char;
      tokens.push({ type: "number", value: text });
      i += text.length;
      continue;
    }
    if (/[A-Za-z_]/.test(char)) {
      let j = i;
      while (j < expr.length && /[A-Za-z0-9_]/.test(expr[j]!)) {
        j += 1;
      }
      tokens.push({ type: "identifier", value: expr.slice(i, j) });
      i = j;
      continue;
    }
    throw syntaxError(`invalid character '${char}'`);
  }
  return tokens;
}

// ---------------------------------------------------------------------------
// Runtime helpers
// ---------------------------------------------------------------------------

function truthy(value: ExprValue): boolean {
  if (Array.isArray(value)) {
    return value.length > 0;
  }
  if (isMissing(value)) {
    return false;
  }
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "bigint") {
    return value !== 0n;
  }
  if (typeof value === "string") {
    return value.length > 0;
  }
  return true;
}

function toNumber(value: ExprValue): number | null {
  if (Array.isArray(value) || isMissing(value)) {
    return null;
  }
  if (typeof value === "number") {
    return value;
  }
  // bigint participates approximately: eval() is a float64 expression engine.
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }
  return null;
}

/** Integer-valued operand for pandas `&`/`|`/`~` (ints and bools only). */
function intLike(value: ExprValue): boolean {
  return typeof value === "bigint" ||
    (typeof value === "number" && Number.isInteger(value) && !Number.isNaN(value));
}

/** Membership equality: pandas isin matches NaN to NaN, joins bool/number. */
function memberOf(value: ExprValue, list: ExprValue[]): boolean {
  if (Array.isArray(value)) {
    return list.some((entry) => entry === value);
  }
  const cell = value;
  return list.some((entry) => !Array.isArray(entry) && scalarEquals(cell, entry));
}

function memberOfSet(value: ExprValue, keySet: Set<string>): boolean {
  if (Array.isArray(value)) {
    return false;
  }
  return keySet.has(scalarKey(value));
}

/** pandas dtype-ish name used in arithmetic TypeError messages. */
function arithTypeName(node: ExprNode, value: ExprValue, dtypes?: Record<string, DType | undefined>): string {
  if (node.isScalar) {
    return `<class '${pythonOperandTypeName(scalarForMessage(value))}'>`;
  }
  const dtype = node.columnName ? dtypes?.[node.columnName] : undefined;
  if (dtype !== undefined) {
    if (/^[uU]?[iI]nt/.test(dtype) || dtype === "number") {
      return dtype === "number" ? "float64" : "int64";
    }
    if (/^[fF]loat/.test(dtype)) {
      return "float64";
    }
    if (dtype === "bool" || dtype === "boolean") {
      return "bool";
    }
    if (dtype === "str" || dtype === "string") {
      return "str";
    }
    if (dtype === "date" || /^[dD]atetime/.test(dtype)) {
      return "datetime64[ns]";
    }
    return "object";
  }
  return valueTypeName(value);
}

function scalarForMessage(value: ExprValue): CellValue {
  return Array.isArray(value) ? null : value;
}

function valueTypeName(value: ExprValue): string {
  const cell = scalarForMessage(value);
  if (isMissing(cell)) {
    return "NoneType";
  }
  if (typeof cell === "number") {
    return Number.isInteger(cell) ? "int64" : "float64";
  }
  if (typeof cell === "bigint") {
    return "int64";
  }
  if (typeof cell === "boolean") {
    return "bool";
  }
  if (typeof cell === "string") {
    return "str";
  }
  if (cell instanceof Date) {
    return "datetime64[ns]";
  }
  return "object";
}

function unsupportedOperand(op: string, left: ExprValue, right: ExprValue): TypeError {
  return new TypeError(
    `unsupported operand type(s) for ${op}: '${pythonOperandTypeName(scalarForMessage(left))}' and '${pythonOperandTypeName(scalarForMessage(right))}'`
  );
}

// ---------------------------------------------------------------------------
// Node constructors
// ---------------------------------------------------------------------------

function merge(...flags: boolean[]): boolean {
  return flags.every(Boolean);
}

function constNode(value: ExprValue): ExprNode {
  return {
    eval: () => value,
    isScalar: true,
    isBool: typeof value === "boolean",
  };
}

function columnNode(name: string, scope: FrameExprScope): ExprNode {
  const dtype = scope.dtypes?.[name];
  return {
    eval: (row) => row[name] ?? null,
    isScalar: false,
    columnName: name,
    isBool: dtype === "bool" || dtype === "boolean",
  };
}

function indexNode(scope: FrameExprScope, level?: number): ExprNode {
  const labels = scope.indexLabels;
  return {
    eval: (_row, ctx) => {
      const label = labels?.[ctx.position];
      if (level === undefined || level === 0 || !Array.isArray(label)) {
        return (label ?? null) as CellValue;
      }
      return (label[level] ?? null) as CellValue;
    },
    isScalar: false,
  };
}

function resolveAtName(name: string, scope: FrameExprScope): unknown {
  if (scope.localDict && name in scope.localDict) {
    return scope.localDict[name];
  }
  for (const resolver of scope.resolvers ?? []) {
    if (name in resolver) {
      return resolver[name];
    }
  }
  if (scope.globalDict && name in scope.globalDict) {
    return scope.globalDict[name];
  }
  throw new UndefinedVariableError(`local variable '${name}' is not defined`);
}

function resolveIdentifier(name: string, scope: FrameExprScope, referenced: string[]): ExprNode {
  if (scope.columns === undefined || scope.columns.includes(name)) {
    if (!referenced.includes(name)) {
      referenced.push(name);
    }
    return columnNode(name, scope);
  }
  const levelIndex = scope.indexNames?.indexOf(name) ?? -1;
  if (levelIndex >= 0) {
    return indexNode(scope, levelIndex);
  }
  if (name === "index") {
    return indexNode(scope);
  }
  throw new UndefinedVariableError(`name '${name}' is not defined`);
}

/** Evaluate a constant subtree once (row/context independent). */
function evalScalar(node: ExprNode): ExprValue {
  return node.eval({}, { position: 0 });
}

// --- arithmetic ---

function arithNode(op: string, left: ExprNode, right: ExprNode, scope: FrameExprScope): ExprNode {
  return {
    isScalar: merge(left.isScalar, right.isScalar),
    eval: (row, ctx) => {
      const lv = left.eval(row, ctx);
      const rv = right.eval(row, ctx);
      if (op === "+" && typeof lv === "string" && typeof rv === "string") {
        // pandas concatenates str columns, but rejects str scalars.
        if (!left.isScalar && !right.isScalar) {
          return lv + rv;
        }
        throw new TypeError(
          `unsupported operand type(s) for +: '${arithTypeName(left, lv, scope.dtypes)}' and '${arithTypeName(right, rv, scope.dtypes)}'`
        );
      }
      if (
        (typeof lv === "string" && !isMissing(rv)) ||
        (typeof rv === "string" && !isMissing(lv))
      ) {
        throw new TypeError(
          `unsupported operand type(s) for ${op}: '${arithTypeName(left, lv, scope.dtypes)}' and '${arithTypeName(right, rv, scope.dtypes)}'`
        );
      }
      const a = toNumber(lv);
      const b = toNumber(rv);
      if (a === null || b === null) {
        return null;
      }
      switch (op) {
        case "+": return a + b;
        case "-": return a - b;
        case "*": return a * b;
        case "/": return a / b;
        case "//": return Math.floor(a / b);
        // pandas/numpy remainder takes the divisor's sign.
        case "%": return ((a % b) + b) % b;
        default: throw new Error(`Unsupported operator '${op}'.`);
      }
    },
  };
}

function negateNode(inner: ExprNode): ExprNode {
  return {
    isScalar: inner.isScalar,
    eval: (row, ctx) => {
      const value = inner.eval(row, ctx);
      const num = toNumber(value);
      if (num !== null) {
        return -num;
      }
      if (isMissing(value)) {
        return null;
      }
      throw new TypeError(
        `bad operand type for unary -: '${pythonOperandTypeName(scalarForMessage(value))}'`
      );
    },
  };
}

function powerNode(base: ExprNode, exponent: ExprNode): ExprNode {
  return {
    isScalar: merge(base.isScalar, exponent.isScalar),
    eval: (row, ctx) => {
      const a = toNumber(base.eval(row, ctx));
      const b = toNumber(exponent.eval(row, ctx));
      if (a === null || b === null) {
        return null;
      }
      return a ** b;
    },
  };
}

// --- boolean ops ---

/** pandas `_disallow_scalar_only_bool_ops`: scalar operand requires both sides bool. */
function checkScalarBoolOp(left: ExprNode, right: ExprNode): void {
  if ((left.isScalar || right.isScalar) && !(left.isBool && right.isBool)) {
    throw new NotImplementedError("cannot evaluate scalar only bool ops");
  }
}

function boolOpNode(op: "&" | "|", left: ExprNode, right: ExprNode, skipScalarCheck = false): ExprNode {
  if (!skipScalarCheck) {
    checkScalarBoolOp(left, right);
  }
  return {
    isScalar: merge(left.isScalar, right.isScalar),
    isBool: (left.isBool || right.isBool) === true ? true : undefined,
    eval: (row, ctx) => {
      const lv = left.eval(row, ctx);
      const rv = right.eval(row, ctx);
      const lBool = typeof lv === "boolean";
      const rBool = typeof rv === "boolean";
      if (lBool && rBool) {
        return op === "&" ? lv && rv : lv || rv;
      }
      if (lBool) {
        if (intLike(rv)) {
          const bit = op === "&" ? Number(rv) & Number(lv) : Number(rv) | Number(lv);
          return bit !== 0;
        }
        return op === "&" ? truthy(rv) && lv : truthy(rv) || lv;
      }
      if (rBool) {
        if (intLike(lv)) {
          const bit = op === "&" ? Number(lv) & Number(rv) : Number(lv) | Number(rv);
          return bit !== 0;
        }
        throw unsupportedOperand(op, lv, rv);
      }
      if (intLike(lv) && intLike(rv)) {
        if (typeof lv === "bigint" || typeof rv === "bigint") {
          return op === "&" ? BigInt(lv as bigint | number) & BigInt(rv as bigint | number)
            : BigInt(lv as bigint | number) | BigInt(rv as bigint | number);
        }
        return op === "&" ? (lv as number) & (rv as number) : (lv as number) | (rv as number);
      }
      throw unsupportedOperand(op, lv, rv);
    },
  };
}

/** `not` and `~` share pandas invert semantics (only precedence differs). */
function invertNode(inner: ExprNode): ExprNode {
  return {
    isScalar: inner.isScalar,
    isBool: inner.isBool,
    eval: (row, ctx) => {
      const value = inner.eval(row, ctx);
      if (typeof value === "boolean") {
        return !value;
      }
      if (typeof value === "bigint") {
        return ~value;
      }
      if (typeof value === "number" && Number.isInteger(value)) {
        return ~value;
      }
      throw new TypeError(
        "ufunc 'invert' not supported for the input types, and the inputs could not be " +
        "safely coerced to any supported types according to the casting rule 'safe'"
      );
    },
  };
}

// --- comparisons ---

const INVERT_TYPE_ERROR =
  "'{op}' not supported between instances of '{l}' and '{r}'";

function compareOrder(op: string, left: ExprValue, right: ExprValue): TypeError {
  return new TypeError(
    INVERT_TYPE_ERROR
      .replace("{op}", op)
      .replace("{l}", pythonOperandTypeName(scalarForMessage(left)))
      .replace("{r}", pythonOperandTypeName(scalarForMessage(right)))
  );
}

/**
 * Element comparison with pandas/query semantics for missing values:
 * `!=` is True on missing, every other op is False.
 * In eval() mode a missing operand yields null (library contract).
 */
function compareCells(
  left: ExprValue,
  right: ExprValue,
  op: string,
  mode: "eval" | "query"
): CellValue {
  if (isMissing(left) || isMissing(right) || Array.isArray(left) || Array.isArray(right)) {
    return mode === "query" ? op === "!=" : null;
  }
  let l = left;
  let r = right;
  // Datetime coercion: pandas converts a scalar to Timestamp when the other
  // side is a datetime term.
  if (l instanceof Date || r instanceof Date) {
    if (!(l instanceof Date)) {
      l = coerceToDate(l);
    }
    if (!(r instanceof Date)) {
      r = coerceToDate(r);
    }
    if (!(l instanceof Date) || !(r instanceof Date) ||
        Number.isNaN(l.getTime()) || Number.isNaN(r.getTime())) {
      return mode === "query" ? op === "!=" : null;
    }
    const diff = l.getTime() - r.getTime();
    switch (op) {
      case ">": return diff > 0;
      case ">=": return diff >= 0;
      case "<": return diff < 0;
      case "<=": return diff <= 0;
      case "==": return diff === 0;
      case "!=": return diff !== 0;
      default: throw new Error(`Unsupported operator '${op}'.`);
    }
  }
  const lStr = typeof l === "string";
  const rStr = typeof r === "string";
  if (lStr !== rStr) {
    if (op === "==") return false;
    if (op === "!=") return true;
    throw compareOrder(op, l, r);
  }
  if (lStr && rStr) {
    const a = l as string;
    const b = r as string;
    switch (op) {
      case ">": return a > b;
      case ">=": return a >= b;
      case "<": return a < b;
      case "<=": return a <= b;
      case "==": return a === b;
      case "!=": return a !== b;
      default: throw new Error(`Unsupported operator '${op}'.`);
    }
  }
  const numeric = (v: CellValue) =>
    typeof v === "number" || typeof v === "bigint" || typeof v === "boolean";
  if (!numeric(l) || !numeric(r)) {
    if (op === "==") return scalarEquals(l, r);
    if (op === "!=") return !scalarEquals(l, r);
    throw compareOrder(op, l, r);
  }
  let result: boolean;
  if (typeof l === "bigint" || typeof r === "bigint") {
    const a = typeof l === "bigint" ? l : BigInt(Math.trunc(Number(l)));
    const b = typeof r === "bigint" ? r : BigInt(Math.trunc(Number(r)));
    switch (op) {
      case ">": result = a > b; break;
      case ">=": result = a >= b; break;
      case "<": result = a < b; break;
      case "<=": result = a <= b; break;
      case "==": result = a === b; break;
      case "!=": result = a !== b; break;
      default: throw new Error(`Unsupported operator '${op}'.`);
    }
    return result;
  }
  const a = Number(l);
  const b = Number(r);
  switch (op) {
    case ">": result = a > b; break;
    case ">=": result = a >= b; break;
    case "<": result = a < b; break;
    case "<=": result = a <= b; break;
    case "==": result = a === b; break;
    case "!=": result = a !== b; break;
    default: throw new Error(`Unsupported operator '${op}'.`);
  }
  return result;
}

function coerceToDate(value: CellValue): Date | CellValue {
  if (typeof value === "string") {
    const parsed = new Date(value);
    return parsed;
  }
  return value;
}

function compareNode(
  op: string,
  left: ExprNode,
  right: ExprNode,
  scope: FrameExprScope
): ExprNode {
  return {
    isScalar: merge(left.isScalar, right.isScalar),
    isBool: true,
    eval: (row, ctx) => {
      const lv = left.eval(row, ctx);
      const rv = right.eval(row, ctx);
      if ((op === "==" || op === "!=") && (Array.isArray(lv) || Array.isArray(rv))) {
        // pandas rewrites `x == [..]`/`x != [..]` as membership.
        const hit = Array.isArray(lv)
          ? memberOf(rv, lv)
          : memberOf(lv, rv as CellValue[]);
        return op === "!=" ? !hit : hit;
      }
      if (left.isNone || right.isNone) {
        // `x == None` / `x != None` are elementwise-False/True in pandas.
        if (op === "==") return false;
        if (op === "!=") return true;
        const other = left.isNone ? rv : lv;
        throw new TypeError(
          `'${op}' not supported between instances of '${left.isNone ? "NoneType" : pythonOperandTypeName(scalarForMessage(other))}' and '${right.isNone ? "NoneType" : pythonOperandTypeName(scalarForMessage(other))}'`
        );
      }
      return compareCells(lv, rv, op, scope.mode ?? "eval");
    },
  };
}

/**
 * pandas `x in y`:
 *  - column/expr lhs → `lhs.isin(rhs)` elementwise (rhs: list, @list, or a
 *    column whose full value-set we were given).
 *  - string scalar lhs → pandas wraps it to `[lhs]` → `rhs.isin([lhs])`
 *    (equality mask over rhs).
 *  - list lhs → `rhs.isin(lhs)` elementwise over rhs.
 *  - other scalar lhs → Python `lhs in rhs` → for a Series rhs that's
 *    membership on the *index labels* → a scalar result.
 */
function inNode(left: ExprNode, right: ExprNode, negated: boolean, scope: FrameExprScope): ExprNode {
  const apply = (v: boolean) => (negated ? !v : v);
  const isinTypeError = (value: ExprValue): TypeError =>
    new TypeError(
      `only list-like objects are allowed to be passed to isin(), you passed a \`${pythonOperandTypeName(scalarForMessage(value))}\``
    );

  if (left.isScalar) {
    const lVal = evalScalar(left);
    if (typeof lVal === "string" || Array.isArray(lVal)) {
      // ['lhs'] / lhs-list → rhs.isin(lhs-as-list)
      const list = Array.isArray(lVal) ? lVal : [lVal];
      if (right.isScalar) {
        const rVal = evalScalar(right);
        return {
          isScalar: true,
          isBool: true,
          eval: () => {
            if (!Array.isArray(rVal)) {
              throw new TypeError(
                `argument of type '${pythonOperandTypeName(scalarForMessage(rVal))}' is not iterable`
              );
            }
            // Python `wrapped-lhs in list` — element equality on the list itself.
            return apply(memberOf(list, rVal));
          },
        };
      }
      return {
        isScalar: false,
        isBool: true,
        eval: (row, ctx) => apply(memberOf(right.eval(row, ctx), list)),
      };
    }
    // Non-string scalar lhs → Python `lhs in rhs` (scalar result).
    return {
      isScalar: true,
      isBool: true,
      eval: (row, ctx) => {
        if (right.isScalar) {
          const rVal = right.eval(row, ctx);
          if (Array.isArray(rVal)) {
            return apply(memberOf(lVal, rVal));
          }
          throw new TypeError(
            `argument of type '${pythonOperandTypeName(scalarForMessage(rVal))}' is not iterable`
          );
        }
        // pandas `x in Series` → Series.__contains__ → index membership.
        const labels = scope.indexLabels ?? [];
        return apply(memberOf(lVal, labels as CellValue[]));
      },
    };
  }

  // Column/expression lhs → lhs.isin(rhs).
  return {
    isScalar: false,
    isBool: true,
    eval: (row, ctx) => {
      const lv = left.eval(row, ctx);
      if (right.columnName !== undefined) {
        const set = ctx.columnValueSets?.get(right.columnName);
        if (set !== undefined) {
          return apply(memberOfSet(lv, set));
        }
      }
      const rv = right.eval(row, ctx);
      if (Array.isArray(rv)) {
        return apply(memberOf(lv, rv));
      }
      if (isMissing(rv) || typeof rv !== "object") {
        throw isinTypeError(rv);
      }
      throw isinTypeError(rv);
    },
  };
}

// --- method calls ---

const SUPPORTED_METHODS = new Set(["isna", "isnull", "notna", "notnull", "isin"]);
const STR_METHODS = new Set(["contains", "startswith", "endswith"]);

function methodCallNode(target: ExprNode, name: string, args: ExprNode[]): ExprNode {
  if (!SUPPORTED_METHODS.has(name)) {
    throw new ValueError(`"${name}" is not a supported function`);
  }
  return {
    isScalar: target.isScalar,
    isBool: true,
    eval: (row, ctx) => {
      const value = target.eval(row, ctx);
      switch (name) {
        case "isna":
        case "isnull":
          return Array.isArray(value) ? false : isMissing(value);
        case "notna":
        case "notnull":
          return Array.isArray(value) ? true : !isMissing(value);
        case "isin": {
          const list = args[0]?.eval(row, ctx);
          if (!Array.isArray(list)) {
            throw new TypeError(
              `only list-like objects are allowed to be passed to isin(), you passed a \`${pythonOperandTypeName(scalarForMessage(list))}\``
            );
          }
          return memberOf(value, list);
        }
        default:
          throw new ValueError(`"${name}" is not a supported function`);
      }
    },
  };
}

function strMethodNode(target: ExprNode, name: string, args: ExprNode[]): ExprNode {
  if (!STR_METHODS.has(name)) {
    throw new ValueError(`"${name}" is not a supported function`);
  }
  return {
    isScalar: target.isScalar,
    isBool: true,
    eval: (row, ctx) => {
      const value = target.eval(row, ctx);
      if (Array.isArray(value) || isMissing(value)) {
        return false;
      }
      const pattern = String(scalarForMessage(args[0]?.eval(row, ctx) ?? null));
      const text = String(value);
      switch (name) {
        case "contains":
          return new RegExp(pattern).test(text);
        case "startswith":
          return text.startsWith(pattern);
        case "endswith":
          return text.endsWith(pattern);
        default:
          throw new ValueError(`"${name}" is not a supported function`);
      }
    },
  };
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

const KEYWORDS = new Set(["and", "or", "not", "in", "is", "if", "else", "True", "False", "None"]);

interface ParseResult {
  node: ExprNode;
  assignmentTarget?: string;
}

class Parser {
  private position = 0;

  constructor(
    private readonly tokens: Token[],
    private readonly scope: FrameExprScope,
    private readonly referenced: string[],
    private readonly setRefs: string[]
  ) {}

  parseRoot(): ParseResult {
    // `name = expr` — pandas eval assignment target.
    const first = this.tokens[0];
    const second = this.tokens[1];
    if (
      first?.type === "identifier" &&
      !KEYWORDS.has(first.value) &&
      second?.type === "operator" &&
      second.value === "="
    ) {
      this.position = 2;
      const node = this.parseOr();
      this.expectEnd();
      return { node, assignmentTarget: first.value };
    }
    const node = this.parseOr();
    const leftover = this.peek();
    if (leftover?.type === "identifier" && leftover.value === "if") {
      throw new NotImplementedError("'IfExp' nodes are not implemented");
    }
    this.expectEnd();
    return { node };
  }

  private expectEnd(): void {
    if (this.position !== this.tokens.length) {
      const token = this.tokens[this.position];
      if (token?.type === "identifier" && token.value === "if") {
        throw new NotImplementedError("'IfExp' nodes are not implemented");
      }
      throw syntaxError("invalid syntax");
    }
  }

  private peek(offset = 0): Token | undefined {
    return this.tokens[this.position + offset];
  }

  private next(): Token | undefined {
    return this.tokens[this.position++];
  }

  private isOp(token: Token | undefined, value: string): boolean {
    return token?.type === "operator" && token.value === value;
  }

  private isKeyword(token: Token | undefined, value: string): boolean {
    return token?.type === "identifier" && token.value === value;
  }

  private consumeOperator(value: string): boolean {
    if (this.isOp(this.peek(), value)) {
      this.position += 1;
      return true;
    }
    return false;
  }

  private consumeKeyword(value: string): boolean {
    if (this.isKeyword(this.peek(), value)) {
      this.position += 1;
      return true;
    }
    return false;
  }

  // or-level: `or` and `|` (pandas demotes `|` to or-precedence)
  private parseOr(): ExprNode {
    let left = this.parseAnd();
    for (;;) {
      const token = this.peek();
      if (this.isOp(token, "^")) {
        throw new AttributeError("'PandasExprVisitor' object has no attribute 'visit_BitXor'");
      }
      if (this.consumeKeyword("or") || this.consumeOperator("|")) {
        const right = this.parseAnd();
        left = boolOpNode("|", left, right);
        continue;
      }
      return left;
    }
  }

  // and-level: `and` and `&`
  private parseAnd(): ExprNode {
    let left = this.parseNot();
    for (;;) {
      const token = this.peek();
      if (this.isOp(token, "^")) {
        throw new AttributeError("'PandasExprVisitor' object has no attribute 'visit_BitXor'");
      }
      if (this.consumeKeyword("and") || this.consumeOperator("&")) {
        const right = this.parseNot();
        left = boolOpNode("&", left, right);
        continue;
      }
      return left;
    }
  }

  // `not` binds looser than comparisons but tighter than `and`.
  private parseNot(): ExprNode {
    if (this.consumeKeyword("not")) {
      return invertNode(this.parseNot());
    }
    return this.parseComparison();
  }

  private parseComparison(): ExprNode {
    let left = this.parseAdditive();
    const parts: { op: string; node: ExprNode }[] = [];
    for (;;) {
      const token = this.peek();
      if (this.isOp(token, ">>")) {
        throw new AttributeError("'PandasExprVisitor' object has no attribute 'visit_RShift'");
      }
      if (this.isOp(token, "<<")) {
        throw new AttributeError("'PandasExprVisitor' object has no attribute 'visit_LShift'");
      }
      if (this.isKeyword(token, "is")) {
        this.next();
        const negated = this.consumeKeyword("not");
        throw new NotImplementedError(
          negated ? "'IsNot' nodes are not implemented" : "'Is' nodes are not implemented"
        );
      }
      if (this.isKeyword(token, "in")) {
        this.next();
        parts.push({ op: "in", node: this.parseAdditive() });
        continue;
      }
      if (this.isKeyword(token, "not") && this.isKeyword(this.peek(1), "in")) {
        this.next();
        this.next();
        parts.push({ op: "not in", node: this.parseAdditive() });
        continue;
      }
      let matched: string | null = null;
      for (const op of [">=", "<=", "==", "!=", ">", "<"]) {
        if (this.isOp(this.peek(), op)) {
          matched = op;
          this.position += 1;
          break;
        }
      }
      if (!matched) {
        break;
      }
      parts.push({ op: matched, node: this.parseAdditive() });
    }
    if (parts.length === 0) {
      return left;
    }
    // Chained comparisons: `a < b < c` → (a < b) and (b < c).
    const nodes: ExprNode[] = [];
    for (const part of parts) {
      const cmp = this.buildComparison(part.op, left, part.node);
      nodes.push(cmp);
      left = part.node;
    }
    return nodes.reduce((acc, node) => boolOpNode("&", acc, node, true));
  }

  private buildComparison(op: string, left: ExprNode, right: ExprNode): ExprNode {
    if (op === "in" || op === "not in") {
      if (!left.isScalar && right.columnName !== undefined &&
          !this.setRefs.includes(right.columnName)) {
        this.setRefs.push(right.columnName);
      }
      return inNode(left, right, op === "not in", this.scope);
    }
    return compareNode(op, left, right, this.scope);
  }

  private parseAdditive(): ExprNode {
    let left = this.parseMultiplicative();
    for (;;) {
      let op: string | null = null;
      if (this.consumeOperator("+")) {
        op = "+";
      } else if (this.consumeOperator("-")) {
        op = "-";
      }
      if (!op) {
        return left;
      }
      left = arithNode(op, left, this.parseMultiplicative(), this.scope);
    }
  }

  private parseMultiplicative(): ExprNode {
    let left = this.parseUnary();
    for (;;) {
      let op: string | null = null;
      for (const candidate of ["*", "//", "/", "%"]) {
        if (this.consumeOperator(candidate)) {
          op = candidate;
          break;
        }
      }
      if (!op) {
        return left;
      }
      left = arithNode(op, left, this.parseUnary(), this.scope);
    }
  }

  private parseUnary(): ExprNode {
    if (this.consumeOperator("-")) {
      return negateNode(this.parseUnary());
    }
    if (this.consumeOperator("+")) {
      return this.parseUnary();
    }
    if (this.consumeOperator("~")) {
      return invertNode(this.parseUnary());
    }
    return this.parsePower();
  }

  private parsePower(): ExprNode {
    const base = this.parsePostfix();
    if (this.consumeOperator("**")) {
      return powerNode(base, this.parseUnary());
    }
    return base;
  }

  // `.method(args)` / `.str.method(args)` postfix chain
  private parsePostfix(): ExprNode {
    let node = this.parsePrimary();
    for (;;) {
      if (!this.consumeOperator(".")) {
        return node;
      }
      const attr = this.peek();
      if (attr?.type !== "identifier") {
        throw syntaxError("invalid syntax");
      }
      this.next();
      if (attr.value === "str" && !this.isOp(this.peek(), "(")) {
        // `.str` accessor — require a following `.method(...)`.
        if (!this.consumeOperator(".")) {
          throw new AttributeError("'Series' object has no attribute 'str'");
        }
        const method = this.next();
        if (method?.type !== "identifier") {
          throw syntaxError("invalid syntax");
        }
        const args = this.parseCallArgs();
        node = strMethodNode(node, method.value, args);
        continue;
      }
      if (!this.isOp(this.peek(), "(")) {
        throw new AttributeError(`'Series' object has no attribute '${attr.value}'`);
      }
      const args = this.parseCallArgs();
      node = methodCallNode(node, attr.value, args);
    }
  }

  private parseCallArgs(): ExprNode[] {
    if (!this.consumeOperator("(")) {
      throw syntaxError("invalid syntax");
    }
    const args: ExprNode[] = [];
    if (this.consumeOperator(")")) {
      return args;
    }
    for (;;) {
      args.push(this.parseOr());
      if (this.consumeOperator(",")) {
        continue;
      }
      if (!this.consumeOperator(")")) {
        throw syntaxError("invalid syntax");
      }
      return args;
    }
  }

  private parseListLiteral(): ExprNode {
    // caller consumed '['
    const elements: ExprNode[] = [];
    if (this.consumeOperator("]")) {
      return constNode([]);
    }
    for (;;) {
      elements.push(this.parseOr());
      if (this.consumeOperator(",")) {
        if (this.isOp(this.peek(), "]")) {
          this.next();
          return this.makeListNode(elements);
        }
        continue;
      }
      if (!this.consumeOperator("]")) {
        throw syntaxError("invalid syntax");
      }
      return this.makeListNode(elements);
    }
  }

  private makeListNode(elements: ExprNode[]): ExprNode {
    const allScalar = elements.every((el) => el.isScalar);
    return {
      isScalar: allScalar,
      eval: (row, ctx) => elements.map((el) => scalarForMessage(el.eval(row, ctx))),
    };
  }

  private parseParenOrTuple(): ExprNode {
    // caller consumed '('
    if (this.consumeOperator(")")) {
      return constNode([]);
    }
    const first = this.parseOr();
    if (!this.consumeOperator(",")) {
      if (!this.consumeOperator(")")) {
        throw syntaxError("invalid syntax");
      }
      return first;
    }
    const elements = [first];
    for (;;) {
      if (this.isOp(this.peek(), ")")) {
        this.next();
        return this.makeListNode(elements);
      }
      elements.push(this.parseOr());
      if (this.consumeOperator(",")) {
        continue;
      }
      if (!this.consumeOperator(")")) {
        throw syntaxError("invalid syntax");
      }
      return this.makeListNode(elements);
    }
  }

  private parsePrimary(): ExprNode {
    const token = this.next();
    if (!token) {
      throw syntaxError("invalid syntax");
    }
    if (token.type === "number") {
      const literal = Number(token.value);
      if (Number.isNaN(literal)) {
        throw new ValueError(`Invalid number literal '${token.value}'.`);
      }
      return constNode(literal);
    }
    if (token.type === "string") {
      return constNode(token.value);
    }
    if (token.type === "backtick") {
      return resolveIdentifier(token.value, this.scope, this.referenced);
    }
    if (token.type === "at") {
      const value = resolveAtName(token.value, this.scope) as ExprValue;
      return constNode(Array.isArray(value) ? (value as CellValue[]) : (value as CellValue));
    }
    if (token.type === "identifier") {
      if (token.value === "True") {
        return constNode(true);
      }
      if (token.value === "False") {
        return constNode(false);
      }
      if (token.value === "None") {
        return { eval: () => null, isScalar: true, isNone: true };
      }
      if (this.isOp(this.peek(), "(")) {
        // Bare function call — pandas supports a small whitelist only.
        throw new ValueError(`"${token.value}" is not a supported function`);
      }
      if (token.value === "and" || token.value === "or" || token.value === "not" ||
          token.value === "in" || token.value === "is") {
        throw syntaxError("invalid syntax");
      }
      return resolveIdentifier(token.value, this.scope, this.referenced);
    }
    if (token.type === "operator") {
      if (token.value === "(") {
        return this.parseParenOrTuple();
      }
      if (token.value === "[") {
        return this.parseListLiteral();
      }
    }
    throw syntaxError("invalid syntax");
  }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Compiles a pandas-style expression into a safe per-row evaluator.
 * `scope.columns` enables strict name resolution (Unknown name →
 * UndefinedVariableError); without it identifiers fall back to row lookup.
 */
export function compileFrameExpression(
  expr: string,
  scope: FrameExprScope = {}
): CompiledFrameExpr {
  if (typeof expr !== "string" || expr.trim().length === 0) {
    throw new ValueError("expr cannot be an empty string");
  }
  const tokens = tokenize(expr);
  if (tokens.length === 0) {
    throw new ValueError("expr cannot be an empty string");
  }
  const referencedColumns: string[] = [];
  const setRefs: string[] = [];
  const { node, assignmentTarget } = new Parser(tokens, scope, referencedColumns, setRefs).parseRoot();
  return {
    evaluate(row: Row, ctx?: FrameExprEvalContext): CellValue {
      const value = node.eval(row, {
        position: ctx?.position ?? 0,
        columnValueSets: ctx?.columnValueSets,
      });
      return value === undefined ? null : (value as CellValue);
    },
    referencedColumns,
    isScalar: node.isScalar,
    assignmentTarget,
    columnSetRefs: setRefs,
  };
}
