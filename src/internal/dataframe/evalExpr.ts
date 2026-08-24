import type { CellValue, Row } from "../../types";
import { isMissing } from "../../utils";

/**
 * Safe manual parser for simple DataFrame.eval expressions such as
 * `"a + b"`, `"a * 2 - c"`, `"a > 2"`, `"(a + b) / 2 >= 10"`.
 *
 * No `eval()` / `new Function()` — expressions are tokenized and compiled
 * into a small AST evaluated per row. Supported syntax:
 *
 *   - numeric literals
 *   - column-name identifiers
 *   - binary operators:  +  -  *  /  //  %  **
 *   - comparisons:       >  >=  <  <=  ==  !=
 *   - unary +/- and parentheses
 *
 * Missing cells propagate: any arithmetic/comparison touching a missing
 * operand evaluates to null (matching NaN propagation in pandas).
 */

type TokenType = "number" | "identifier" | "operator";

interface Token {
  type: TokenType;
  value: string;
}

const TWO_CHAR_OPERATORS = ["**", "//", ">=", "<=", "==", "!="];
const ONE_CHAR_OPERATORS = "+-*/%><()".split("");

function tokenize(expr: string): Token[] {
  const tokens: Token[] = [];
  let i = 0;
  while (i < expr.length) {
    const char = expr[i]!;
    if (char === " " || char === "\t") {
      i += 1;
      continue;
    }
    const twoChar = expr.slice(i, i + 2);
    if (TWO_CHAR_OPERATORS.includes(twoChar)) {
      tokens.push({ type: "operator", value: twoChar });
      i += 2;
      continue;
    }
    if (ONE_CHAR_OPERATORS.includes(char)) {
      tokens.push({ type: "operator", value: char });
      i += 1;
      continue;
    }
    if (/[0-9]/.test(char) || (char === "." && /[0-9]/.test(expr[i + 1] ?? ""))) {
      let j = i;
      while (j < expr.length && /[0-9.eE]/.test(expr[j]!)) {
        // stop scientific notation at a second e/E or sign handling: keep simple digits+dot
        if ((expr[j] === "e" || expr[j] === "E") &&
            !/[0-9]/.test(expr[j + 1] ?? "") &&
            expr[j + 1] !== "-" && expr[j + 1] !== "+") {
          break;
        }
        j += 1;
      }
      tokens.push({ type: "number", value: expr.slice(i, j) });
      i = j;
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
    throw new Error(`Invalid character '${char}' in expression.`);
  }
  return tokens;
}

/** Row-level evaluator node: returns a numeric/boolean result or null. */
type ExprNode = (row: Row) => CellValue;

function toNumber(value: CellValue): number | null {
  if (isMissing(value)) {
    return null;
  }
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }
  return null;
}

class Parser {
  private tokens: Token[];
  private position = 0;

  constructor(tokens: Token[]) {
    this.tokens = tokens;
  }

  /** Parses the full token stream and returns the root evaluator node. */
  parseRoot(): ExprNode {
    const node = this.parseComparison();
    if (this.position !== this.tokens.length) {
      throw new Error("Unexpected trailing input in expression.");
    }
    return node;
  }

  private peek(): Token | undefined {
    return this.tokens[this.position];
  }

  private consumeOperator(value: string): boolean {
    const token = this.peek();
    if (token?.type === "operator" && token.value === value) {
      this.position += 1;
      return true;
    }
    return false;
  }

  private parseComparison(): ExprNode {
    let left = this.parseAdditive();
    for (;;) {
      let matched: string | null = null;
      for (const op of [">=", "<=", "==", "!=", ">", "<"]) {
        if (this.consumeOperator(op)) {
          matched = op;
          break;
        }
      }
      if (!matched) {
        return left;
      }
      const right = this.parseAdditive();
      const leftFn = left;
      const rightFn = right;
      left = (row) => compareCells(leftFn(row), rightFn(row), matched!);
    }
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
      const right = this.parseMultiplicative();
      const leftFn = left;
      left = (row) => arithCells(leftFn(row), right(row), op!);
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
      const right = this.parseUnary();
      const leftFn = left;
      left = (row) => arithCells(leftFn(row), right(row), op!);
    }
  }

  private parseUnary(): ExprNode {
    if (this.consumeOperator("-")) {
      const inner = this.parseUnary();
      return (row) => {
        const value = toNumber(inner(row));
        return value === null ? null : -value;
      };
    }
    if (this.consumeOperator("+")) {
      return this.parseUnary();
    }
    return this.parsePower();
  }

  private parsePower(): ExprNode {
    const base = this.parsePrimary();
    if (this.consumeOperator("**")) {
      const exponent = this.parseUnary();
      return (row) => {
        const baseValue = toNumber(base(row));
        const expValue = toNumber(exponent(row));
        if (baseValue === null || expValue === null) {
          return null;
        }
        return baseValue ** expValue;
      };
    }
    return base;
  }

  private parsePrimary(): ExprNode {
    const token = this.peek();
    if (!token) {
      throw new Error("Unexpected end of expression.");
    }
    if (token.type === "number") {
      this.position += 1;
      const literal = Number(token.value);
      if (Number.isNaN(literal)) {
        throw new Error(`Invalid number literal '${token.value}'.`);
      }
      return () => literal;
    }
    if (token.type === "identifier") {
      this.position += 1;
      const column = token.value;
      return (row) => row[column] ?? null;
    }
    if (token.type === "operator" && token.value === "(") {
      this.position += 1;
      const inner = this.parseComparison();
      if (!this.consumeOperator(")")) {
        throw new Error("Expected closing parenthesis.");
      }
      return inner;
    }
    throw new Error(`Unexpected token '${token.value}' in expression.`);
  }
}

function arithCells(left: CellValue, right: CellValue, op: string): CellValue {
  const a = toNumber(left);
  const b = toNumber(right);
  if (a === null || b === null) {
    return null;
  }
  switch (op) {
    case "+":
      return a + b;
    case "-":
      return a - b;
    case "*":
      return a * b;
    case "/":
      return b === 0 ? null : a / b;
    case "//":
      return b === 0 ? null : Math.floor(a / b);
    case "%":
      return b === 0 ? null : a % b;
    default:
      throw new Error(`Unsupported operator '${op}'.`);
  }
}

function compareCells(left: CellValue, right: CellValue, op: string): CellValue {
  if (isMissing(left) || isMissing(right)) {
    return null;
  }
  let result: boolean;
  if (typeof left === "string" && typeof right === "string") {
    switch (op) {
      case ">": result = left > right; break;
      case ">=": result = left >= right; break;
      case "<": result = left < right; break;
      case "<=": result = left <= right; break;
      case "==": result = left === right; break;
      case "!=": result = left !== right; break;
      default: throw new Error(`Unsupported operator '${op}'.`);
    }
    return result;
  }
  const a = toNumber(left);
  const b = toNumber(right);
  if (a === null || b === null) {
    return null;
  }
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

export interface CompiledFrameExpr {
  /** Evaluates the expression for one row; null where operands are missing. */
  evaluate(row: Row): CellValue;
  /** Column names referenced by the expression, in order of appearance. */
  referencedColumns: string[];
}

/** Compiles a simple column expression into a safe per-row evaluator. */
export function compileFrameExpression(expr: string): CompiledFrameExpr {
  if (typeof expr !== "string" || expr.trim().length === 0) {
    throw new Error("eval() expects a non-empty string expression.");
  }
  const tokens = tokenize(expr);
  const referencedColumns: string[] = [];
  for (const token of tokens) {
    if (token.type === "identifier" && !referencedColumns.includes(token.value)) {
      referencedColumns.push(token.value);
    }
  }
  const rootNode = new Parser(tokens).parseRoot();
  return {
    evaluate(row: Row): CellValue {
      const value = rootNode(row);
      return value === undefined ? null : value;
    },
    referencedColumns,
  };
}
