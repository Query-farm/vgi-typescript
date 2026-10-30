// Copyright 2026 Query Farm LLC - https://query.farm
// The bounded DuckDB scalar grammars the vgi extension accepts for column
// expressions (vgi_column_expression.cpp: Parse/Scalar) and native CHECK
// constraints (vgi_constraint_contract.cpp: ParseNumericCheck). A worker that
// states something outside them fails ATTACH; checking here names the table and
// column in the worker's own tests instead. This is a conservative recognizer,
// not a DuckDB parser: anything it does not understand is refused.

export type LiteralType = "NULL" | "BOOLEAN" | "INTEGER" | "BIGINT" | "HUGEINT" | "DECIMAL" | "DOUBLE" | "VARCHAR";

export type ScalarNode =
  | { kind: "constant"; type: LiteralType }
  | { kind: "column"; names: string[] }
  | { kind: "cast"; type: string | null; qualified: boolean; child: ScalarNode }
  | { kind: "comparison"; op: string; left: ScalarNode; right: ScalarNode }
  | { kind: "conjunction"; op: "AND" | "OR"; children: ScalarNode[] }
  | { kind: "operator"; op: "NOT" | "IS_NULL" | "IS_NOT_NULL" | "COALESCE"; children: ScalarNode[] }
  | { kind: "function"; name: string; qualified: boolean; children: ScalarNode[] };

type Token =
  | { t: "num"; v: string }
  | { t: "str"; v: string }
  | { t: "ident"; v: string; quoted: boolean }
  | { t: "op"; v: string }
  | { t: "end" };

class GrammarError extends Error {}
const fail = (message: string): never => {
  throw new GrammarError(message);
};

// Words the DuckDB grammar never reads as a bare column reference here.
const RESERVED = new Set([
  "all", "and", "any", "array", "as", "asc", "between", "both", "case", "cast", "check", "collate", "column",
  "constraint", "create", "default", "desc", "distinct", "do", "else", "end", "except", "false", "fetch", "for",
  "foreign", "from", "glob", "grant", "group", "having", "ilike", "in", "intersect", "interval", "into", "is",
  "isnull", "like", "limit", "not", "notnull", "null", "offset", "on", "or", "order", "primary", "references",
  "returning", "select", "similar", "some", "table", "then", "to", "true", "try_cast", "union", "unique", "using",
  "when", "where", "window", "with",
]);

const OPERATORS = ["::", "<=", ">=", "<>", "!=", "==", "||", "//", "**", "=", "<", ">", "+", "-", "*", "/", "%", "^", "(", ")", ",", "."];

function tokenize(sql: string): Token[] {
  const out: Token[] = [];
  let i = 0;
  while (i < sql.length) {
    const c = sql[i];
    if (/\s/.test(c)) { i++; continue; }
    if (sql.startsWith("--", i)) {
      while (i < sql.length && sql[i] !== "\n") i++;
      continue;
    }
    if (sql.startsWith("/*", i)) {
      const close = sql.indexOf("*/", i + 2);
      if (close < 0) fail("unterminated comment");
      i = close + 2;
      continue;
    }
    if (/[0-9]/.test(c) || (c === "." && /[0-9]/.test(sql[i + 1] ?? ""))) {
      const m = /^(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?/.exec(sql.slice(i))!;
      if (/[A-Za-z_]/.test(sql[i + m[0].length] ?? "")) fail("unsupported numeric literal");
      out.push({ t: "num", v: m[0] });
      i += m[0].length;
      continue;
    }
    if (c === "'" || c === '"') {
      let j = i + 1, text = "";
      for (;;) {
        if (j >= sql.length) fail("unterminated quoted text");
        if (sql[j] === c) {
          if (sql[j + 1] === c) { text += c; j += 2; continue; }
          break;
        }
        text += sql[j++];
      }
      out.push(c === "'" ? { t: "str", v: text } : { t: "ident", v: text, quoted: true });
      i = j + 1;
      continue;
    }
    if (/[A-Za-z_\u0080-￿]/.test(c)) {
      const m = /^[A-Za-z_\u0080-￿][A-Za-z0-9_$\u0080-￿]*/.exec(sql.slice(i))!;
      out.push({ t: "ident", v: m[0], quoted: false });
      i += m[0].length;
      continue;
    }
    const op = OPERATORS.find((o) => sql.startsWith(o, i));
    if (!op) fail(`unsupported character '${c}'`);
    out.push({ t: "op", v: op! });
    i += op!.length;
  }
  out.push({ t: "end" });
  return out;
}

// DuckDB's default binding of a cast target, restricted to the types any of the
// grammars can accept; every other type maps to null (refused).
const CAST_TYPES: Record<string, string> = {
  boolean: "BOOLEAN", bool: "BOOLEAN", logical: "BOOLEAN",
  tinyint: "TINYINT", int1: "TINYINT",
  smallint: "SMALLINT", int2: "SMALLINT", short: "SMALLINT",
  integer: "INTEGER", int: "INTEGER", int4: "INTEGER", signed: "INTEGER",
  bigint: "BIGINT", int8: "BIGINT", long: "BIGINT",
  float: "FLOAT", real: "FLOAT", float4: "FLOAT",
  double: "DOUBLE", float8: "DOUBLE",
  decimal: "DECIMAL", numeric: "DECIMAL",
  varchar: "VARCHAR", text: "VARCHAR", string: "VARCHAR", char: "VARCHAR", bpchar: "VARCHAR",
  nvarchar: "VARCHAR", character: "VARCHAR",
};

const INT64_MAX = (1n << 63n) - 1n;

function literalNumber(text: string, negative: boolean): ScalarNode {
  if (/^[0-9]+$/.test(text)) {
    const value = BigInt(text) * (negative ? -1n : 1n);
    const type: LiteralType = value >= -(2n ** 31n) && value < 2n ** 31n ? "INTEGER"
      : value >= -INT64_MAX - 1n && value <= INT64_MAX ? "BIGINT" : "HUGEINT";
    return { kind: "constant", type };
  }
  // A decimal point without an exponent is a DECIMAL literal while it fits.
  if (!/[eE]/.test(text) && text.replace(".", "").replace(/^0+/, "").length <= 38) return { kind: "constant", type: "DECIMAL" };
  return { kind: "constant", type: "DOUBLE" };
}

class Parser {
  private pos = 0;
  constructor(private readonly tokens: Token[]) {}

  private peek(offset = 0): Token { return this.tokens[Math.min(this.pos + offset, this.tokens.length - 1)]; }
  private next(): Token { return this.tokens[this.pos++]; }
  private isOp(v: string, offset = 0): boolean {
    const t = this.peek(offset);
    return t.t === "op" && t.v === v;
  }
  private isWord(word: string, offset = 0): boolean {
    const t = this.peek(offset);
    return t.t === "ident" && !t.quoted && t.v.toLowerCase() === word;
  }
  private expectOp(v: string): void {
    if (!this.isOp(v)) fail(`expected '${v}'`);
    this.pos++;
  }

  parseAll(): ScalarNode {
    const node = this.expr();
    if (this.peek().t !== "end") fail("unexpected text after the expression");
    return node;
  }

  private expr(): ScalarNode { return this.or(); }

  private or(): ScalarNode {
    let left = this.and();
    while (this.isWord("or")) {
      this.pos++;
      left = { kind: "conjunction", op: "OR", children: [left, this.and()] };
    }
    return left;
  }

  private and(): ScalarNode {
    let left = this.not();
    while (this.isWord("and")) {
      this.pos++;
      left = { kind: "conjunction", op: "AND", children: [left, this.not()] };
    }
    return left;
  }

  private not(): ScalarNode {
    if (this.isWord("not")) {
      this.pos++;
      return { kind: "operator", op: "NOT", children: [this.not()] };
    }
    return this.is();
  }

  private is(): ScalarNode {
    let left = this.comparison();
    for (;;) {
      if (this.isWord("isnull")) { this.pos++; left = { kind: "operator", op: "IS_NULL", children: [left] }; continue; }
      if (this.isWord("notnull")) { this.pos++; left = { kind: "operator", op: "IS_NOT_NULL", children: [left] }; continue; }
      if (this.isWord("is")) {
        this.pos++;
        const negated = this.isWord("not");
        if (negated) this.pos++;
        if (!this.isWord("null")) fail("only IS [NOT] NULL is supported");
        this.pos++;
        left = { kind: "operator", op: negated ? "IS_NOT_NULL" : "IS_NULL", children: [left] };
        continue;
      }
      return left;
    }
  }

  private comparison(): ScalarNode {
    const left = this.otherOp();
    const t = this.peek();
    if (t.t === "op" && ["=", "==", "<>", "!=", "<", ">", "<=", ">="].includes(t.v)) {
      this.pos++;
      const right = this.otherOp();
      const next = this.peek();
      if (next.t === "op" && ["=", "==", "<>", "!=", "<", ">", "<=", ">="].includes(next.v)) fail("chained comparisons are unsupported");
      return { kind: "comparison", op: t.v === "==" ? "=" : t.v === "!=" ? "<>" : t.v, left, right };
    }
    for (const word of ["between", "in", "like", "ilike", "glob", "similar", "not", "collate"]) {
      if (this.isWord(word)) fail(`${word.toUpperCase()} is outside the supported scalar grammar`);
    }
    return left;
  }

  // DuckDB's generic operator precedence level (||, //, **).
  private otherOp(): ScalarNode {
    let left = this.additive();
    for (;;) {
      const t = this.peek();
      if (t.t === "op" && ["||", "//", "**"].includes(t.v)) {
        this.pos++;
        left = { kind: "function", name: t.v, qualified: false, children: [left, this.additive()] };
        continue;
      }
      return left;
    }
  }

  private additive(): ScalarNode {
    let left = this.multiplicative();
    for (;;) {
      const t = this.peek();
      if (t.t === "op" && (t.v === "+" || t.v === "-")) {
        this.pos++;
        left = { kind: "function", name: t.v, qualified: false, children: [left, this.multiplicative()] };
        continue;
      }
      return left;
    }
  }

  private multiplicative(): ScalarNode {
    let left = this.power();
    for (;;) {
      const t = this.peek();
      if (t.t === "op" && (t.v === "*" || t.v === "/" || t.v === "%")) {
        this.pos++;
        left = { kind: "function", name: t.v, qualified: false, children: [left, this.power()] };
        continue;
      }
      return left;
    }
  }

  private power(): ScalarNode {
    let left = this.unary();
    while (this.isOp("^")) {
      this.pos++;
      left = { kind: "function", name: "^", qualified: false, children: [left, this.unary()] };
    }
    return left;
  }

  private unary(): ScalarNode {
    if (this.isOp("-") || this.isOp("+")) {
      const sign = (this.next() as { v: string }).v;
      const operand = this.unary();
      // The grammar folds a minus applied directly to a numeric literal.
      if (sign === "-" && operand.kind === "constant" && (operand as any).fromNumber) {
        const folded = literalNumber((operand as any).text, !(operand as any).negative);
        return Object.assign(folded, { fromNumber: true, text: (operand as any).text, negative: !(operand as any).negative });
      }
      return { kind: "function", name: sign, qualified: false, children: [operand] };
    }
    return this.postfix();
  }

  private postfix(): ScalarNode {
    let node = this.primary();
    for (;;) {
      if (this.isOp("::")) {
        this.pos++;
        node = { kind: "cast", ...this.typeName(), child: node };
        continue;
      }
      if (this.isOp("[")) fail("subscripts are unsupported");
      return node;
    }
  }

  private typeName(): { type: string | null; qualified: boolean } {
    const t = this.next();
    if (t.t !== "ident") fail("expected a type name");
    const first = (t as { v: string }).v.toLowerCase();
    let qualified = false;
    while (this.isOp(".")) {
      this.pos++;
      if (this.next().t !== "ident") fail("expected a type name");
      qualified = true;
    }
    let name = first;
    if (first === "double" && this.isWord("precision")) { this.pos++; name = "double"; }
    if (first === "character" && this.isWord("varying")) { this.pos++; name = "varchar"; }
    let params: string[] = [];
    if (this.isOp("(")) {
      this.pos++;
      for (;;) {
        const p = this.next();
        if (p.t !== "num") fail("unsupported type parameter");
        params.push((p as { v: string }).v);
        if (this.isOp(",")) { this.pos++; continue; }
        break;
      }
      this.expectOp(")");
    }
    if (this.isOp("[")) fail("array types are unsupported");
    let type = CAST_TYPES[name] ?? null;
    if (type === "FLOAT" && params.length === 1 && Number(params[0]) > 24) type = "DOUBLE";
    return { type, qualified };
  }

  private primary(): ScalarNode {
    const t = this.next();
    if (t.t === "num") return Object.assign(literalNumber(t.v, false), { fromNumber: true, text: t.v, negative: false });
    if (t.t === "str") {
      if (this.peek().t === "str") fail("adjacent string literals are unsupported");
      return { kind: "constant", type: "VARCHAR" };
    }
    if (t.t === "op") {
      if (t.v === "(") {
        const inner = this.expr();
        if (this.isOp(",")) fail("row constructors are unsupported");
        this.expectOp(")");
        return inner;
      }
      return fail(`unexpected '${t.v}'`);
    }
    if (t.t === "end") return fail("expected an expression");
    const word = t.v.toLowerCase();
    if (!t.quoted) {
      if (word === "null") return { kind: "constant", type: "NULL" };
      if (word === "true" || word === "false") return { kind: "constant", type: "BOOLEAN" };
      if (word === "cast" && this.isOp("(")) {
        this.pos++;
        const child = this.expr();
        if (!this.isWord("as")) fail("expected AS in CAST");
        this.pos++;
        const target = this.typeName();
        this.expectOp(")");
        return { kind: "cast", ...target, child };
      }
      if (word === "case") fail("CASE is outside the supported scalar grammar");
      if (RESERVED.has(word)) fail(`unexpected keyword ${t.v.toUpperCase()}`);
    }
    // Qualified names: a column reference or function with more than one part.
    const names = [t.v];
    while (this.isOp(".")) {
      this.pos++;
      const part = this.next();
      if (part.t !== "ident") fail("unsupported qualified name");
      names.push((part as { v: string }).v);
    }
    if (this.isOp("(")) {
      this.pos++;
      const children: ScalarNode[] = [];
      if (this.isWord("distinct")) fail("DISTINCT function arguments are unsupported");
      if (!this.isOp(")")) {
        for (;;) {
          if (this.isOp("*")) fail("star arguments are unsupported");
          if (this.peek(1).t === "op" && ((this.peek(1) as { v: string }).v === ":" || (this.peek(1) as { v: string }).v === "=")
              && this.peek().t === "ident") fail("named scalar arguments are unsupported");
          children.push(this.expr());
          if (this.isOp(",")) { this.pos++; continue; }
          break;
        }
      }
      if (this.isWord("order")) fail("ordered function arguments are unsupported");
      this.expectOp(")");
      if (this.isWord("filter") || this.isWord("over")) fail("aggregate and window functions are unsupported");
      if (names.length === 1 && !t.quoted && word === "coalesce") return { kind: "operator", op: "COALESCE", children };
      return { kind: "function", name: names[names.length - 1], qualified: names.length > 1, children };
    }
    if (this.peek().t === "str") fail("typed literals are unsupported");
    return { kind: "column", names };
  }
}

/** Parse one DuckDB scalar expression in the bounded grammar's syntax. */
export function parseScalarSql(sql: string): ScalarNode {
  if (typeof sql !== "string" || !sql || sql.includes("\0") || new TextEncoder().encode(sql).length > 1024 * 1024)
    fail("invalid SQL size or text");
  return new Parser(tokenize(sql)).parseAll();
}

const PRIMITIVE = new Set(["NULL", "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "VARCHAR"]);
const GENERATED_PRIMITIVE = new Set(["NULL", "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "VARCHAR"]);
const ALLOWED_FUNCTIONS = new Set(["+", "-", "*", "%", "abs", "lower", "upper"]);

/**
 * Check `sql` against the extension's column-expression grammar
 * (vgi_column_expression.cpp `Parse`): a DEFAULT (`usingColumns` false: no
 * column references), a USING expression (`usingColumns` true: `/` allowed) or
 * a VIRTUAL generated column (`generated`: column references, `||`, no
 * DECIMAL/floating literals or casts). Returns the referenced column names.
 * Throws an Error naming the rule broken.
 */
export function validateColumnExpressionSql(
  sql: string,
  options: { usingColumns: boolean; generated: boolean },
): string[] {
  const { usingColumns, generated } = options;
  const primitive = generated ? GENERATED_PRIMITIVE : PRIMITIVE;
  const columns: string[] = [];
  let nodes = 0;
  const visit = (node: ScalarNode, depth: number): void => {
    if (depth > 64 || ++nodes > 4096) fail("scalar expression is too complex");
    switch (node.kind) {
      case "constant":
        if (!primitive.has(node.type)) fail(`unsupported scalar literal type ${node.type}`);
        return;
      case "column":
        if (!usingColumns || node.names.length !== 1) fail("column references are unsupported in this scope");
        columns.push(node.names[0]);
        return;
      case "cast":
        if (node.qualified) fail("qualified scalar cast types are unsupported");
        if (node.type === null || !primitive.has(node.type)) fail("unsupported scalar cast");
        visit(node.child, depth + 1);
        return;
      case "comparison":
        visit(node.left, depth + 1);
        visit(node.right, depth + 1);
        return;
      case "conjunction":
      case "operator":
        node.children.forEach((c) => visit(c, depth + 1));
        return;
      case "function": {
        const lower = node.name.toLowerCase();
        const ok = !node.qualified && (ALLOWED_FUNCTIONS.has(lower) ||
          (usingColumns && !generated && node.name === "/") || (generated && node.name === "||"));
        if (!ok) fail(`unsupported scalar function '${node.name}'`);
        node.children.forEach((c) => visit(c, depth + 1));
        return;
      }
    }
  };
  try {
    visit(parseScalarSql(sql), 0);
  } catch (error) {
    if (error instanceof GrammarError) throw error;
    throw new GrammarError(String((error as Error)?.message ?? error));
  }
  return columns;
}

/**
 * The extension's native CHECK read grammar (`ParseNumericCheck`): exactly one
 * comparison (=, <>, <, >, <=, >=) whose operands are each a non-NULL numeric
 * literal or an unqualified column reference. `resolveColumn` checks a
 * referenced column (by case-insensitive name) and throws when it is not an
 * acceptable operand.
 */
export function validateNumericCheckSql(sql: string, resolveColumn: (name: string) => void): void {
  const node = parseScalarSql(sql);
  if (node.kind !== "comparison") fail("native CHECK read supports only Boolean numeric comparisons");
  const comparison = node as Extract<ScalarNode, { kind: "comparison" }>;
  for (const operand of [comparison.left, comparison.right]) {
    if (operand.kind === "constant") {
      if (operand.type === "NULL" || operand.type === "BOOLEAN" || operand.type === "VARCHAR")
        fail("CHECK requires numeric operands");
      continue;
    }
    if (operand.kind !== "column") fail("native CHECK read supports only numeric column/constant comparisons");
    const column = operand as Extract<ScalarNode, { kind: "column" }>;
    if (column.names.length !== 1) fail("CHECK requires unqualified column references");
    resolveColumn(column.names[0]);
  }
}
