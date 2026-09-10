// Copyright 2025, 2026 Query Farm LLC - https://query.farm

import {
  type VgiBatch,
  type VgiDataType,
  type VgiField,
  type VgiSchema,
  isBool,
  isDate,
  isList,
  isStruct,
  isTime,
  isTimestamp,
  isUtf8,
  readCanonicalValue,
  typeSignature,
} from "../arrow/index.js";
import { PushdownFilters } from "./evaluate.js";
import {
  ComparisonOp,
  DUCKDB_SESSION_CONTEXT,
  DUCKDB_STANDARD_V1,
  FILTER_ENCODING,
  FILTER_VERSION,
  NO_EVALUATION_CONTEXT,
  type EvaluationContext,
  type FilterCapabilities,
  type FilterExpression,
  type FilterSet,
  type FilterPredicate,
  type FunctionIdentity,
  type PredicateMode,
  type PredicateSource,
  type StandardFilterFunction,
} from "./types.js";

const MAX_JSON_BYTES = 1 << 20;
const MAX_DEPTH = 64;
const MAX_NODES = 10_000;
const MAX_PREDICATES = 1_024;
const MAX_PREDICATE_IDS = 4_096;
const MAX_ARGUMENTS = 256;
const MAX_ID_BYTES = 128;
const MAX_FINGERPRINT_BYTES = 256;
const PAYLOAD_NAME = /^(?:value|type|artifact)_(?:0|[1-9][0-9]*)$/;
const IDENTITY_NAMESPACE = /^[a-z][a-z0-9]*(?:\.[a-z][a-z0-9_]*)*$/;
const IDENTITY_NAME = /^[a-z][a-z0-9_]*$/;
const MODES = new Set<PredicateMode>(["required", "advisory"]);
const SOURCES = new Set<PredicateSource>(["query", "join", "top_n", "split_refinement", "other"]);
const STANDARD_FUNCTIONS = new Set<StandardFilterFunction>([
  "starts_with", "ends_with", "contains", "list_contains",
]);
const KNOWN_EXTENSION_FUNCTIONS = new Set(["duckdb.spatial/intersects_extent@1"]);
const KNOWN_RUNTIME_ALGORITHMS = new Set([
  "duckdb.runtime_filter/bloom@1",
  "duckdb.runtime_filter/prefix_range@1",
]);
const KNOWN_ARROW_EXTENSIONS = new Set([
  "arrow.bool8", "arrow.json", "arrow.uuid",
  "geoarrow.linestring", "geoarrow.multilinestring", "geoarrow.multipoint",
  "geoarrow.multipolygon", "geoarrow.point", "geoarrow.polygon", "geoarrow.wkb",
]);

export class FilterV2Error extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FilterV2Error";
  }
}

export interface DeserializeFilterOptions extends FilterCapabilities {
  /** The authoritative, unprojected bind output schema. */
  outputSchema: VgiSchema;
  joinKeyBatches?: VgiBatch[];
}

function utf8Length(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function expectObject(value: unknown, where: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new FilterV2Error(`${where} must be a JSON object`);
  }
  return value as Record<string, unknown>;
}

function expectArray(value: unknown, where: string): unknown[] {
  if (!Array.isArray(value)) throw new FilterV2Error(`${where} must be a JSON array`);
  return value;
}

function expectString(value: unknown, where: string, allowEmpty = false): string {
  if (typeof value !== "string" || (!allowEmpty && value.length === 0)) {
    throw new FilterV2Error(`${where} must be a${allowEmpty ? "" : " nonempty"} string`);
  }
  return value;
}

function expectBoolean(value: unknown, where: string): boolean {
  if (typeof value !== "boolean") throw new FilterV2Error(`${where} must be a Boolean`);
  return value;
}

function expectUint(value: unknown, where: string, positive = false): number {
  if (!Number.isSafeInteger(value) || (value as number) < (positive ? 1 : 0)) {
    throw new FilterV2Error(`${where} must be a safely representable unsigned integer`);
  }
  return value as number;
}

function expectKeys(
  object: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[],
  where: string,
): void {
  const allowed = new Set([...required, ...optional]);
  const missing = required.filter((key) => !(key in object));
  const unknown = Object.keys(object).filter((key) => !allowed.has(key));
  if (missing.length) throw new FilterV2Error(`${where} is missing ${missing.join(", ")}`);
  if (unknown.length) throw new FilterV2Error(`${where} has unknown properties ${unknown.join(", ")}`);
}

function validateArrowExtensions(field: VgiField): void {
  const names = [field.metadata?.get("ARROW:extension:name"), (field.type as any).extensionName,
    (field.type as any).extension_name].filter((value): value is string => typeof value === "string");
  for (const name of names) {
    if (!KNOWN_ARROW_EXTENSIONS.has(name)) throw new FilterV2Error(`unknown Arrow extension type ${name}`);
  }
  const children = (field.type as any).children as VgiField[] | undefined;
  children?.forEach(validateArrowExtensions);
}

// JSON.parse silently accepts duplicate object names. VGI v2 does not, so this
// deliberately small parser detects them while retaining native JS values.
class StrictJsonParser {
  private offset = 0;

  constructor(private readonly text: string) {}

  parse(): unknown {
    const result = this.value();
    this.space();
    if (this.offset !== this.text.length) throw this.error("trailing JSON data");
    return result;
  }

  private error(message: string): FilterV2Error {
    return new FilterV2Error(`invalid filter JSON at byte ${this.offset}: ${message}`);
  }

  private space(): void {
    while (/\s/.test(this.text[this.offset] ?? "")) this.offset++;
  }

  private value(): unknown {
    this.space();
    const ch = this.text[this.offset];
    if (ch === "{") return this.object();
    if (ch === "[") return this.array();
    if (ch === '"') return this.string();
    for (const [word, value] of [["true", true], ["false", false], ["null", null]] as const) {
      if (this.text.startsWith(word, this.offset)) {
        this.offset += word.length;
        return value;
      }
    }
    const match = this.text.slice(this.offset).match(/^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/);
    if (match) {
      this.offset += match[0].length;
      const number = Number(match[0]);
      if (!Number.isFinite(number)) throw this.error("non-finite number");
      return number;
    }
    throw this.error("expected a JSON value");
  }

  private string(): string {
    const start = this.offset++;
    let escaped = false;
    while (this.offset < this.text.length) {
      const ch = this.text[this.offset++];
      if (!escaped && ch === '"') {
        try {
          return JSON.parse(this.text.slice(start, this.offset));
        } catch {
          throw this.error("invalid string escape");
        }
      }
      if (!escaped && ch.charCodeAt(0) < 0x20) throw this.error("control character in string");
      if (!escaped && ch === "\\") escaped = true;
      else escaped = false;
    }
    throw this.error("unterminated string");
  }

  private object(): Record<string, unknown> {
    this.offset++;
    const result: Record<string, unknown> = {};
    const names = new Set<string>();
    this.space();
    if (this.text[this.offset] === "}") {
      this.offset++;
      return result;
    }
    while (true) {
      this.space();
      if (this.text[this.offset] !== '"') throw this.error("expected object key");
      const key = this.string();
      if (names.has(key)) throw this.error(`duplicate object key ${JSON.stringify(key)}`);
      names.add(key);
      this.space();
      if (this.text[this.offset++] !== ":") throw this.error("expected ':'");
      result[key] = this.value();
      this.space();
      const separator = this.text[this.offset++];
      if (separator === "}") return result;
      if (separator !== ",") throw this.error("expected ',' or '}'");
    }
  }

  private array(): unknown[] {
    this.offset++;
    const result: unknown[] = [];
    this.space();
    if (this.text[this.offset] === "]") {
      this.offset++;
      return result;
    }
    while (true) {
      result.push(this.value());
      this.space();
      const separator = this.text[this.offset++];
      if (separator === "]") return result;
      if (separator !== ",") throw this.error("expected ',' or ']'");
    }
  }
}

function metadataValue(schema: VgiSchema, key: string): string {
  const value = schema.metadata?.get(key);
  if (value === undefined) throw new FilterV2Error(`missing schema metadata ${key}`);
  return value;
}

function parseContext(schema: VgiSchema): EvaluationContext {
  if (metadataValue(schema, "vgi_filter_encoding") !== FILTER_ENCODING) {
    throw new FilterV2Error("unsupported filter encoding");
  }
  if (metadataValue(schema, "vgi_filter_version") !== FILTER_VERSION) {
    throw new FilterV2Error("unsupported filter version");
  }
  const profile = metadataValue(schema, "vgi_evaluation_context");
  const contextKeys = [
    "vgi_time_zone", "vgi_calendar", "vgi_default_collation",
    "vgi_ieee_floating_point_ops", "vgi_integer_division",
    "vgi_context_provider_fingerprint",
  ];
  if (profile === NO_EVALUATION_CONTEXT) {
    if (contextKeys.some((key) => schema.metadata.has(key))) {
      throw new FilterV2Error("vgi.none.v1 forbids DuckDB session context metadata");
    }
    return { profile };
  }
  if (profile !== DUCKDB_SESSION_CONTEXT) throw new FilterV2Error(`unknown evaluation context ${profile}`);
  const required = contextKeys.slice(0, 5);
  for (const key of required) metadataValue(schema, key);
  const bool = (key: string): boolean => {
    const value = metadataValue(schema, key);
    if (value !== "true" && value !== "false") throw new FilterV2Error(`${key} must be true or false`);
    return value === "true";
  };
  const fingerprint = schema.metadata.get("vgi_context_provider_fingerprint");
  if (fingerprint !== undefined && (!fingerprint || utf8Length(fingerprint) > MAX_FINGERPRINT_BYTES)) {
    throw new FilterV2Error("invalid context provider fingerprint");
  }
  return {
    profile,
    timeZone: metadataValue(schema, "vgi_time_zone"),
    calendar: metadataValue(schema, "vgi_calendar"),
    defaultCollation: metadataValue(schema, "vgi_default_collation"),
    ieeeFloatingPointOps: bool("vgi_ieee_floating_point_ops"),
    integerDivision: bool("vgi_integer_division"),
    providerFingerprint: fingerprint,
  };
}

function identityKey(identity: FunctionIdentity): string {
  return `${identity.namespace}/${identity.name}@${identity.version}`;
}

class FilterParser {
  private nodes = 0;
  readonly context: EvaluationContext;
  readonly document: Record<string, unknown>;

  constructor(
    readonly batch: VgiBatch,
    readonly options: DeserializeFilterOptions,
  ) {
    if (batch.numRows !== 1) throw new FilterV2Error("filter batch must contain exactly one row");
    const first = batch.schema.fields[0];
    if (!first || first.name !== "filter_spec" || first.nullable || !isUtf8(first.type)) {
      throw new FilterV2Error("first field must be filter_spec: utf8 not null");
    }
    const names = batch.schema.fields.map((field) => field.name);
    if (new Set(names).size !== names.length) throw new FilterV2Error("filter payload names must be unique");
    for (let i = 1; i < names.length; i++) {
      if (!PAYLOAD_NAME.test(names[i])) throw new FilterV2Error(`noncanonical payload name ${names[i]}`);
      if (names[i].startsWith("type_") && this.valueAt(i) !== null) {
        throw new FilterV2Error(`${names[i]} must contain NULL`);
      }
    }
    this.context = parseContext(batch.schema);
    this.validateContextCapability();
    const raw = this.valueAt(0);
    if (typeof raw !== "string" || utf8Length(raw) > MAX_JSON_BYTES) {
      throw new FilterV2Error("filter_spec must be UTF-8 JSON no larger than 1 MiB");
    }
    this.document = expectObject(new StrictJsonParser(raw).parse(), "filter document");
  }

  private valueAt(index: number): unknown {
    const column = this.batch.getChildAt(index);
    if (!column) throw new FilterV2Error(`missing payload column ${index}`);
    return readCanonicalValue(this.batch.schema.fields[index].type, column, 0);
  }

  private validateContextCapability(): void {
    if (this.context.profile === NO_EVALUATION_CONTEXT) return;
    const matching = (this.options.evaluationContexts ?? [])
      .filter((value) => value.profile === this.context.profile);
    if (!matching.length) throw new FilterV2Error(`evaluation context ${this.context.profile} was not advertised`);
    if (this.context.providerFingerprint !== undefined &&
        !matching.some((value) => value.providerFingerprint === this.context.providerFingerprint)) {
      throw new FilterV2Error("evaluation context fingerprint was not advertised");
    }
  }

  private header(kind: "snapshot" | "delta", member: "predicates" | "updates"): unknown[] {
    expectKeys(this.document, ["encoding", "semantics", "kind", member], [], "filter document");
    if (this.document.encoding !== FILTER_ENCODING) throw new FilterV2Error("document encoding must be vgi.filters.v2");
    if (this.document.semantics !== DUCKDB_STANDARD_V1) throw new FilterV2Error("unsupported filter semantics");
    if (this.document.kind !== kind) throw new FilterV2Error(`expected a ${kind} document`);
    return expectArray(this.document[member], member);
  }

  snapshot(): PushdownFilters {
    const entries = this.header("snapshot", "predicates");
    if (entries.length > MAX_PREDICATES) throw new FilterV2Error("snapshot predicate limit exceeded");
    const predicates = entries.map((value, index) => this.predicate(expectObject(value, `predicates[${index}]`), `predicates[${index}]`));
    const ids = new Set<string>();
    for (const predicate of predicates) {
      if (predicate.revision !== 0) throw new FilterV2Error("snapshot revisions must be zero");
      if (ids.has(predicate.id)) throw new FilterV2Error(`duplicate predicate id ${predicate.id}`);
      ids.add(predicate.id);
    }
    return new PushdownFilters(predicates, this.context, this.options, new Map(predicates.map((p) => [p.id, 0])),
      new Set(predicates.filter((p) => p.mode === "required").map((p) => p.id)));
  }

  delta(prior: PushdownFilters): PushdownFilters {
    if (JSON.stringify(this.context) !== JSON.stringify(prior.evaluationContext)) {
      throw new FilterV2Error("evaluation context changed within one scan");
    }
    const entries = this.header("delta", "updates");
    const live = new Map(prior.predicates.map((predicate) => [predicate.id, predicate]));
    const revisions = new Map(prior.revisions);
    const parsed: Array<{ id: string; revision: number; predicate?: FilterPredicate }> = [];
    const seen = new Set<string>();
    for (let index = 0; index < entries.length; index++) {
      const where = `updates[${index}]`;
      const object = expectObject(entries[index], where);
      const operation = expectString(object.operation, `${where}.operation`);
      const common = operation === "upsert"
        ? ["operation", "id", "revision", "mode", "source", "expression"]
        : ["operation", "id", "revision"];
      expectKeys(object, common, [], where);
      if (operation !== "upsert" && operation !== "remove") throw new FilterV2Error(`${where} has invalid operation`);
      const id = this.predicateId(object.id, where);
      const revision = expectUint(object.revision, `${where}.revision`);
      if (seen.has(id)) throw new FilterV2Error(`duplicate delta predicate id ${id}`);
      seen.add(id);
      if (prior.requiredIds.has(id)) throw new FilterV2Error(`delta targets required predicate ${id}`);
      if (operation === "upsert") {
        const mode = expectString(object.mode, `${where}.mode`) as PredicateMode;
        const source = expectString(object.source, `${where}.source`) as PredicateSource;
        if (!MODES.has(mode) || !SOURCES.has(source)) {
          throw new FilterV2Error(`${where} has unknown mode or source`);
        }
        expectObject(object.expression, `${where}.expression`);
        if (mode !== "advisory") throw new FilterV2Error("delta upserts must be advisory");
      }
      if (revision <= (revisions.get(id) ?? -1)) continue;
      parsed.push({ id, revision, predicate: operation === "upsert" ? this.predicate(object, where, true) : undefined });
    }
    if (new Set([...revisions.keys(), ...parsed.map((entry) => entry.id)]).size > MAX_PREDICATE_IDS) {
      throw new FilterV2Error("delta predicate-id limit exceeded");
    }
    for (const update of parsed) {
      revisions.set(update.id, update.revision);
      if (update.predicate) live.set(update.id, update.predicate);
      else live.delete(update.id);
    }
    return new PushdownFilters([...live.values()], this.context, prior.options, revisions, new Set(prior.requiredIds));
  }

  private predicateId(value: unknown, where: string): string {
    const id = expectString(value, `${where}.id`);
    if (utf8Length(id) > MAX_ID_BYTES) throw new FilterV2Error(`${where}.id is too long`);
    return id;
  }

  private predicate(object: Record<string, unknown>, where: string, update = false): FilterPredicate {
    expectKeys(object, [...(update ? ["operation"] : []), "id", "revision", "mode", "source", "expression"], [], where);
    const mode = expectString(object.mode, `${where}.mode`) as PredicateMode;
    const source = expectString(object.source, `${where}.source`) as PredicateSource;
    if (!MODES.has(mode) || !SOURCES.has(source)) throw new FilterV2Error(`${where} has unknown mode or source`);
    const expression = this.expression(expectObject(object.expression, `${where}.expression`), 1, true);
    if (this.context.profile === NO_EVALUATION_CONTEXT && requiresSessionContext(expression)) {
      throw new FilterV2Error("context-dependent expression requires vgi.duckdb.session.v1");
    }
    if (expression.node === "runtime_filter" && mode !== "advisory") {
      throw new FilterV2Error("runtime_filter predicates must be advisory");
    }
    if (expression.node !== "runtime_filter" && !isBooleanExpression(expression)) {
      throw new FilterV2Error("predicate root must resolve to BOOLEAN");
    }
    return {
      id: this.predicateId(object.id, where),
      revision: expectUint(object.revision, `${where}.revision`),
      mode,
      source,
      expression,
    };
  }

  private payload(prefix: "value" | "type" | "artifact", rawRef: unknown): { ref: number; field: VgiField; value: unknown } {
    const ref = expectUint(rawRef, `${prefix}_ref`);
    const name = `${prefix}_${ref}`;
    const indexes = this.batch.schema.fields.flatMap((field, index) => field.name === name ? [index] : []);
    if (indexes.length !== 1) throw new FilterV2Error(`missing or duplicate payload ${name}`);
    return { ref, field: this.batch.schema.fields[indexes[0]], value: this.valueAt(indexes[0]) };
  }

  private identity(raw: unknown, where: string): FunctionIdentity {
    const object = expectObject(raw, where);
    expectKeys(object, ["namespace", "name", "version"], [], where);
    const identity = {
      namespace: expectString(object.namespace, `${where}.namespace`),
      name: expectString(object.name, `${where}.name`),
      version: expectUint(object.version, `${where}.version`, true),
    };
    if (!IDENTITY_NAMESPACE.test(identity.namespace) || !IDENTITY_NAME.test(identity.name)) {
      throw new FilterV2Error(`${where} has a noncanonical identity`);
    }
    return identity;
  }

  private expression(object: Record<string, unknown>, depth: number, root = false): FilterExpression {
    if (depth > MAX_DEPTH) throw new FilterV2Error("expression depth limit exceeded");
    if (++this.nodes > MAX_NODES) throw new FilterV2Error("expression node limit exceeded");
    const node = object.node;
    const child = (value: unknown) => this.expression(expectObject(value, "child expression"), depth + 1);
    if (node === "column_ref") {
      expectKeys(object, ["node", "column_index", "column_name"], [], "column_ref");
      const columnIndex = expectUint(object.column_index, "column_ref.column_index");
      const columnName = expectString(object.column_name, "column_ref.column_name");
      const field = this.options.outputSchema.fields[columnIndex];
      if (!field || field.name !== columnName) {
        throw new FilterV2Error(`column_ref ${columnName} does not match index ${columnIndex}`);
      }
      validateArrowExtensions(field);
      return { node, columnIndex, columnName, dataType: field.type };
    }
    if (node === "field_ref") {
      expectKeys(object, ["node", "expression", "field_index", "field_name"], [], "field_ref");
      const expression = child(object.expression);
      const parent = expressionType(expression);
      if (!parent || !isStruct(parent)) throw new FilterV2Error("field_ref input must be STRUCT");
      const fieldIndex = expectUint(object.field_index, "field_ref.field_index");
      const fieldName = expectString(object.field_name, "field_ref.field_name");
      const field = (parent as any).children?.[fieldIndex] as VgiField | undefined;
      if (!field || field.name !== fieldName) throw new FilterV2Error("field_ref name/index mismatch");
      return { node, expression, fieldIndex, fieldName, dataType: field.type };
    }
    if (node === "literal") {
      expectKeys(object, ["node", "value_ref"], [], "literal");
      const payload = this.payload("value", object.value_ref);
      validateArrowExtensions(payload.field);
      return { node, valueRef: payload.ref, field: payload.field, value: payload.value };
    }
    if (node === "comparison") {
      expectKeys(object, ["node", "op", "left", "right"], [], "comparison");
      const op = expectString(object.op, "comparison.op") as ComparisonOp;
      if (!Object.values(ComparisonOp).includes(op)) throw new FilterV2Error(`unknown comparison operator ${op}`);
      return { node, op, left: child(object.left), right: child(object.right) };
    }
    if (node === "and" || node === "or") {
      expectKeys(object, ["node", "children"], [], node);
      const children = expectArray(object.children, `${node}.children`).map(child);
      if (children.length < 2 || children.some((value) => !isBooleanExpression(value))) {
        throw new FilterV2Error(`${node} requires at least two Boolean children`);
      }
      return { node, children };
    }
    if (node === "not") {
      expectKeys(object, ["node", "expression"], [], "not");
      const expression = child(object.expression);
      if (!isBooleanExpression(expression)) throw new FilterV2Error("not input must be Boolean");
      return { node, expression };
    }
    if (node === "is_null") {
      expectKeys(object, ["node", "expression", "negated"], [], "is_null");
      return { node, expression: child(object.expression), negated: expectBoolean(object.negated, "is_null.negated") };
    }
    if (node === "in") {
      expectKeys(object, ["node", "expression", "set", "negated"], [], "in");
      const setObject = expectObject(object.set, "in.set");
      const kind = setObject.kind;
      let parsedSet: FilterSet;
      if (kind === "literal") {
        expectKeys(setObject, ["kind", "value_ref"], [], "in.set");
        const payload = this.payload("value", setObject.value_ref);
        validateArrowExtensions(payload.field);
        if (!isList(payload.field.type) || !Array.isArray(payload.value)) {
          throw new FilterV2Error("literal IN payload must be a non-NULL list");
        }
        parsedSet = { kind, valueRef: payload.ref, field: payload.field, values: payload.value };
      } else if (kind === "external") {
        expectKeys(setObject, ["kind", "batch_index", "column_index", "column_name"], [], "in.set");
        const batchIndex = expectUint(setObject.batch_index, "in.set.batch_index");
        const columnIndex = expectUint(setObject.column_index, "in.set.column_index");
        const columnName = expectString(setObject.column_name, "in.set.column_name");
        const batch = this.options.joinKeyBatches?.[batchIndex];
        const field = batch?.schema.fields[columnIndex];
        const column = batch?.getChildAt(columnIndex);
        if (!batch || !field || !column || field.name !== columnName) {
          throw new FilterV2Error("external IN reference is unavailable or mismatched");
        }
        validateArrowExtensions(field);
        const values = Array.from({ length: batch.numRows }, (_, index) => readCanonicalValue(field.type, column, index));
        parsedSet = { kind, batchIndex, columnIndex, columnName, values, batch };
      } else {
        throw new FilterV2Error("unknown IN set kind");
      }
      return { node, expression: child(object.expression), set: parsedSet, negated: expectBoolean(object.negated, "in.negated") };
    }
    if (node === "cast") {
      expectKeys(object, ["node", "expression", "type_ref"], [], "cast");
      const payload = this.payload("type", object.type_ref);
      validateArrowExtensions(payload.field);
      if (payload.value !== null) throw new FilterV2Error("cast type payload must be NULL");
      return { node, expression: child(object.expression), typeRef: payload.ref, field: payload.field };
    }
    if (node === "arithmetic") {
      expectKeys(object, ["node", "op", "left", "right"], [], "arithmetic");
      const op = expectString(object.op, "arithmetic.op");
      if (!["add", "subtract", "multiply", "divide", "modulo"].includes(op)) {
        throw new FilterV2Error(`unknown arithmetic operator ${op}`);
      }
      return { node, op: op as any, left: child(object.left), right: child(object.right) };
    }
    if (node === "negate") {
      expectKeys(object, ["node", "expression"], [], "negate");
      return { node, expression: child(object.expression) };
    }
    if (node === "call") {
      expectKeys(object, ["node", "function", "arguments"], ["options"], "call");
      let fn: StandardFilterFunction | FunctionIdentity;
      if (typeof object.function === "string") {
        if (!STANDARD_FUNCTIONS.has(object.function as StandardFilterFunction)) {
          throw new FilterV2Error(`unknown standard filter function ${object.function}`);
        }
        fn = object.function as StandardFilterFunction;
      } else {
        fn = this.identity(object.function, "call.function");
        const key = identityKey(fn);
        if (!KNOWN_EXTENSION_FUNCTIONS.has(key)) throw new FilterV2Error(`unknown extension function ${key}`);
        if (!(this.options.extensionFunctions ?? []).some((value) => identityKey(value) === key)) {
          throw new FilterV2Error(`extension function ${key} was not advertised`);
        }
      }
      const args = expectArray(object.arguments, "call.arguments");
      if (args.length > MAX_ARGUMENTS) throw new FilterV2Error("call argument limit exceeded");
      if (args.length !== 2) throw new FilterV2Error("registered v2 filter functions require exactly two arguments");
      const parsedArgs = args.map(child);
      if (typeof fn === "string") validateStandardCall(fn, parsedArgs);
      const options = object.options === undefined ? undefined : expectObject(object.options, "call.options");
      if (typeof fn === "string" && options !== undefined) throw new FilterV2Error("standard functions forbid options");
      if (options && Object.keys(options).length) throw new FilterV2Error("extension function options are unsupported");
      return { node, function: fn, arguments: parsedArgs, options };
    }
    if (node === "runtime_filter") {
      expectKeys(object, ["node", "algorithm", "input", "artifact_ref", "null_handling"], [], "runtime_filter");
      if (!root) throw new FilterV2Error("runtime_filter is allowed only at a predicate root");
      const algorithm = this.identity(object.algorithm, "runtime_filter.algorithm");
      const key = identityKey(algorithm);
      if (!KNOWN_RUNTIME_ALGORITHMS.has(key)) throw new FilterV2Error(`unknown runtime filter ${key}`);
      const payload = this.payload("artifact", object.artifact_ref);
      validateArrowExtensions(payload.field);
      const nullHandling = expectString(object.null_handling, "runtime_filter.null_handling");
      if (nullHandling !== "pass" && nullHandling !== "reject") throw new FilterV2Error("invalid runtime null handling");
      const supported = (this.options.runtimeAlgorithms ?? []).some((value) => identityKey(value) === key);
      if (supported) throw new FilterV2Error("runtime algorithm was advertised without an evaluator");
      return {
        node, algorithm, input: child(object.input), artifactRef: payload.ref,
        field: payload.field, artifact: payload.value, nullHandling, supported,
      };
    }
    throw new FilterV2Error(`unknown expression node ${String(node)}`);
  }
}

function expressionType(expression: FilterExpression): VgiDataType | undefined {
  switch (expression.node) {
    case "column_ref": return expression.dataType;
    case "field_ref": return expression.dataType;
    case "literal": return expression.field.type;
    case "cast": return expression.field.type;
    default: return undefined;
  }
}

function validateStandardCall(fn: StandardFilterFunction, args: FilterExpression[]): void {
  const types = args.map(expressionType);
  let matches = false;
  if (fn === "starts_with" || fn === "ends_with" || fn === "contains") {
    matches = types.every((type) => type !== undefined && isUtf8(type));
  } else if (fn === "list_contains") {
    const listType = types[0];
    const needleType = types[1];
    const element = listType && isList(listType) ? (listType as any).children?.[0]?.type as VgiDataType | undefined : undefined;
    matches = element !== undefined && needleType !== undefined && typeSignature(element) === typeSignature(needleType);
  }
  if (!matches) throw new FilterV2Error(`${fn} arguments do not bind under vgi.duckdb.standard.v1`);
}

function isBooleanExpression(expression: FilterExpression): boolean {
  if (["comparison", "and", "or", "not", "is_null", "in", "runtime_filter", "call"].includes(expression.node)) {
    return true;
  }
  return expression.node === "literal" && isBool(expression.field.type);
}

function requiresSessionContext(expression: FilterExpression): boolean {
  switch (expression.node) {
    case "arithmetic":
      return expression.op === "divide" || expression.op === "modulo" ||
        requiresSessionContext(expression.left) || requiresSessionContext(expression.right);
    case "cast":
      const source = expressionType(expression.expression);
      const contextual = (type: VgiDataType | undefined): boolean => !!type &&
        (isUtf8(type) || isDate(type) || isTime(type) || isTimestamp(type));
      return (contextual(source) && contextual(expression.field.type)) || requiresSessionContext(expression.expression);
    case "field_ref": case "not": case "is_null": case "negate":
      return requiresSessionContext(expression.expression);
    case "comparison":
      return requiresSessionContext(expression.left) || requiresSessionContext(expression.right);
    case "and": case "or":
      return expression.children.some(requiresSessionContext);
    case "in":
      return requiresSessionContext(expression.expression);
    case "call":
      return expression.arguments.some(requiresSessionContext);
    case "runtime_filter":
      return requiresSessionContext(expression.input);
    default:
      return false;
  }
}

export function deserializeFilters(batch: VgiBatch, options: DeserializeFilterOptions): PushdownFilters {
  return new FilterParser(batch, options).snapshot();
}

export function applyFilterDelta(prior: PushdownFilters, batch: VgiBatch): PushdownFilters {
  return new FilterParser(batch, prior.options).delta(prior);
}

/** @deprecated External-set references are positional in v2; pass joinKeyBatches instead. */
export function buildJoinKeysLookup(joinKeyBatches: VgiBatch[]): (columnName: string) => unknown[] | null {
  const values = new Map<string, unknown[]>();
  for (const batch of joinKeyBatches) {
    batch.schema.fields.forEach((field, columnIndex) => {
      const column = batch.getChildAt(columnIndex);
      if (!column) return;
      values.set(field.name, Array.from({ length: batch.numRows }, (_, row) => readCanonicalValue(field.type, column, row)));
    });
  }
  return (name) => values.get(name) ?? null;
}
