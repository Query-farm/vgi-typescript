// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import {
  schema,
  serializeSchema,
  deserializeBatch,
  type VgiSchema,
} from "../arrow/index.js";
import { validateEncoded } from "../catalog/column-expressions.js";
import { decodeSqlExpression } from "../generated/vgi-protocol-types.js";
import { batchToScalarDict } from "../util/arrow/index.js";
import type { BindRequest, BindResponse } from "../protocol/types.js";
import {
  normalizeBindRepeatability,
  parseBindRepeatability,
} from "../protocol/serializers/bind.js";
export const SCALAR_PROJECTION_PROFILE = "vgi.expression.duckdb.scalar.v1";
export interface ProjectionProposal {
  bindCall: BindRequest;
  bindResult: BindResponse;
  expressions: Uint8Array[];
  resultSchema: VgiSchema;
}
export interface ProjectionAcceptance {
  acceptedIndices: number[];
  bindResult: BindResponse | null;
}
/** Strict envelope boundary for the projection RPC and its original bind records. */
export function projectionRecord(
  bytes: Uint8Array,
  expected: VgiSchema,
): Record<string, any> {
  if (!bytes.length || bytes.length > 8 * 1024 * 1024)
    throw new Error("invalid projection record size");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let offset = 0,
    schemas = 0,
    batches = 0;
  while (offset < bytes.length) {
    if (bytes.length - offset < 4)
      throw new Error("truncated projection IPC prefix");
    const prefix = view.getInt32(offset, true) === -1 ? 8 : 4;
    if (bytes.length - offset < prefix)
      throw new Error("truncated projection IPC prefix");
    const length = view.getInt32(offset + prefix - 4, true);
    offset += prefix;
    if (length === 0) {
      break;
    }
    if (length < 0 || offset + length > bytes.length)
      throw new Error("invalid projection IPC message");
    const table = offset + view.getUint32(offset, true);
    const vtable = table - view.getInt32(table, true);
    const slot = (id: number) =>
      4 + id * 2 < view.getUint16(vtable, true)
        ? view.getUint16(vtable + 4 + id * 2, true)
        : 0;
    const kindSlot = slot(1),
      bodySlot = slot(3);
    const kind = kindSlot ? view.getUint8(table + kindSlot) : 0;
    const body = bodySlot
      ? Number(view.getBigInt64(table + bodySlot, true))
      : 0;
    if (
      body < 0 ||
      body > bytes.length - offset - length ||
      !Number.isSafeInteger(body)
    )
      throw new Error("invalid projection IPC body");
    if (kind === 1 && schemas === 0 && batches === 0) schemas++;
    else if (kind === 2 && schemas === 1 && batches === 0) {
      /* dictionary before record */
    } else if (kind === 3 && schemas === 1 && batches === 0) batches++;
    else throw new Error("projection requires one complete IPC batch");
    offset += length + body;
  }
  // Arrow permits physical EOF after a complete message, with or without EOS.
  if (offset !== bytes.length || schemas !== 1 || batches !== 1)
    throw new Error("projection requires one complete IPC batch");
  const batch = deserializeBatch(bytes);
  // IPC dictionary ids are stream-local and may be reassigned on re-encoding.
  // Compare the exact Arrow shape and metadata instead of those transport ids.
  const metadata = (m: ReadonlyMap<string, string>) =>
    [...m].sort(([a], [b]) => a.localeCompare(b));
  const fieldShape = (f: any): unknown => [
    f.name,
    f.nullable,
    String(f.type),
    metadata(f.metadata),
    (f.type.children ?? []).map(fieldShape),
  ];
  const shape = (s: VgiSchema) =>
    JSON.stringify([s.fields.map(fieldShape), metadata(s.metadata)]);
  if (batch.numRows !== 1 || shape(batch.schema) !== shape(expected))
    throw new Error("invalid projection record schema/row count");
  for (let i = 0; i < expected.fields.length; i++)
    if (!expected.fields[i]!.nullable && batch.getChildAt(i)?.get(0) == null)
      throw new Error("required projection field is NULL");
  return batchToScalarDict(batch);
}
export function validateProjectionOffer(result: BindResponse): void {
  const profiles = result.projection_expression_profiles ?? [];
  if (
    profiles.length > 64 ||
    new Set(profiles).size !== profiles.length ||
    profiles.some(
      (p) =>
        typeof p !== "string" ||
        p.length > 256 ||
        !/^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.v[1-9][0-9]*$/.test(p),
    )
  )
    throw new Error("unsupported projection profile offer");
}
export function validateProjectionProposal(p: ProjectionProposal): void {
  parseBindRepeatability(p.bindResult.repeatability);
  validateProjectionOffer(p.bindResult);
  if (
    p.bindCall.function_type !== "table" ||
    p.bindCall.input_schema !== null ||
    p.bindCall.schema_path == null
  )
    throw new Error("projection requires an ordinary table and explicit owner");
  if (
    [
      p.bindResult.lookup_secret_types,
      p.bindResult.lookup_scopes,
      p.bindResult.lookup_names,
    ].some((v) => v?.length)
  )
    throw new Error("projection cannot request secrets");
  if (
    p.expressions.length < 1 ||
    p.expressions.length > 1024 ||
    p.resultSchema.fields.length !== p.expressions.length ||
    p.resultSchema.metadata.size
  )
    throw new Error("invalid projection result schema/count");
  const names = [
    ...p.bindResult.output_schema.fields,
    ...p.resultSchema.fields,
  ].map((f) => f.name.replace(/[A-Z]/g, (c) => c.toLowerCase()));
  if (
    names.some((n) => !n || n.includes("\0")) ||
    new Set(names).size !== names.length
  )
    throw new Error("ambiguous projection column names");
  let context: string | undefined;
  for (const raw of p.expressions) {
    validateEncoded(
      btoa(Array.from(raw, (b) => String.fromCharCode(b)).join("")),
      p.bindCall.schema_path,
    );
    const expression = decodeSqlExpression(raw);
    if (
      !(p.bindResult.projection_expression_profiles ?? []).includes(
        expression.profile,
      )
    )
      throw new Error("projection profile was not offered");
    const next = JSON.stringify(expression.evaluation_context);
    if (context !== undefined && next !== context)
      throw new Error("projection contexts differ");
    context = next;
  }
}
export function validateProjectionAcceptance(
  p: ProjectionProposal,
  r: ProjectionAcceptance,
): void {
  validateProjectionProposal(p);
  if ((r.acceptedIndices.length === 0) !== (r.bindResult === null))
    throw new Error("projection indices and bind result must occur together");
  let previous = -1;
  for (const i of r.acceptedIndices) {
    if (!Number.isSafeInteger(i) || i <= previous || i >= p.expressions.length)
      throw new Error("invalid accepted projection index");
    previous = i;
  }
  if (!r.bindResult) return;
  validateProjectionOffer(r.bindResult);
  if (
    normalizeBindRepeatability(r.bindResult.repeatability) !==
    p.bindResult.repeatability
  )
    throw new Error("accepted projection changed bind repeatability");
  if (
    JSON.stringify(r.bindResult.projection_expression_profiles ?? []) !==
      JSON.stringify(p.bindResult.projection_expression_profiles ?? []) ||
    [
      r.bindResult.lookup_secret_types,
      r.bindResult.lookup_scopes,
      r.bindResult.lookup_names,
    ].some((v) => v?.length)
  )
    throw new Error("accepted projection changed bind offer/secrets");
  const expected = schema(
    [
      ...p.bindResult.output_schema.fields,
      ...r.acceptedIndices.map((i) => p.resultSchema.fields[i]),
    ],
    p.bindResult.output_schema.metadata,
  );
  const a = serializeSchema(expected),
    b = serializeSchema(r.bindResult.output_schema);
  if (a.length !== b.length || a.some((v, i) => v !== b[i]))
    throw new Error("accepted projection changed append-only schema");
}
