// Copyright 2026 Query Farm LLC - https://query.farm
// The vgi.v3 record codecs and builders a custom catalog (worker side) or a
// client needs, re-exported from one place for every package entry point.

export {
  constraintRecord, readConstraintRecord, validateTableConstraints, validateForeignKeyTargets,
  servedTableSchema, servedTableInfo, servedConstraintFields, validateConstraintNullability,
  selectReadProfiles, selectCatalogCapabilities, catalogCapabilities, constraintCapabilities,
  defaultClientCapabilities,
  CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION, CONSTRAINT_CREATE, CONSTRAINT_MUTATE, CONSTRAINT_ENFORCE, TRANSACTION_DDL,
  type ConstraintRecordSpec, type ConstraintRecordView, type ConstraintKind, type ConstraintEnforcement,
  type ConstraintValidation, type ConstraintTableRef, type ValidatedConstraints,
} from "./constraints.js";
export {
  sqlExpression, generatedExpressionMetadata, defaultExpressionMetadata,
  GENERATED_EXPRESSION_KEY, GENERATION_KEY, DEFAULT_EXPRESSION_KEY,
} from "./column-expressions.js";
export { validateColumnExpressionSql, validateNumericCheckSql } from "./sql-grammar.js";
export { canonicalIndex, emptyIndexOptions, type CanonicalIndexSpec } from "./indexes.js";
export {
  encodeCatalogCapabilities, decodeCatalogCapabilities, buildCatalogCapabilities,
  encodeSqlExpression, decodeSqlExpression,
  encodeCatalogObjectRef, decodeCatalogObjectRef,
  encodeConstraintInfo, decodeConstraintInfo,
  encodeConstraintDefinition, decodeConstraintDefinition, buildConstraintDefinition,
  encodeForeignKeyDefinition, decodeForeignKeyDefinition,
  encodeConstraintChange, decodeConstraintChange, buildConstraintChange,
  encodeConstraintSnapshot, decodeConstraintSnapshot,
  encodeConstraintsApplyRequest, decodeConstraintsApplyRequest,
  encodeIndexDefinition, decodeIndexDefinition,
  encodeMacroOverload, decodeMacroOverload, buildMacroOverload,
  type CatalogCapabilities, type SqlExpression, type CatalogObjectRef, type ConstraintInfo, type ConstraintDefinition,
  type ForeignKeyDefinition, type ConstraintChange, type ConstraintSnapshot, type ConstraintsApplyRequest,
  type IndexDefinition, type MacroOverload, type MacroParameter,
} from "../generated/vgi-protocol-types.js";
export type { ClientCapabilities } from "../generated/vgi-client.js";
