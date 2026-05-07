// Back-compat shim. Arrow utilities have moved to src/arrow/ (the backend
// facade). This file is kept so existing imports of `../util/arrow/index.js`
// continue to work during step 2 of the facade migration. Once all callers
// have switched to `../arrow/index.js`, this file can be deleted.

export {
  emptyBatch,
  batchFromRows,
  batchFromColumns,
  iterRows,
  batchToScalarDict,
  batchToSecretDict,
  safeNumber,
  decodeDictValue,
  filterBatch,
  projectSchema,
  projectBatch,
  serializeSchema,
  deserializeSchema,
  serializeBatch,
  deserializeBatch,
} from "../../arrow/index.js";
