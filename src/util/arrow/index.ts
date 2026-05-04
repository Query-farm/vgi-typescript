// Arrow utility helpers — barrel for the split modules under util/arrow/.

export { emptyBatch } from "./empty.js";
export { batchFromRows, batchFromColumns } from "./build.js";
export { iterRows, batchToScalarDict, batchToSecretDict, safeNumber, decodeDictValue } from "./iterate.js";
export { filterBatch } from "./filter.js";
export { projectSchema, projectBatch } from "./project.js";
export { serializeSchema, deserializeSchema, serializeBatch, deserializeBatch } from "./ipc.js";
