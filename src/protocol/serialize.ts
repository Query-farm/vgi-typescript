// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Arrow serialization/deserialization for VGI protocol types.
// Barrel re-export — implementations live in serializers/. The wire format
// must match Python's ArrowSerializableDataclass output (identical field
// names, types, and metadata).

export { serializeArguments, deserializeArguments } from "./serializers/arguments.js";
export {
  serializeBindRequest,
  deserializeBindRequest,
  serializeBindResponse,
  deserializeBindResponse,
} from "./serializers/bind.js";
export {
  serializeInitRequest,
  deserializeInitRequest,
  serializeGlobalInitResponse,
  deserializeGlobalInitResponse,
} from "./serializers/init.js";
export {
  deserializeCardinalityRequest,
  serializeTableCardinality,
} from "./serializers/cardinality.js";
