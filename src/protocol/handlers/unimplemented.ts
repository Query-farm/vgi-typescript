// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// vgi.v2 methods this SDK registers but does not implement.
//
// "The protocol is the unit of optionality": every SDK hosts EVERY vgi.v2
// method with exactly the reference's schemas, so vgi_rpc.Reflection.v1
// reports one vgi.v2 protocol hash everywhere (see VGI_V2_PROTOCOL_HASH in
// ../dispatch.ts). The registration and the UNIMPLEMENTED default for every
// method are generated (src/generated/vgi-service.ts); this module only names
// the methods whose generated default this worker leaves in place, so a test
// can assert each one refuses with UNIMPLEMENTED.
//
// To implement one, add its handler to a group in this directory; it drops off
// this list by itself.

import { FunctionRegistry } from "../../functions/registry.js";
import {
  UNIMPLEMENTED_VGI_SERVICE,
  VGI_V2_METHODS,
  type VgiMethodSpec,
} from "../../generated/vgi-service.js";
import { buildVgiService } from "../dispatch.js";

export { unimplementedMessage } from "../../generated/vgi-service.js";

const service = buildVgiService({ registry: new FunctionRegistry() });

/** The vgi.v2 methods (of the reference's 72) this SDK leaves at the generated default. */
export const UNIMPLEMENTED_METHODS: readonly VgiMethodSpec[] = VGI_V2_METHODS.filter(
  (m) => service[m.key] === UNIMPLEMENTED_VGI_SERVICE[m.key],
);
