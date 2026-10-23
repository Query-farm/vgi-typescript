// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Continued output (table-in-out exchange).
//
// A table-in-out exchange answers each input batch with one reply. A function
// can answer one input with several: it emits each reply but the last with
// `moreOutputMetadata(true)`, which puts `vgi.status: HAVE_MORE_OUTPUT` on the
// reply. The client then asks for the next reply with a continuation tick -- a
// zero-row batch of the input schema carrying `vgi.status: CONTINUE` -- and the
// framework answers it by calling the function's `resume()`. Every reply's
// parent rows index the last real (non-tick) input batch.
//
// `resume()` gets no input, so the function's state must hold where to resume.
// Over HTTP that state reaches the next turn only inside the stream's state
// token, so keep it small: positions to resume from, never buffered output rows.
//
// The protocol is strict, with no capability negotiation: while output is
// pending a tick is the only valid input, and a tick is valid only then. Cache
// control may only ride an input's first reply, and "more" is refused outside a
// table-in-out exchange. Mirrors vgi-python's `emit(more=True)` / `resume()`.

import type { OutputCollector } from "@query-farm/vgi-rpc";
import type { VgiBatch } from "../arrow/index.js";

/** Metadata key carrying a table-in-out exchange status, on the batch itself. */
export const STATUS_KEY = "vgi.status";
/** Reply status: this input has more output; ask for it with a tick. */
export const HAVE_MORE_OUTPUT = "HAVE_MORE_OUTPUT";
/** Input status: a continuation tick, asking for the last input's next reply. */
export const CONTINUE = "CONTINUE";

const CACHE_KEY_PREFIX = "vgi.cache.";

/**
 * Mark an emitted table-in-out reply as one of several for the same input
 * batch when `more` is true: the client asks for the next one with a
 * continuation tick, which the framework answers through the function's
 * `resume()`. `false` adds nothing, so the last reply can pass its own
 * condition. Composes with {@link parentRowsMetadata} and
 * `cacheControlMetadata` via `extra`:
 *
 * ```ts
 * out.emit(batch, moreOutputMetadata(state.row < state.counts.length,
 *   parentRowsMetadata(parents, batch.numRows)));
 * ```
 *
 * Valid only from the `process()` or `resume()` of a table-in-out function
 * that implements `resume()`; the framework reads it off the emitted reply.
 */
export function moreOutputMetadata(more: boolean, extra?: Map<string, string>): Map<string, string> {
  const md = new Map<string, string>(extra ?? []);
  if (more) md.set(STATUS_KEY, HAVE_MORE_OUTPUT);
  return md;
}

/** The metadata the turn's data batch went out with (per-emit over batch-carried). */
function replyMetadata(out: OutputCollector): Map<string, string> | null {
  const idx = out.dataBatchIdx;
  if (idx == null) return null;
  const emitted = out.batches[idx];
  const md = new Map<string, string>((emitted.batch as any).metadata ?? []);
  for (const [k, v] of emitted.metadata ?? []) md.set(k, v);
  return md;
}

/**
 * Refuse "more" on a stream that cannot continue: anything but a table-in-out
 * exchange, where the client would never send the tick. Run by the dispatcher
 * after every non-table-in-out turn.
 */
export function refuseMoreOutput(name: string, out: OutputCollector): void {
  if (replyMetadata(out)?.get(STATUS_KEY) === HAVE_MORE_OUTPUT) {
    throw new Error(
      `${name}: HAVE_MORE_OUTPUT (moreOutputMetadata) is only valid in a table-in-out ` +
        `function's process() or resume()`,
    );
  }
}

/** The exchange state a continuable table-in-out turn reads and updates. */
export interface ContinuationState {
  /**
   * Whether the last reply said HAVE_MORE_OUTPUT, so the next input must be a
   * tick. Carried in the HTTP state token (see protocol/state-serializer.ts).
   */
  morePending?: boolean;
}

/**
 * Run one table-in-out exchange turn under the continued-output rules.
 *
 * Classifies the input (a tick or real input), refuses the inputs the protocol
 * forbids, calls `run(tick)` -- which calls `process()` for real input and
 * `resume()` for a tick -- and records from the emitted reply whether output is
 * now pending.
 *
 * `resumable` is whether the function implements `resume()`: one that does not
 * may not say more.
 */
export async function continuationTurn(
  name: string,
  eState: ContinuationState,
  input: VgiBatch,
  out: OutputCollector,
  resumable: boolean,
  run: (tick: boolean) => Promise<void>,
): Promise<void> {
  // The status rides the input batch's own metadata; vgi-rpc also surfaces it
  // on the collector, so read either.
  const inputStatus =
    ((input as any)?.metadata as Map<string, string> | undefined)?.get(STATUS_KEY) ??
    out.inputMetadata?.get(STATUS_KEY);
  const tick = inputStatus === CONTINUE;
  if (tick && !eState.morePending) {
    throw new Error(`${name}: continuation tick received, but no output is pending`);
  }
  if (tick && input.numRows !== 0) {
    throw new Error(`${name}: a continuation tick must have no rows, got ${input.numRows}`);
  }
  if (!tick && eState.morePending) {
    throw new Error(`${name}: input received while output is pending; expected a continuation tick`);
  }

  await run(tick);

  const md = replyMetadata(out);
  const more = md?.get(STATUS_KEY) === HAVE_MORE_OUTPUT;
  if (more && !resumable) {
    throw new Error(`${name}: emitted HAVE_MORE_OUTPUT but does not implement resume()`);
  }
  if (tick) {
    // Every tick must be answered: an empty HTTP body or end-of-stream in
    // answer to one is a protocol violation.
    if (md == null) throw new Error(`${name}: resume() must emit a batch for every continuation tick`);
    for (const key of md.keys()) {
      if (key.startsWith(CACHE_KEY_PREFIX)) {
        throw new Error(
          `${name}: cache control is only valid on an input's first reply, ` +
            `not on one answering a continuation tick (${key})`,
        );
      }
    }
  }
  eState.morePending = more;
}

/**
 * Refuse, at definition, a continuable function the client cannot drive.
 *
 * `resume()` continues from the function's state alone, so it needs an
 * `initialState` (the state is what the HTTP token carries between turns).
 * `maxWorkers: 1` runs the function on one connection shared by every
 * pipeline, which cannot leave output pending while another's input waits.
 */
export function checkResumeContract(
  name: string,
  config: { resume?: unknown; initialState?: unknown; maxWorkers?: number },
): void {
  if (config.resume == null) return;
  if (typeof config.resume !== "function") {
    throw new Error(`${name}: resume must be a function`);
  }
  if (config.maxWorkers === 1) {
    throw new Error(
      `${name}: resume() (continued output) is not supported with maxWorkers: 1; ` +
        `the client runs such a function on one shared connection.`,
    );
  }
  if (config.initialState == null) {
    throw new Error(
      `${name}: resume() continues from its state alone, so the function must ` +
        `declare an initialState holding where to resume.`,
    );
  }
}

/**
 * Check `inputRowsPerCall`: a positive integer, on a blended row-transform
 * function only (the extension refuses it at bind too).
 */
export function checkInputRowsPerCall(name: string, value: unknown, blended: boolean): number | undefined {
  if (value == null) return undefined;
  if (!blended) {
    throw new Error(
      `${name}: inputRowsPerCall applies only to a blended row-transform function, ` +
        `whose per-row input the client can split into calls`,
    );
  }
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${name}: inputRowsPerCall must be a positive integer, got ${String(value)}`);
  }
  return value;
}
