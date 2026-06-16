# Migration guide

## 0.1.x → 0.2.0 — the type-handling break

A pre-1.0 breaking change standardizes how columnar values are represented as JS
values in and out of every function. The representation is now uniform across both
Arrow backends (arrow-js for Node/Bun, flechette for Workers/browser) and symmetric
across reads and writes: a value read from a column rebuilds into the same column.

See the **Type representations** section of the [README](./README.md) for the full
per-type table and the typed author API.

### What changed for consumers

- **`date32` / `date64` columns are now JS `Date` in *and* out by default.**
  Previously dates were inconsistent — a day-number went *in* but a `Date` came back
  *out*. Both directions are now `Date` under the default `rich` representation.
- **Reads return rich values.** `iterRows`, scalar `compute` inputs, and
  setting/secret reads all surface the `rich` value for their column type.
- **Non-date temporal types are lossless `bigint` raw units.** `time64`,
  `timestamp[s/ms/us/ns]`, and `duration[s/ms/us/ns]` are the exact `bigint` count in
  their declared unit — never a `Date`, never narrowed, no precision loss.
- **Decimals are unscaled `bigint`.** A `decimal(18, 2)` value of `123.45` is the
  bigint `12345n`. Apply the scale yourself; the precision/scale travel with the
  column type.
- **Codecs validate and throw.** Invalid or lossy input (non-integer where an integer
  is required, a `bigint` that overflows the declared width or the safe-integer range,
  an out-of-range `Date`, the wrong byte count for a `fixedSizeBinary`) raises a clear
  `codec[<type>]: …` `TypeError` at build time instead of corrupting the wire data.
- **Opt into `repr: 'raw'`** on `defineScalarFunction` for branded, unit-tagged raw
  units everywhere. In raw mode `date32`/`date64` are the plain day-number /
  ms-`bigint` (branded `Date32` / `Date64Ms`) rather than a `Date`.

### The common case: dates

```ts
// BEFORE (old, inconsistent): wrote a day-number, read back a Date.
returns: dateDay,
compute: () => [20000],                 // 20000 days since epoch

// AFTER (rich, default): write a Date, read a Date — symmetric.
returns: dateDay,
compute: () => [new Date("2024-10-19")],

// AFTER (raw): opt in to the branded day-number.
returns: dateDay,
repr: "raw",
compute: () => [asDate32(20000)],       // branded number, not a Date
```

### Checklist

1. Find every `date32` / `date64` column you write from a function and change
   day-numbers / ms-integers to `Date` (or set `repr: 'raw'` and wrap with
   `asDate32` / `asDate64Ms`).
2. Confirm `timestamp` / `time64` / `duration` producers emit `bigint` in the
   declared unit, and consumers read `bigint` (not `Date`).
3. Confirm decimal producers emit the **unscaled** integer as a `bigint`.
4. Run your tests — codec validation now throws on values it previously coerced.
