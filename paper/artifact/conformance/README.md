# Differential conformance artifact

This directory keeps semantic evidence separate from API-name coverage and
performance measurements. `generate-cases.ts` writes an exact, language-neutral
JSON corpus. The same corpus is then executed by pandas 3.0.5 and by
`bun_panda` in forced-TypeScript, forced-Wasm, and adaptive modes.

The default corpus contains 200 valid and 50 invalid/boundary cases for each
of ten operation families (2,500 cases total). Every case carries its seed,
input rows, column order, index labels, operation, and arguments. JSON values
that JSON cannot represent directly use tagged objects such as
`{"$number":"NaN"}`.

Run the complete local study with:

```sh
bun run conformance
```

Outputs are written under `paper/data/conformance/`:

- `cases.json`: frozen generated inputs;
- `pandas.json`: pandas 3.0.5 observations;
- `typescript.json`, `wasm.json`, and `adaptive.json`: `bun_panda` observations;
- `summary.json`: denominators and agreement counts; and
- `mismatch-ledger.json`: every disagreement, categorized without suppression.

The harness compares status, output kind, values, row and column labels,
ordering, broad dtype families, exception categories, and mutation state.
Floating values use a declared `1e-9` absolute and relative tolerance. Native
dtype spellings are retained as diagnostics but are not equated across pandas'
NumPy/extension dtype system and `bun_panda`'s smaller dtype vocabulary.

This is a reproducible first behavioral layer, not proof of full pandas
compatibility. MultiIndex, extension dtypes, warnings, categorical metadata,
and timezone semantics remain explicit expansion gates in the Q1 plan.
