#!/usr/bin/env python3
"""
Codemod: rewrite imports of `@query-farm/apache-arrow` to use the facade
at `src/arrow/index.ts`. See plan: step 2 of the facade migration.

Transformations (per file, in order):
  1. `new Schema(...)`/`new Field(...)`/etc.  -> `schema(...)`/`field(...)`/...
  2. `DataType.isX(...)`                       -> `isX(...)`
  3. `instanceof Null`                          -> `// TODO(facade) instanceof Null` (manual)
  4. `: Schema` / `Schema | ...` / `<Schema>`  -> `VgiSchema` / etc. (type refs only)
  5. `import { ... } from "@query-farm/apache-arrow"` -> `import { ... } from "<rel>/arrow/index.js"`

Run from repo root:  python3 scripts/migrate-to-facade.py [path ...]
With no args, processes every src/ file that imports apache-arrow except the
arrow-js implementation (src/arrow/impl-arrowjs/) and the generated schemas
(src/generated/, regenerated upstream).
"""

import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"
ARROW_FACADE = SRC / "arrow" / "index.js"

# class -> factory function name (for `new ClassName(...)` -> `factory(...)`)
CTOR_TO_FACTORY = {
    "Schema": "schema",
    "Field": "field",
    "Null": "nullType",
    "Bool": "bool",
    "Int8": "int8", "Int16": "int16", "Int32": "int32", "Int64": "int64",
    "Uint8": "uint8", "Uint16": "uint16", "Uint32": "uint32", "Uint64": "uint64",
    "Float16": "float16", "Float32": "float32", "Float64": "float64",
    "Utf8": "utf8",
    "Binary": "binary",
    "FixedSizeBinary": "fixedSizeBinary",
    "Decimal": "decimal",
    "Date_": "date",
    "Time": "time",
    "Timestamp": "timestamp",
    "Duration": "duration",
    "Interval": "interval",
    "List": "list",
    "FixedSizeList": "fixedSizeList",
    "Struct": "struct",
    "Map_": "map",
    "Dictionary": "dictionary",
    "SparseUnion": "sparseUnion",
    "DenseUnion": "denseUnion",
}

# class -> VgiX type name (for type-position references). Primitive type
# classes (Utf8, Int32, etc.) collapse to the generic `VgiDataType` since
# the facade doesn't expose per-primitive type aliases.
TYPE_RENAMES = {
    "Schema": "VgiSchema",
    "RecordBatch": "VgiBatch",
    "DataType": "VgiDataType",
    "Field": "VgiField",
    "Vector": "VgiColumn",
    # Primitive types -> VgiDataType when used as type annotations
    "Null": "VgiDataType",
    "Bool": "VgiDataType",
    "Int8": "VgiDataType", "Int16": "VgiDataType", "Int32": "VgiDataType", "Int64": "VgiDataType",
    "Uint8": "VgiDataType", "Uint16": "VgiDataType", "Uint32": "VgiDataType", "Uint64": "VgiDataType",
    "Float16": "VgiDataType", "Float32": "VgiDataType", "Float64": "VgiDataType",
    "Utf8": "VgiDataType", "Binary": "VgiDataType",
    "FixedSizeBinary": "VgiDataType", "FixedSizeList": "VgiDataType",
    "Decimal": "VgiDataType",
    "Date_": "VgiDataType", "Time": "VgiDataType",
    "Timestamp": "VgiDataType", "Duration": "VgiDataType",
    "Interval": "VgiDataType",
    "List": "VgiDataType", "Struct": "VgiDataType", "Map_": "VgiDataType",
    "Dictionary": "VgiDataType",
    "SparseUnion": "VgiDataType", "DenseUnion": "VgiDataType",
}

# Constants/enums passed through as-is from the facade
PASSTHROUGH = {"TimeUnit", "DateUnit", "IntervalUnit", "UnionMode", "Type"}

# Low-level arrow-js APIs without facade equivalents. Files that use these
# stay on `@query-farm/apache-arrow` for now and get manually ported when
# the flechette backend lands (step 3 of the facade migration).
ARROWJS_ONLY = {
    "makeData", "vectorFromArray", "Data", "Vector",
    "RecordBatchReader", "RecordBatchStreamWriter",
    "Visitor", "Builder", "AsyncByteQueue",
}

# Predicates: DataType.isX -> isX
PREDICATE_RE = re.compile(r"\bDataType\.is([A-Z][a-zA-Z0-9]*)\b")
INSTANCEOF_RE = re.compile(r"\binstanceof\s+([A-Z][a-zA-Z0-9_]*)\b")
IMPORT_RE = re.compile(
    r'^(?P<lead>(?:import\s+type\s+|import\s+))\{(?P<names>[^}]*)\}\s+from\s+["\']@query-farm/apache-arrow["\'];?\s*$',
    re.MULTILINE,
)
# `new ClassName(` -> factory call. Class names can contain digits (Utf8, Int32).
NEW_RE = re.compile(r"\bnew\s+([A-Z][A-Za-z0-9_]*)\(")
# Type reference word boundary
def make_type_re(name: str) -> re.Pattern:
    # Match the bare class name only when used as a type (not preceded by
    # `new ` / `instanceof ` / `.` / `import` and not followed by `.` / `(`).
    return re.compile(
        rf"(?<![A-Za-z_0-9.])(?<!new )(?<!instanceof )\b{name}\b(?![A-Za-z_0-9])(?!\()"
    )


def relpath_to_facade(file_path: Path) -> str:
    rel = os.path.relpath(ARROW_FACADE, file_path.parent)
    # ensure POSIX separators and explicit ./
    rel = rel.replace(os.sep, "/")
    if not rel.startswith("."):
        rel = "./" + rel
    return rel


def collect_imported_names(content: str) -> set[str]:
    names: set[str] = set()
    for m in IMPORT_RE.finditer(content):
        for raw in m.group("names").split(","):
            raw = raw.strip()
            if not raw:
                continue
            if raw.startswith("type "):
                raw = raw[len("type ") :].strip()
            # alias: `Foo as Bar`
            raw = raw.split(" as ")[0].strip()
            names.add(raw)
    return names


# Detect local-variable shadowing for facade factory names. If `field` (or
# `schema`/`struct`/etc.) appears as a const/let/parameter name in the file,
# the factory import must be aliased to avoid ambiguity. Heuristic-only —
# false positives are fine because aliasing is always safe.
SHADOW_PATTERNS = re.compile(
    r"(?:\b(?:const|let|var)\s+|"             # `const foo`
    r"\((?:[^)]*?,\s*)?|"                      # function param list
    r"\b(?:for\s*\(\s*(?:const|let|var)\s+))" # `for (const foo`
    r"(field|schema|struct|list|map|union|decimal|timestamp|duration|"
    r"dictionary|time|date|interval|binary|utf8|bool|int|float)"
    r"\b\s*[:=,)]"
)

def find_shadowed_factories(content: str) -> set[str]:
    return {m.group(1) for m in SHADOW_PATTERNS.finditer(content)}


# Tokenize comments/strings so type renames can skip them. We replace each
# masked region with a placeholder, do substitutions on the rest, then
# restore.
_MASK_RE = re.compile(
    r"//[^\n]*|"            # line comment
    r"/\*.*?\*/|"           # block comment
    r"`(?:\\.|[^`\\])*`|"   # template literal
    r"\"(?:\\.|[^\"\\])*\"",  # double-quoted string
    # NB: single-quoted strings intentionally NOT masked. JS regex literals
    # (e.g. `/'/`) can contain a `'` that the SQ pattern would treat as the
    # opening of a string and then greedily consume code (including
    # backticks) until the next `'`. The type names we rename (Schema, Utf8,
    # etc.) rarely appear inside single-quoted string literals in this
    # codebase — verified by inspection.
    re.DOTALL,
)


def _rename_outside_comments_and_strings(
    text: str, renames: dict[str, str]
) -> str:
    """Apply type-name renames only outside comments/strings."""
    # Mask comments and strings out, do the rename on what's left, restore.
    masked: list[str] = []

    def stash(m: re.Match) -> str:
        masked.append(m.group(0))
        return f"\x00MASK{len(masked) - 1}\x00"

    code = _MASK_RE.sub(stash, text)
    for cls, vgi in renames.items():
        code = make_type_re(cls).sub(vgi, code)

    def unstash(m: re.Match) -> str:
        return masked[int(m.group(1))]

    return re.sub(r"\x00MASK(\d+)\x00", unstash, code)


def rewrite_imports(
    content: str, file_path: Path, used_factories: set[str],
    factory_alias: dict[str, str] | None = None,
) -> str:
    """Replace each `from "@query-farm/apache-arrow"` line with a facade import,
    keeping any ARROWJS_ONLY symbols on a separate arrow-js import line.
    `factory_alias` maps factory name -> aliased name (e.g. `field` -> `field_`)
    when a local variable shadows the factory in this file.
    """
    rel = relpath_to_facade(file_path)
    factory_alias = factory_alias or {}

    def factory_import_form(name: str) -> str:
        """Render a factory name in import-list form, applying alias if any."""
        alias = factory_alias.get(name)
        return f"{name} as {alias}" if alias else name

    def replace(match: re.Match) -> str:
        lead = match.group("lead").strip()
        is_type_only_import = lead.startswith("import type")

        facade_names: list[str] = []
        arrowjs_names: list[str] = []  # symbols staying on @query-farm/apache-arrow

        for raw in match.group("names").split(","):
            raw = raw.strip()
            if not raw:
                continue
            type_prefix = ""
            if raw.startswith("type "):
                type_prefix = "type "
                raw = raw[len("type ") :].strip()
            alias_suffix = ""
            if " as " in raw:
                raw, alias = raw.split(" as ", 1)
                alias_suffix = f" as {alias.strip()}"
                raw = raw.strip()

            if raw in ARROWJS_ONLY:
                arrowjs_names.append(f"{type_prefix}{raw}{alias_suffix}")
            elif raw in TYPE_RENAMES:
                facade_names.append(f"type {TYPE_RENAMES[raw]}{alias_suffix}")
                factory = CTOR_TO_FACTORY.get(raw)
                if factory and factory in used_factories:
                    facade_names.append(factory_import_form(factory))
            elif raw in CTOR_TO_FACTORY:
                fac = CTOR_TO_FACTORY[raw]
                facade_names.append(factory_import_form(fac) + alias_suffix)
            elif raw in PASSTHROUGH:
                facade_names.append(raw)
            else:
                # Unknown -- keep on arrow-js so tsc surfaces the issue
                arrowjs_names.append(f"{type_prefix}{raw}{alias_suffix}")

        def dedupe(xs: list[str]) -> list[str]:
            seen, out = set(), []
            for x in xs:
                if x not in seen:
                    seen.add(x); out.append(x)
            return out
        facade_names = dedupe(facade_names)
        arrowjs_names = dedupe(arrowjs_names)

        lines = []
        if facade_names:
            joined = ", ".join(facade_names)
            if is_type_only_import:
                lines.append(f'import type {{ {joined.replace("type ", "")} }} from "{rel}";')
            else:
                lines.append(f'import {{ {joined} }} from "{rel}";')
        if arrowjs_names:
            joined = ", ".join(arrowjs_names)
            if is_type_only_import:
                lines.append(f'import type {{ {joined.replace("type ", "")} }} from "@query-farm/apache-arrow";')
            else:
                lines.append(f'import {{ {joined} }} from "@query-farm/apache-arrow";')
        return "\n".join(lines)

    return IMPORT_RE.sub(replace, content)


def process_file(path: Path) -> tuple[bool, list[str]]:
    """Returns (changed, warnings)."""
    text = path.read_text()
    original = text
    warnings: list[str] = []

    if "@query-farm/apache-arrow" not in text:
        return False, []

    imported = collect_imported_names(text)
    # Per-file factory aliasing to avoid shadowing local `field`/`schema`/etc.
    shadowed = find_shadowed_factories(text)
    factory_alias = {name: f"{name}_" for name in shadowed}

    used_factories: set[str] = set()

    # 1. new ClassName( -> factory( (using alias if shadowed)
    def replace_new(m: re.Match) -> str:
        cls = m.group(1)
        factory = CTOR_TO_FACTORY.get(cls)
        if factory:
            used_factories.add(factory)
            return f"{factory_alias.get(factory, factory)}("
        return m.group(0)
    text = NEW_RE.sub(replace_new, text)

    # 2. DataType.isX -> isX
    pred_calls: set[str] = set()
    def replace_pred(m: re.Match) -> str:
        pred_calls.add(f"is{m.group(1)}")
        return f"is{m.group(1)}"
    text = PREDICATE_RE.sub(replace_pred, text)

    # 3. instanceof X — flag for manual review
    for m in INSTANCEOF_RE.finditer(text):
        cls = m.group(1)
        if cls in CTOR_TO_FACTORY or cls in TYPE_RENAMES:
            warnings.append(
                f"  {path}: `instanceof {cls}` needs manual conversion to `is{cls}(value)`"
            )

    # 4. Imports — must run BEFORE type renames so the original class names
    # in the import line are still present and parseable.
    text = rewrite_imports(text, path, used_factories, factory_alias)

    # 5. Type references: Schema -> VgiSchema (only outside `new`/`instanceof`/`.`)
    # Skip comments and string literals so we don't mangle prose / SQL.
    text = _rename_outside_comments_and_strings(text, TYPE_RENAMES)

    # If predicates were used, ensure they're imported.
    if pred_calls:
        # find the facade import we just wrote
        facade_path_re = re.compile(
            r'^(import(?:\s+type)?\s+\{[^}]*\}\s+from\s+["\'][^"\']*arrow/index\.js["\'];?)\s*$',
            re.MULTILINE,
        )
        m = facade_path_re.search(text)
        if m:
            line = m.group(1)
            # Insert predicate names if not already present
            existing = {n.strip().lstrip("type ").split(" as ")[0].strip()
                        for n in re.search(r"\{([^}]*)\}", line).group(1).split(",")}
            new_preds = sorted(p for p in pred_calls if p not in existing)
            if new_preds:
                replacement = re.sub(
                    r"\{([^}]*)\}",
                    lambda mm: "{ " + mm.group(1).strip().rstrip(",") + ", " + ", ".join(new_preds) + " }",
                    line,
                    count=1,
                )
                text = text.replace(line, replacement)
        else:
            # No facade import yet (file used only DataType.isX as a static).
            # Insert a fresh import line at top after first existing import.
            rel = relpath_to_facade(path)
            preds = ", ".join(sorted(pred_calls))
            text = re.sub(
                r"^(\s*import[^\n]*\n)",
                lambda mm: mm.group(1) + f'import {{ {preds} }} from "{rel}";\n',
                text,
                count=1,
            )

    if text != original:
        path.write_text(text)
        return True, warnings
    return False, warnings


def main() -> int:
    args = sys.argv[1:]
    if args:
        files = [Path(a).resolve() for a in args]
    else:
        files = sorted(
            p for p in SRC.rglob("*.ts")
            if "@query-farm/apache-arrow" in p.read_text()
            and "/arrow/impl-arrowjs/" not in str(p)
            and "/generated/" not in str(p)
        )

    changed = 0
    all_warnings: list[str] = []
    for f in files:
        c, w = process_file(f)
        if c:
            changed += 1
            print(f"  ✓ {f.relative_to(REPO_ROOT)}")
        all_warnings.extend(w)

    print(f"\n{changed}/{len(files)} files changed.")
    if all_warnings:
        print("\nWarnings (manual review needed):")
        for w in all_warnings:
            print(w)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
