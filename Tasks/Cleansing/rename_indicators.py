
# Mapping: file prefix -> {old_column_name: new_column_name}
# Only list columns that conflict with other file types.
RENAME_MAP = {
    "bts": {
        "no_signal": "bts_no_signal",
    },
    "cps": {
        "no_signal": "cps_no_signal",
    },
}


def rename_indicators(df, file_prefix):
    mapping = RENAME_MAP.get(file_prefix)
    if not mapping:
        return df

    applicable = {old: new for old, new in mapping.items() if old in df.columns}
    if not applicable:
        return df

    df = df.rename(columns=applicable)
    return df


def validate_no_conflicts(all_dataframes: dict):
    print("\n--- Indicator Column Conflict Validation ---")

    from collections import defaultdict
    indicator_sources = defaultdict(list)

    for prefix, cols in all_dataframes.items():
        for col in cols:
            if col == "id_province":
                continue
            indicator_sources[col].append(prefix)

    conflicts = {col: sources for col, sources in indicator_sources.items() if len(sources) > 1}

    if conflicts:
        print("  [FAIL] Conflicts found:")
        for col, sources in conflicts.items():
            print(f"    '{col}' appears in: {sources}")
    else:
        print("  [OK] All indicator column names are unique across file types.")