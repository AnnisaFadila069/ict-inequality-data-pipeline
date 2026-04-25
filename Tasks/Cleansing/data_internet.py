import re
import numpy as np
import pandas as pd


# Files that use integer-based values (signal strength counts, etc.)
# These need comma/dot removed entirely rather than being treated as decimal separators.
INTEGER_VALUE_FILES = {"ifc", "bts", "cps"}

# Indonesian words found in bilingual BPS column headers.
# Shared loanwords (media, online, digital, email) are excluded intentionally
# so they do not act as stop-words during English extraction.
INDONESIAN_WORDS = {
    "ada", "tidak", "yang", "ke", "dari", "dalam",
    "perkotaan", "perdesaan", "provinsi", "rumah",
    "sinyal", "kuat", "lemah", "sangat", "berfungsi", "jarang", "atas",
    "sendiri", "bukan", "kantor", "sekolah", "tempat", "umum",
    "kendaraan", "bergerak",
    "mendapat", "informasi", "berita", "mengenai", "barang", "jasa",
    "mengirim", "menerima", "sosial",
    "pembelian", "penjualan", "fasilitas", "finansial",
    "pembelajaran", "bekerja", "hiburan", "pembuatan", "konten", "lainnya",
    "sd", "smp", "sma", "d1-d3",
}


def _is_indonesian_word(word):
    return word.lower().strip() in INDONESIAN_WORDS


def _extract_english(raw_col):
    """
    Extract the English portion from a bilingual Indonesian/English column name.
    BPS headers follow the pattern: [Indonesian phrase] [English phrase].
    Scans right-to-left; stops when an Indonesian word is encountered.
    Duplicate words from bilingual overlap are resolved by keeping the last occurrence.
    """
    space_tokens = raw_col.strip().split()
    english_words = []

    for token in reversed(space_tokens):
        sub_tokens = [t for t in re.split(r"[/+]", token) if t.strip()]
        id_subs = [t for t in sub_tokens if _is_indonesian_word(t)]
        en_subs = [t for t in sub_tokens if not _is_indonesian_word(t)]

        if not id_subs:
            for w in reversed(en_subs):
                english_words.insert(0, w)
        elif not en_subs:
            break
        else:
            for w in reversed(en_subs):
                english_words.insert(0, w)
            break

    if not english_words:
        return raw_col

    # Keep last occurrence to resolve bilingual word overlap
    seen = set()
    unique_words = []
    for word in reversed(english_words):
        if word.lower() not in seen:
            seen.add(word.lower())
            unique_words.insert(0, word)

    return " ".join(unique_words)


def _to_snake_case(col):
    col = str(col).strip().lower()
    col = re.sub(r"[^\w\s]", "", col)
    col = re.sub(r"\s+", "_", col)
    return col


def _find_province_col(df):
    for col in df.columns:
        if "province" in col:
            return col
    return None


def _get_file_prefix(filename):
    """Return base prefix of filename, e.g. 'bts_rural.xlsx' -> 'bts'."""
    return filename.lower().split("_")[0].split(".")[0]


def clean(df, filename):
    """
    Clean a single internet indicator Excel file (raw format from BPS).

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame read with header=None.
    filename : str
        Base filename (e.g. 'ail_rural.xlsx'), used to select cleaning mode.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with id_province as first column.
    """
    is_integer_file = _get_file_prefix(filename) in INTEGER_VALUE_FILES

    # 1. Set header from row index 1, drop rows 0-2, reset index
    df.columns = df.iloc[1]
    df = df.drop([0, 1, 2]).reset_index(drop=True)

    # 2. Extract English portion of each column name, then snake_case
    df.columns = [_to_snake_case(_extract_english(str(col))) for col in df.columns]

    # 3. Locate the province column
    prov_col = _find_province_col(df)
    if prov_col is None:
        raise KeyError(f"[{filename}] No column containing 'province' found")

    # 4. Extract numeric province ID; assign 0 for the national 'Indonesia' row
    df["id_province"] = df[prov_col].str.extract(r"^(\d+)")
    df.loc[df[prov_col].str.strip().str.lower() == "indonesia", "id_province"] = "0"
    df["id_province"] = pd.to_numeric(df["id_province"], errors="coerce").astype("Int64")
    df = df.drop(columns=[prov_col])

    # 5. Move id_province to first position
    cols = ["id_province"] + [c for c in df.columns if c != "id_province"]
    df = df[cols]

    # 6. Convert value columns to numeric
    value_cols = df.columns.drop("id_province")
    for col in value_cols:
        df[col] = df[col].astype(str).str.strip()
        if is_integer_file:
            # Remove thousand separators (both comma and dot) then parse as int
            df[col] = (
                df[col]
                .str.replace(",", "", regex=False)
                .str.replace(".", "", regex=False)
            )
        else:
            # Treat dash/dot-only strings as NaN; convert comma decimals to dot
            df[col] = (
                df[col]
                .str.replace(r"^[\-\.]+$", "", regex=True)
                .str.replace(",", ".", regex=False)
                .replace("", np.nan)
            )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df
