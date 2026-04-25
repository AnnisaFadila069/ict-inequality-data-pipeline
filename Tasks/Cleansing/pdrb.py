import os
import re
import difflib
import pandas as pd


def _build_province_map_from_ai(raw_folder):
    """
    Build {province_name_lower: id_province} from raw ai.xlsx.
    Province rows follow the format 'NN. Name' (e.g. '11. Aceh').
    The 'Indonesia' aggregate row is mapped to id = 0.
    """
    ai_path = os.path.join(raw_folder, "ai.xlsx")
    if not os.path.exists(ai_path):
        raise FileNotFoundError(f"ai.xlsx not found in {raw_folder}")

    df = pd.read_excel(ai_path, header=None, dtype=str)

    # Row 1 is the actual header; rows 0-2 are metadata
    df.columns = df.iloc[1]
    df = df.drop([0, 1, 2]).reset_index(drop=True)

    # Find the province column
    prov_col = next((c for c in df.columns if "Province" in str(c)), None)
    if prov_col is None:
        raise ValueError("Province column not found in ai.xlsx")

    province_map = {}
    for val in df[prov_col].dropna():
        val = str(val).strip()
        # "11. Aceh" → id=11, name="aceh"
        m = re.match(r"^(\d+)\.\s*(.+)$", val)
        if m:
            prov_id = int(m.group(1))
            name = m.group(2).strip().lower()
            province_map[name] = prov_id
            # Also add expanded form: "kep. riau" → "kepulauan riau"
            expanded = re.sub(r"\bkep\.\s*", "kepulauan ", name).strip()
            if expanded != name:
                province_map[expanded] = prov_id
        elif val.lower() == "indonesia":
            province_map["indonesia"] = 0

    # Aliases for known ai.xlsx data errors:
    # ai.xlsx erroneously labels id=53 as "Nusa Tenggara Barat" (should be "Timur").
    # Force-correct both entries so they override whatever ai.xlsx produced.
    province_map["nusa tenggara barat"] = 52
    province_map["nusa tenggara timur"] = 53

    return province_map


def _fuzzy_id(province_name, province_map, threshold=0.8):
    """
    Return id_province for province_name using fuzzy matching.
    Tries exact match first; falls back to difflib with the given threshold.
    Returns None if no match meets the threshold.
    """
    if not province_name or pd.isna(province_name):
        return None

    name_lower = str(province_name).strip().lower()

    # Exact match
    if name_lower in province_map:
        return province_map[name_lower]

    # Fuzzy match (80% similarity threshold)
    candidates = list(province_map.keys())
    matches = difflib.get_close_matches(name_lower, candidates, n=1, cutoff=threshold)
    if matches:
        return province_map[matches[0]]

    return None


def clean(file_path):
    """
    Clean the PDRB (Regional GDP per capita) file.

    id_province is resolved by fuzzy-matching (≥80%) province names
    against the province list in ai.xlsx from the same raw data folder.
    The 'province' column is dropped from the output.

    Output:
        Columns: id_province, pdrb
    """

    df = pd.read_excel(file_path, dtype=str)

    # Rename columns
    df = df.rename(columns={
        "Provinsi": "province",
        "Produk Domestik Regional Bruto per Kapita Atas Dasar Harga Berlaku (Ribu Rp)": "pdrb",
    })

    # Drop empty / noise rows
    df = df.dropna(subset=["province", "pdrb"])
    df = df[~df["province"].str.contains(
        r"Catatan|Perbedaan|Sumber|^$", na=False, regex=True
    )]

    # Strip any leading "NN. " prefix (in case the file has it)
    df["province"] = df["province"].str.replace(r"^\d+\.\s*", "", regex=True)
    df.loc[df["province"].str.strip().str.lower() == "indonesia", "province"] = "Indonesia"

    # =========================
    # CLEAN PDRB VALUE
    # =========================
    df["pdrb"] = (
        df["pdrb"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace(".", "", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
    )

    # =========================
    # ADD INDONESIA TOTAL ROW
    # =========================
    indonesia_total = pd.DataFrame({
        "province": ["Indonesia"],
        "pdrb": [df["pdrb"].sum()],
    })
    df = df[df["province"].str.lower() != "indonesia"]
    df = pd.concat([df, indonesia_total], ignore_index=True)

    # =========================
    # MAP PROVINCE → id_province  (fuzzy, ≥80%)
    # =========================
    raw_folder = os.path.dirname(os.path.abspath(file_path))
    province_map = _build_province_map_from_ai(raw_folder)

    df["id_province"] = df["province"].apply(
        lambda p: _fuzzy_id(p, province_map, threshold=0.8)
    )

    # Indonesia always gets id_province = 0
    df.loc[df["province"].str.strip().str.lower() == "indonesia", "id_province"] = 0

    df["id_province"] = pd.to_numeric(df["id_province"], errors="coerce").astype("Int64")
    df = df.drop(columns=["province"])

    # =========================
    # FINAL OUTPUT
    # =========================
    df = df[["id_province", "pdrb"]].reset_index(drop=True)

    return df
