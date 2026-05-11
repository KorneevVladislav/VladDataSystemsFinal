from pathlib import Path
import pandas as pd
import re
import csv

INPUT_DIR = Path(".")
OUTPUT_FILE = "combined_google_trends.csv"

all_rows = []

def clean_tag(raw: str) -> str:
    raw = raw.strip().lower()

    # Example: "azhagu: (Worldwide)" -> "azhagu"
    raw = re.sub(r":\s*\(.*?\)\s*$", "", raw)

    raw = re.sub(r"[^a-z0-9 _-]+", "", raw)
    raw = re.sub(r"\s+", " ", raw).strip()

    return raw

for path in sorted(INPUT_DIR.glob("*.csv")):
    if path.name == OUTPUT_FILE:
        continue

    print(f"Reading {path.name}")

    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()

    header_index = None
    for i, line in enumerate(lines):
        lower = line.lower().strip()

        if lower.startswith("day,") or lower.startswith("week,") or lower.startswith("month,") or lower.startswith("date,"):
            header_index = i
            break

    if header_index is None:
        print(f"  Skipping {path.name}: no Day/Week/Month/Date header found")
        continue

    df = pd.read_csv(path, skiprows=header_index)

    if df.empty or len(df.columns) < 2:
        print(f"  Skipping {path.name}: not enough columns")
        continue

    date_col = df.columns[0]
    score_col = df.columns[1]

    tag = clean_tag(score_col)

    if not tag:
        tag = clean_tag(path.stem)

    out = pd.DataFrame({
        "date": pd.to_datetime(df[date_col], errors="coerce").dt.date,
        "tag": tag,
        "trend_score": pd.to_numeric(df[score_col], errors="coerce")
    })

    out = out.dropna(subset=["date", "tag", "trend_score"])

    if out.empty:
        print(f"  Skipping {path.name}: no valid data rows")
        continue

    out["trend_score"] = out["trend_score"].astype(int)

    all_rows.append(out)

    print(f"  Added {len(out)} rows for tag: {tag}")

if not all_rows:
    raise RuntimeError("No usable Google Trends CSV files found.")

combined = pd.concat(all_rows, ignore_index=True)

combined.to_csv(
    OUTPUT_FILE,
    index=False,
    encoding="utf-8",
    quoting=csv.QUOTE_MINIMAL
)

print(f"Saved {len(combined)} rows to {OUTPUT_FILE}")
