"""
clean_mines.py
Cleans and combines limestone and sand & gravel mine CSVs.

Steps:
  1. Normalize column names (Mine Name_x -> Mine Name)
  2. Drop rows with geocode_method == "none"
  3. Drop rows with NaN lat or lon
  4. Add mine_type column ("limestone_mine" or "sand_gravel_mine")
  5. Combine into a single output CSV
  6. Write pipeline-ready clean_mines.csv (mine_id, lat, lon) to data/
"""

import pandas as pd
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data" / "outputs"
LIMESTONE_CSV = DATA_DIR / "limestone_mines_03242026.csv"
SAND_GRAVEL_CSV = DATA_DIR / "sand_gravel_mines_03242026.csv"
OUTPUT_CSV = DATA_DIR / "combined_mines_clean.csv"
PIPELINE_CSV = ROOT_DIR / "data" / "clean_mines.csv"

# ── Load ───────────────────────────────────────────────────────────────────────
limestone = pd.read_csv(LIMESTONE_CSV, dtype=str)
sand_gravel = pd.read_csv(SAND_GRAVEL_CSV, dtype=str)

print(f"Loaded limestone:   {len(limestone):>6} rows")
print(f"Loaded sand/gravel: {len(sand_gravel):>6} rows")

# ── Normalize headers ──────────────────────────────────────────────────────────
sand_gravel = sand_gravel.rename(columns={"Mine Name_x": "Mine Name"})

# ── Tag commodity type ─────────────────────────────────────────────────────────
limestone["mine_type"] = "limestone_mine"
sand_gravel["mine_type"] = "sand_gravel_mine"

# ── Combine ────────────────────────────────────────────────────────────────────
df = pd.concat([limestone, sand_gravel], ignore_index=True)
print(f"\nCombined:           {len(df):>6} rows")

# ── Drop missing geocode ───────────────────────────────────────────────────────
before = len(df)
df = df[df["geocode_method"].str.strip() != "none"]
print(f"After drop no geocode:{len(df):>6} rows  (removed {before - len(df)})")

# ── Drop NaN lat/lon ───────────────────────────────────────────────────────────
before = len(df)
df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
df = df.dropna(subset=["lat", "lon"])
print(f"After drop NaN coords:{len(df):>6} rows  (removed {before - len(df)})")

# ── Reorder columns (mine_type near the front) ─────────────────────────────────
cols = ["Mine ID", "Mine Name", "mine_type", "Commodity", "Mine Status",
        "Status Date", "Type of Mine", "Street", "City", "State", "Zip Code",
        "full_address", "lat", "lon", "confidence", "geocode_method"]
df = df[cols]

# ── Save full combined file ────────────────────────────────────────────────────
df.to_csv(OUTPUT_CSV, index=False)
print(f"\nSaved → {OUTPUT_CSV}")
print(f"Final row count: {len(df)}")

# ── Save pipeline-ready file ───────────────────────────────────────────────────
pipeline_df = df.rename(columns={"Mine ID": "mine_id"})[["mine_id", "lat", "lon"]]
pipeline_df.to_csv(PIPELINE_CSV, index=False)
print(f"Saved → {PIPELINE_CSV}  (pipeline-ready: mine_id, lat, lon)")
