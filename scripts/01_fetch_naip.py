"""
01_fetch_naip.py

Submits Google Earth Engine batch export tasks for NAIP imagery
at each mine location across all configured epochs.

Usage:
    python scripts/01_fetch_naip.py

    # Dry run (print tasks without submitting):
    python scripts/01_fetch_naip.py --dry-run

    # Limit to first N mines (useful for testing):
    python scripts/01_fetch_naip.py --limit 10

Output:
    - GEE export tasks queued to Google Drive (DRIVE_FOLDER in config.py)
    - data/outputs/naip_export_manifest.csv updated with task status

Each exported file is named:  {mine_id}_{epoch_label}.tif
Bands: R, G, B, NIR (same as NAIP source)

Prerequisites:
    1. pip install earthengine-api
    2. earthengine authenticate
    3. Update GEE_PROJECT and DRIVE_FOLDER in scripts/config.py
"""

import argparse
import csv
import sys
import time
from pathlib import Path
from typing import Optional

import ee
import pandas as pd
from tqdm import tqdm

# Allow running from the project root or the scripts/ directory
sys.path.insert(0, str(Path(__file__).parent))
from config import (
    DRIVE_FOLDER,
    EPOCHS,
    EXPORT_SCALE,
    GEE_PROJECT,
    MINE_BUFFER_METERS,
    MINES_CSV,
    NAIP_MANIFEST,
    TASK_BATCH_SIZE,
)

# ---------------------------------------------------------------------------
# Status values written to the manifest
# ---------------------------------------------------------------------------
STATUS_SUBMITTED = "submitted"
STATUS_NO_COVERAGE = "no_coverage"   # No NAIP images found for this epoch/location
STATUS_SKIPPED = "skipped"           # Already exported in a previous run


def init_gee() -> None:
    """Authenticate and initialize the GEE Python client."""
    try:
        ee.Initialize(project=GEE_PROJECT)
    except Exception:
        # Fallback: triggers browser-based auth if credentials aren't cached
        ee.Authenticate()
        ee.Initialize(project=GEE_PROJECT)


def load_mines(csv_path: Path) -> pd.DataFrame:
    """
    Load mine locations from CSV.
    Expected columns: mine_id, lat, lon
    Extra columns are ignored.
    """
    df = pd.read_csv(csv_path)

    required = {"mine_id", "lat", "lon"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"mines.csv is missing columns: {missing}\n"
            f"Found columns: {list(df.columns)}\n"
            "Update MINES_CSV column names in config.py or rename your CSV columns."
        )

    # Drop rows with null coordinates — these can't be processed
    before = len(df)
    df = df.dropna(subset=["lat", "lon"])
    dropped = before - len(df)
    if dropped:
        print(f"  [warn] Dropped {dropped} rows with null lat/lon.")

    # Coerce types
    df["lat"] = df["lat"].astype(float)
    df["lon"] = df["lon"].astype(float)
    df["mine_id"] = df["mine_id"].astype(str)

    return df


def load_manifest(manifest_path: Path) -> set[str]:
    """
    Return the set of already-processed (mine_id, epoch) keys.
    Used to skip re-submitting tasks when resuming an interrupted run.
    """
    if not manifest_path.exists():
        return set()

    df = pd.read_csv(manifest_path)
    done_statuses = {STATUS_SUBMITTED, STATUS_NO_COVERAGE}
    completed = df[df["status"].isin(done_statuses)]
    return set(zip(completed["mine_id"].astype(str), completed["epoch"]))


def build_naip_mosaic(
    lon: float,
    lat: float,
    start_date: str,
    end_date: str,
    buffer_m: int,
) -> tuple[Optional[ee.Image], ee.Geometry]:
    """
    Query NAIP for a given point + date range and return a clipped mosaic.

    Returns (image, region) where image is None if no NAIP data exists
    for this location/epoch (common — NAIP coverage is state-by-state).
    """
    point = ee.Geometry.Point([lon, lat])
    region = point.buffer(buffer_m).bounds()

    collection = (
        ee.ImageCollection("USDA/NAIP/DOQQ")
        .filterBounds(region)
        .filterDate(start_date, end_date)
        .select(["R", "G", "B", "N"])  # N = Near-Infrared
    )

    count = collection.size().getInfo()
    if count == 0:
        return None, region

    # Mosaic: most-recent image on top to minimise seams within the epoch window
    mosaic = collection.sort("system:time_start", False).mosaic().clip(region)
    return mosaic, region


def submit_export_task(
    image: ee.Image,
    region: ee.Geometry,
    task_name: str,
    scale: int,
    drive_folder: str,
) -> ee.batch.Task:
    """Submit a single GEE export-to-Drive task and return the task object."""
    task = ee.batch.Export.image.toDrive(
        image=image,
        description=task_name,
        folder=drive_folder,
        fileNamePrefix=task_name,
        region=region,
        scale=scale,
        crs="EPSG:4326",
        fileFormat="GeoTIFF",
        maxPixels=1e9,
    )
    task.start()
    return task


def append_manifest_rows(manifest_path: Path, rows: list[dict]) -> None:
    """Append rows to the manifest CSV, creating it with a header if needed."""
    write_header = not manifest_path.exists()
    with open(manifest_path, "a", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["mine_id", "epoch", "task_name", "status", "note"]
        )
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def wait_for_queue_headroom(max_active: int = 2800, poll_interval: int = 60) -> None:
    """
    Block until the number of RUNNING + READY GEE tasks drops below max_active.
    GEE hard-caps at 3000 queued tasks; this keeps a safety margin.
    """
    while True:
        tasks = ee.data.getTaskList()
        active = sum(
            1 for t in tasks if t["state"] in ("RUNNING", "READY")
        )
        if active < max_active:
            return
        print(
            f"  [queue] {active} active GEE tasks — waiting {poll_interval}s "
            "for headroom..."
        )
        time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(dry_run: bool = False, limit: Optional[int] = None) -> None:
    print("=== 01_fetch_naip.py ===")

    # Setup
    if not dry_run:
        print("Initializing GEE...")
        init_gee()

    mines = load_mines(MINES_CSV)
    if limit:
        mines = mines.head(limit)
        print(f"  [limit] Processing first {limit} mines.")

    already_done = load_manifest(NAIP_MANIFEST)
    print(f"Loaded {len(mines)} mines. {len(already_done)} (mine, epoch) pairs already in manifest.")

    # Build the full work list, skipping already-completed pairs
    work = []
    for _, row in mines.iterrows():
        for epoch_label, start, end in EPOCHS:
            key = (str(row["mine_id"]), epoch_label)
            if key in already_done:
                continue
            work.append((row["mine_id"], row["lat"], row["lon"], epoch_label, start, end))

    print(f"Tasks to submit: {len(work)}")
    if dry_run:
        print("[dry-run] No tasks submitted. Exiting.")
        return

    # Submit in batches to avoid overloading the GEE task queue
    manifest_rows: list[dict] = []
    submitted_count = 0

    for mine_id, lat, lon, epoch_label, start, end in tqdm(work, desc="Submitting tasks"):
        task_name = f"{mine_id}_{epoch_label}"

        try:
            image, region = build_naip_mosaic(lat, lon, start, end, MINE_BUFFER_METERS)
        except Exception as e:
            # Network or GEE API error — log and continue rather than crashing
            manifest_rows.append({
                "mine_id": mine_id,
                "epoch": epoch_label,
                "task_name": task_name,
                "status": "error",
                "note": str(e)[:200],
            })
            continue

        if image is None:
            # No NAIP coverage for this mine/epoch — expected for many combinations
            manifest_rows.append({
                "mine_id": mine_id,
                "epoch": epoch_label,
                "task_name": task_name,
                "status": STATUS_NO_COVERAGE,
                "note": f"No NAIP images in {start[:4]}–{end[:4]}",
            })
            continue

        try:
            submit_export_task(image, region, task_name, EXPORT_SCALE, DRIVE_FOLDER)
            manifest_rows.append({
                "mine_id": mine_id,
                "epoch": epoch_label,
                "task_name": task_name,
                "status": STATUS_SUBMITTED,
                "note": "",
            })
            submitted_count += 1
        except Exception as e:
            manifest_rows.append({
                "mine_id": mine_id,
                "epoch": epoch_label,
                "task_name": task_name,
                "status": "error",
                "note": str(e)[:200],
            })

        # Flush manifest every batch so progress is saved if the script is interrupted
        if len(manifest_rows) >= TASK_BATCH_SIZE:
            append_manifest_rows(NAIP_MANIFEST, manifest_rows)
            manifest_rows.clear()
            wait_for_queue_headroom()

    # Final flush
    if manifest_rows:
        append_manifest_rows(NAIP_MANIFEST, manifest_rows)

    print(f"\nDone. {submitted_count} tasks submitted to GEE.")
    print(f"Monitor progress at: https://code.earthengine.google.com/tasks")
    print(f"Manifest written to: {NAIP_MANIFEST}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch NAIP imagery via GEE for all mine locations.")
    parser.add_argument("--dry-run", action="store_true", help="Print tasks without submitting to GEE.")
    parser.add_argument("--limit", type=int, default=None, help="Process only the first N mines (for testing).")
    args = parser.parse_args()
    main(dry_run=args.dry_run, limit=args.limit)
