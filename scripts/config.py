"""
Shared configuration for all pipeline scripts.
Edit these values to match your setup before running.
"""

import os
from pathlib import Path

# --- Paths ---
ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data"
OUTPUTS_DIR = DATA_DIR / "outputs"
GEOTIFFS_DIR = OUTPUTS_DIR / "geotiffs"
MASKS_DIR = OUTPUTS_DIR / "masks"
POLYGONS_DIR = OUTPUTS_DIR / "polygons"

MINES_CSV = DATA_DIR / "clean_mines.csv"  # expected columns: mine_id, lat, lon

# Manifest tracks export status across runs (allows resuming interrupted batches)
NAIP_MANIFEST = OUTPUTS_DIR / "naip_export_manifest.csv"

# --- GEE Settings ---
# Your GEE project ID (from https://console.cloud.google.com)
GEE_PROJECT = "thesis-teleconnection-project"

# Google Drive folder where GEE will export images
DRIVE_FOLDER = "thesis_naip_exports"

# --- NAIP Epochs ---
# Each epoch is a (label, start_date, end_date) tuple.
# NAIP availability varies by state; missing epochs are handled gracefully.
EPOCHS = [
    ("E1", "2003-01-01", "2007-12-31"),
    ("E2", "2008-01-01", "2012-12-31"),
    ("E3", "2013-01-01", "2017-12-31"),
    ("E4", "2018-01-01", "2022-12-31"),
]

# --- Spatial Settings ---
# Buffer radius around each mine centroid (meters)
MINE_BUFFER_METERS = 2000

# Output image scale (meters/pixel). NAIP native res is ~1m; 2m reduces file size significantly.
EXPORT_SCALE = 2

# Max GEE export tasks to submit before pausing to check queue depth.
# GEE allows up to 3000 queued tasks per user.
TASK_BATCH_SIZE = 200
