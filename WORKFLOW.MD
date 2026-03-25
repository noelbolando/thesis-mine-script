# Workflow

## Pipeline Overview

```
data/mines.csv  (mine_id, lat, lon)
       │
       ▼
01_fetch_naip.py     ← GEE batch export → Google Drive (GeoTIFFs per mine/epoch)
       │
       ▼
02_segment_mines.py  ← SAM segmentation → binary masks + vectorized polygons
       │
       ▼
03_postprocess.py    ← False positive filtering (NDVI, area, spectral checks)
       │
       ▼
04_temporal_analysis.py  ← Area change over time → GeoDataFrame output
```

## Scripts

| Script | Purpose | Input | Output |
|--------|---------|-------|--------|
| `scripts/01_fetch_naip.py` | Pull NAIP imagery from GEE per mine/epoch | `data/mines.csv` | GeoTIFFs in Drive, `naip_export_manifest.csv` |
| `scripts/02_segment_mines.py` | Run SAM on each GeoTIFF | GeoTIFFs | Binary masks + GeoJSON polygons |
| `scripts/03_postprocess.py` | Filter false positives | Polygons | Validated polygons |
| `scripts/04_temporal_analysis.py` | Compute area change per epoch | Validated polygons | Final GeoDataFrame |
| `scripts/config.py` | Shared configuration | — | — |

## Epochs

| Label | Year Range |
|-------|-----------|
| E1    | 2003–2007 |
| E2    | 2008–2012 |
| E3    | 2013–2017 |
| E4    | 2018–2022 |

## False Positive Strategy

Mines are flagged during `03_postprocess.py` if:
- **NDVI too high** (mean > 0.3) — segment is likely vegetation, not bare earth
- **Area out of range** — mask < 1 ha or > 50 km²
- **Low brightness / high NIR ratio** — inconsistent with disturbed bare earth

Flagged mines are written to a separate review file rather than discarded outright.

## Setup

```bash
pip install -r requirements.txt
earthengine authenticate
# Set GEE_PROJECT and DRIVE_FOLDER in scripts/config.py
```

## Running the Pipeline

```bash
# Test with 10 mines first
python scripts/01_fetch_naip.py --limit 10

# Full run
python scripts/01_fetch_naip.py

# Monitor GEE tasks at:
# https://code.earthengine.google.com/tasks
```