---
name: aggregate_mine_pipeline
description: Thesis project tracking aggregate mine growth using GEE + SAM + NAIP data
type: project
---

User is building a Python pipeline to identify and track growth of aggregate mines in the US over time.

**Why:** Thesis research quantifying mine expansion relative to urban cement demand.

**Stack:** Google Earth Engine (NAIP imagery), SAM (segmentation), Python, GeoPandas

**Status (2026-03-23):**
- `scripts/config.py`, `scripts/01_fetch_naip.py`, `requirements.txt`, `WORKFLOW.MD` are written
- User is geocoding mine CSV and still setting up GEE authentication
- Next step: user delivers `data/mines.csv` (columns: mine_id, lat, lon), then write `02_segment_mines.py`

**Scale:** ~2000 mine locations (some may be faulty/false positives)

**Key decisions made:**
- GEE batch export to Google Drive (not local downloads)
- 5-year epochs: E1 2003–2007, E2 2008–2012, E3 2013–2017, E4 2018–2022
- 2km buffer around each mine centroid, 2m/px export resolution
- False positive filtering in 03_postprocess.py (NDVI, area sanity, spectral checks)
- Manifest CSV for resumability across interrupted runs

**How to apply:** When user returns, pick up at `02_segment_mines.py` (SAM segmentation loop).
