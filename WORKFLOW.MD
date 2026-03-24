# Workflow

Mine Coordinates (CSV)
        │
        ▼
┌─────────────────────────┐
│  1. GEE: Fetch NAIP     │  ← Pull imagery per mine, per 5-yr epoch
│     imagery per epoch   │
└────────────┬────────────┘
             │ GeoTIFF (RGB + NIR)
             ▼
┌─────────────────────────┐
│  2. SAM: Segment mine   │  ← Prompt SAM with mine centroid
│     boundary            │
└────────────┬────────────┘
             │ Polygon (GeoJSON)
             ▼
┌─────────────────────────┐
│  3. Post-process        │  ← Filter by spectral/spatial cues
│     & validate          │     to isolate bare earth / disturbed land
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  4. Temporal analysis   │  ← Compare polygons across epochs,
│     & area tracking     │     compute area change, export GeoDataFrame
└─────────────────────────┘