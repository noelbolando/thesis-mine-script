# Thesis Project Repo

This project folder contains all the code and data to quantify the expansion and proliferation of mines in distal locations to meet the demand for aggregate material to satisfy the demand for cement in growing concentrated urban areas. 

## Tools Used:
- GoogleEarthEngine API
- NAIP Spatial Data
- [SAM](https://github.com/facebookresearch/segment-anything)
    - SAM creates a boundary so you don’t have to classify the boundary of the mine

## Questions We Seek to Answer:
- How much area is occupied by these mines?
- How much does this area change over time?
- Can we calculate the amount of aggregate material extracted from the ground over time?

## Autonomous Pipeline:
- Investigate 1 mine at a time with NAIP data, leveraging GEE and SAM
- This is developed in Python so we can call the SAM and GEE APIs

## Metadata:
- Mine locations (lat/long)
