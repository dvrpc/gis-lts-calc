# Level of Traffic Stress (LTS) Calculation

This project calculates Level of Traffic Stress (LTS) scores on the DVRPC model network.  It also includes a component with conflating speed data from Overture Maps to help with more accurate scoring.

## Overview

The workflow:
1. Sets up a PostgreSQL/PostGIS database
2. Loads Overture Maps transportation segment data
3. Loads DVRPC model network shapefile
4. Conflates speed data from Overture to model network
5. Calculates LTS scores (1-4)
6. Cleans up temporary files

## Prerequisites

- PostgreSQL with PostGIS extension
- Python 3.9+
- Required tools: `psql`, `ogr2ogr`

## Setup

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Create a `.env` file with your configuration:
```env
# PostgreSQL Connection
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DB=lts

# Overture Maps Configuration
OVERTURE_VERSION=2025-11-19.0

# Overture Maps download bounding box (west, south, east, north)
BBOX_WEST=-76.210785
BBOX_SOUTH=39.478606
BBOX_EAST=-73.885803
BBOX_NORTH=40.601963

# Model Network Shapefile (without .shp extension)
BIKE_NETWORK_SHAPEFILE=input/bike_network_dec1_2025_link
```

3. Place your model network shapefile in the `input/` directory and update `NETWORK_SHAPEFILE` in `.env`

### Required Model Network Shapefile Fields

Your shapefile must contain these fields:
- `no` (numeric) - Segment ID
- `fromnodeno` (numeric) - From node ID
- `tonodeno` (numeric) - To node ID
- `typeno` (varchar) - Road type code (72, 79 for residential)
- `numlanes` (numeric) - Number of lanes
- `bike_fac~1` (numeric) - Bike facility type (0-9)
- `vcur_prt~2` (varchar) - Speed field (e.g., "25mph")
- `geom` - Geometry (LineString)

## Usage

Run the complete workflow:
```bash
python3 run.py
```

## Output

Final table: `output.model_network_with_lts`

Contains all original bike network fields plus:
- `speed_mph`: Speed limit (from Overture or original data)
- `posted_speed`: Speed from Overture conflation (if available)
- `lts`: Level of Traffic Stress score (1-4, or -1/-2 for special cases)

## Files

- `run.py` - Main workflow script
- `conflate_speed_data.sql` - Spatial conflation logic
- `calculate_lts.sql` - LTS calculation using DVRPC methodology
- `old_lts.py` - Reference: Original LTS methodology from Visum
- `requirements.txt` - Python dependencies
- `.env` - Database configuration (create this)

## Bike Facility Codes

The `bike_fac~1` field should use these codes:
- `0` - No bike facility
- `1` - Sharrows
- `2` - Bike Lane
- `3` - Buffered Bike Lane
- `4` - Multi-use Trail
- `5` - Signed Bike Route
- `6` - Protected Bike Lane
- `9` - Opposite direction of one-way street

## Notes

- The overture_roads.geojson file (1.7GB) is automatically removed after loading to save space
- Speed data is preferentially taken from Overture Maps, falling back to original `vcur_prt~2` field
- The conflation uses spatial matching with 10m buffer and bearing alignment (±20°)

