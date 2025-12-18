# Data Processing Pipeline Overview

## High-Level Steps

### 1. Extract Lithium Data from Raster Files
**Script:** `extract_lithium_data.py`  
**Input:** 
- `Li_class_dom.tif` (domestic wells)
- `Li_class_pub.tif` (public wells)

**Process:**
- Reads TIF raster files containing lithium concentration categories
- Extracts geospatial coordinates (longitude, latitude) for each grid cell
- Converts pixel coordinates to geographic coordinates (WGS84)
- Filters out NoData values
- Combines data from both well types

**Output:** `lithium_categories.csv`
- Columns: `longitude`, `latitude`, `well_type`, `lithium_category`

---

### 2. Add County Information
**Script:** `add_county_info.py`  
**Input:** 
- `lithium_categories.csv`
- County boundaries shapefile (`tl_2025_us_county/tl_2025_us_county.shp`)

**Process:**
- Performs spatial join between lithium data points and county boundaries
- Matches each grid cell to its corresponding county using point-in-polygon analysis
- Adds county code (GEOID) and county name to each record

**Output:** `lithium_categories_with_counties.csv`
- Columns: `longitude`, `latitude`, `well_type`, `county_code`, `county_name`, `lithium_category`

---

### 3. Aggregate to County Level
**Note:** Aggregation code not present in repository, but output file exists.

**Input:** `lithium_categories_with_counties.csv`

**Process:**
- Groups data by county
- Calculates statistics for each county:
  - Total grid cell counts (overall and by well type)
  - Counts and percentages for each lithium category (1-4)
  - Average category values
  - Dominant category per county
  - Separate statistics for domestic vs. public wells

**Output:** `lithium_aggregated_by_county.csv`

---

### 4. Load Data into DuckDB
**Script:** `create_duckdb.py`  
**Input:** 
- All CSV files in directory (including aggregated lithium data)
- 3 Excel files with mortality data:
  - `Underlying Cause of Death, 1999-2020_Parkinsons.xls`
  - `Underlying Cause of Death, 1999-2020-AD.xls`
  - `Underlying Cause of Death, 1999-2020-non AD.xls`

**Process:**
- Creates DuckDB database (`data.duckdb`)
- Reads CSV files directly into tables
- Reads Excel files (actually TSV format) into tables
- Sanitizes table and column names for database compatibility
- Each file becomes a separate table

**Output:** `data.duckdb` (DuckDB database file)

---

## Data Flow Summary

```
TIF Raster Files → Extract Coordinates → Add County Info → Aggregate by County → Load to DuckDB
                                                              ↓
                                              Mortality Excel Files ──────────────┘
```
