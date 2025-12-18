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
- Creates point geometries from longitude/latitude coordinates
- Performs spatial join using GeoPandas `sjoin()` with `predicate='within'` (point-in-polygon analysis)
- Converts coordinate systems to WGS84 (EPSG:4326) for compatibility
- Matches each grid cell to its corresponding county polygon
- Adds county code (GEOID/FIPS code) and county name to each record
- Grid cells outside county boundaries remain unmatched (~2.2% of total)

**Output:** `lithium_categories_with_counties.csv`
- Columns: `longitude`, `latitude`, `well_type`, `county_code`, `county_name`, `lithium_category`
- Rows: ~16.3 million grid cells (includes matched and unmatched cells)

---

### 3. Aggregate to County Level
**Script:** `aggregate_by_county.py`  
**Input:** 
- `lithium_categories_with_counties.csv`

**Process:**
- Filters out grid cells without valid county assignments (~362,606 cells excluded)
- Groups data by county code and county name
- Calculates statistics for each county:
  - Total grid cell counts (overall and by well type: domestic vs public)
  - Cell counts for each lithium category (1-4) for overall, domestic, and public separately
  - Percentages for each category: `(cells_in_category / total_cells) × 100`
  - Weighted average category: `(cat1×1 + cat2×2 + cat3×3 + cat4×4) / total_cells`
  - Dominant category (category with highest count)
  - All statistics calculated separately for overall, domestic, and public well types
- Validates data quality (percentages sum to 100%, totals match)

**Output:** `lithium_aggregated_by_county.csv`
- Columns: 35 columns including county identifiers, cell counts, percentages, averages, and dominant categories for overall, domestic, and public well types
- Rows: 3,109 counties (one row per county)

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
- Filters mortality data to include only rows with valid county codes
- Sanitizes table and column names for database compatibility
- Excludes large raw lithium data files (uses aggregated version instead)
- Each file becomes a separate table

**Output:** `data.duckdb` (DuckDB database file)
- Contains tables for aggregated lithium data and mortality data files

---

## Data Flow Summary

```
TIF Raster Files → Extract Coordinates → Add County Info → Aggregate by County → Load to DuckDB
                                                              ↓
                                              Mortality Excel Files ──────────────┘
```
