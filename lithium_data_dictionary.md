# Data Dictionary: lithium_aggregated_by_county.csv

## Overview
This dataset contains county-level aggregations of predicted lithium concentration categories in groundwater used as drinking water across the conterminous United States. The data is derived from extreme gradient boosting model predictions that estimate the most probable lithium concentration category for each grid cell, separately for domestic and public supply wells.

## Lithium Concentration Categories
The model predicts one of four concentration categories based on lithium levels (µg/L):
- **Category 1**: ≤4 µg/L
- **Category 2**: >4 to ≤10 µg/L
- **Category 3**: >10 to ≤30 µg/L
- **Category 4**: >30 µg/L

## Well Types
- **Domestic**: Private domestic supply wells
- **Public**: Public supply wells

## Column Definitions

### County Identifiers
| Column | Type | Description |
|--------|------|-------------|
| `county_code` | Float | 5-digit FIPS county code (e.g., 10001.0 for Kent County, DE) |
| `county_name` | String | County name |

### Overall Statistics (All Well Types Combined)
| Column | Type | Description |
|--------|------|-------------|
| `total_grid_cells` | Integer | Total number of grid cells in the county (domestic + public) |
| `domestic_cells` | Integer | Number of grid cells for domestic wells |
| `public_cells` | Integer | Number of grid cells for public wells |
| `cells_cat_1` | Integer | Count of grid cells predicted as Category 1 (≤4 µg/L) |
| `cells_cat_2` | Integer | Count of grid cells predicted as Category 2 (>4 to ≤10 µg/L) |
| `cells_cat_3` | Integer | Count of grid cells predicted as Category 3 (>10 to ≤30 µg/L) |
| `cells_cat_4` | Integer | Count of grid cells predicted as Category 4 (>30 µg/L) |
| `pct_cat_1` | Float | Percentage of grid cells in Category 1 (rounded to 2 decimals) |
| `pct_cat_2` | Float | Percentage of grid cells in Category 2 (rounded to 2 decimals) |
| `pct_cat_3` | Float | Percentage of grid cells in Category 3 (rounded to 2 decimals) |
| `pct_cat_4` | Float | Percentage of grid cells in Category 4 (rounded to 2 decimals) |
| `avg_category` | Float | Weighted average category value (1-4), calculated as: (cat1×1 + cat2×2 + cat3×3 + cat4×4) / total_cells |
| `dominant_category` | Integer | Category with the highest count (1, 2, 3, or 4) |

### Domestic Well Statistics
| Column | Type | Description |
|--------|------|-------------|
| `dom_cells_cat_1` | Integer | Count of domestic well grid cells in Category 1 |
| `dom_cells_cat_2` | Integer | Count of domestic well grid cells in Category 2 |
| `dom_cells_cat_3` | Integer | Count of domestic well grid cells in Category 3 |
| `dom_cells_cat_4` | Integer | Count of domestic well grid cells in Category 4 |
| `dom_pct_cat_1` | Float | Percentage of domestic cells in Category 1 |
| `dom_pct_cat_2` | Float | Percentage of domestic cells in Category 2 |
| `dom_pct_cat_3` | Float | Percentage of domestic cells in Category 3 |
| `dom_pct_cat_4` | Float | Percentage of domestic cells in Category 4 |
| `dom_avg_category` | Float | Weighted average category for domestic wells |
| `dom_dominant_category` | Integer | Dominant category for domestic wells |

### Public Well Statistics
| Column | Type | Description |
|--------|------|-------------|
| `pub_cells_cat_1` | Integer | Count of public well grid cells in Category 1 |
| `pub_cells_cat_2` | Integer | Count of public well grid cells in Category 2 |
| `pub_cells_cat_3` | Integer | Count of public well grid cells in Category 3 |
| `pub_cells_cat_4` | Integer | Count of public well grid cells in Category 4 |
| `pub_pct_cat_1` | Float | Percentage of public cells in Category 1 |
| `pub_pct_cat_2` | Float | Percentage of public cells in Category 2 |
| `pub_pct_cat_3` | Float | Percentage of public cells in Category 3 |
| `pub_pct_cat_4` | Float | Percentage of public cells in Category 4 |
| `pub_avg_category` | Float | Weighted average category for public wells |
| `pub_dominant_category` | Integer | Dominant category for public wells |

## Data Processing Methodology

### Geocoding (County Assignment)
County assignment was performed using a spatial join operation:
1. **Input Data**: Grid cell coordinates (longitude, latitude) with lithium category predictions
2. **County Boundaries**: U.S. Census Bureau TIGER/Line shapefile (`tl_2025_us_county.shp`) containing county polygons
3. **Spatial Join Method**: Point-in-polygon analysis using GeoPandas `sjoin()` function with `predicate='within'`
   - Each grid cell point was matched to the county polygon containing it
   - Coordinate Reference System (CRS): Both datasets converted to WGS84 (EPSG:4326) for compatibility
4. **Output**: Each grid cell assigned a 5-digit FIPS county code (`GEOID`) and county name
5. **Unmatched Records**: Grid cells falling outside county boundaries (e.g., coastal areas, international waters) were excluded from aggregation (~362,606 cells, ~2.2% of total)

### Aggregation Process
County-level statistics were calculated through the following steps:

1. **Filtering**: Removed grid cells without valid county assignments (empty or null county codes)

2. **Grouping**: Grouped all grid cells by `county_code` and `county_name`

3. **Cell Counts**:
   - Total cells per county (sum of all grid cells)
   - Cells by well type (domestic vs public)
   - Cells by lithium category (1-4) for overall, domestic, and public separately

4. **Percentage Calculations**:
   - Category percentages: `(cells_in_category / total_cells) × 100`
   - Calculated separately for overall, domestic, and public well types
   - Rounded to 2 decimal places

5. **Average Category**:
   - Weighted average: `(cat1×1 + cat2×2 + cat3×3 + cat4×4) / total_cells`
   - Values range from 1.0 (all Category 1) to 4.0 (all Category 4)
   - Calculated separately for overall, domestic, and public

6. **Dominant Category**:
   - Category with the highest cell count for each county
   - Determined separately for overall, domestic, and public well types

7. **Data Quality**:
   - All calculations verified to ensure no division by zero errors
   - Percentages validated to sum to 100% (within rounding tolerance)
   - Totals validated: `domestic_cells + public_cells = total_grid_cells`

## Notes
- Grid cells represent spatial predictions from raster model outputs
- Each grid cell has predictions for both domestic and public well types
- Percentages sum to 100% for each well type group
- Average category values range from 1.0 to 4.0, where lower values indicate lower predicted lithium concentrations
- Only counties with valid county codes are included (rows without county assignments were excluded during aggregation)
- Data covers the conterminous United States only
