#!/usr/bin/env python3
"""
Spatially join county boundary data with lithium concentration CSV.
Adds county code (GEOID) and county name columns to the CSV.
"""

import geopandas as gpd
import pandas as pd
from pathlib import Path
from shapely.geometry import Point


def add_county_info(csv_path, shapefile_path, output_path):
    """
    Perform spatial join between CSV points and county boundaries.
    
    Parameters:
    -----------
    csv_path : str
        Path to the input CSV file with longitude and latitude columns
    shapefile_path : str
        Path to the county boundaries shapefile
    output_path : str
        Path to save the output CSV with county information
    """
    print("Loading CSV file...")
    df = pd.read_csv(csv_path)
    print(f"  Loaded {len(df):,} rows")
    
    print("\nLoading county boundaries shapefile...")
    counties = gpd.read_file(shapefile_path)
    print(f"  Loaded {len(counties):,} counties")
    print(f"  CRS: {counties.crs}")
    
    # Create GeoDataFrame from CSV points
    print("\nCreating point geometries from CSV coordinates...")
    geometry = [Point(lon, lat) for lon, lat in zip(df['longitude'], df['latitude'])]
    points_gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
    
    # Ensure counties are in the same CRS
    if counties.crs != points_gdf.crs:
        print(f"  Converting counties from {counties.crs} to {points_gdf.crs}...")
        counties = counties.to_crs(points_gdf.crs)
    
    print("\nPerforming spatial join...")
    # Perform spatial join - this will add county attributes to each point
    joined = gpd.sjoin(points_gdf, counties[['GEOID', 'NAME', 'geometry']], 
                       how='left', predicate='within')
    
    # Rename columns for clarity
    joined = joined.rename(columns={'GEOID': 'county_code', 'NAME': 'county_name'})
    
    # Convert county_code to string to preserve leading zeros (handle NaN values)
    # GEOID should already be string, but ensure it stays that way
    joined['county_code'] = joined['county_code'].fillna('').astype(str)
    joined['county_code'] = joined['county_code'].replace('nan', '')
    
    # Drop the geometry column and index_right from the join
    if 'geometry' in joined.columns:
        joined = joined.drop(columns=['geometry'])
    if 'index_right' in joined.columns:
        joined = joined.drop(columns=['index_right'])
    
    # Check how many points were matched (empty string means no match)
    matched = (joined['county_code'] != '').sum()
    unmatched = (joined['county_code'] == '').sum()
    
    print(f"  Matched {matched:,} points to counties ({matched/len(joined)*100:.1f}%)")
    if unmatched > 0:
        print(f"  Warning: {unmatched:,} points did not match any county ({unmatched/len(joined)*100:.1f}%)")
    
    # Reorder columns to put county info after well_type
    cols = ['longitude', 'latitude', 'well_type', 'county_code', 'county_name', 'lithium_category']
    joined = joined[cols]
    
    print(f"\nSaving output to {output_path}...")
    # Save with empty strings for missing values (not NaN)
    joined.to_csv(output_path, index=False, na_rep='')
    
    print(f"\nSuccess! Added county information to CSV")
    print(f"  Output file: {output_path}")
    print(f"  Total rows: {len(joined):,}")
    
    # Show sample of county distribution
    if matched > 0:
        print(f"\nTop 10 counties by number of grid cells:")
        county_counts = joined['county_name'].value_counts().head(10)
        for county, count in county_counts.items():
            print(f"  {county}: {count:,} cells")
    
    return joined


def main():
    """Main function."""
    base_dir = Path(__file__).parent
    csv_path = base_dir / 'lithium_categories.csv'
    shapefile_path = base_dir / 'tl_2025_us_county' / 'tl_2025_us_county.shp'
    output_path = base_dir / 'lithium_categories_with_counties.csv'
    
    # Check if files exist
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    if not shapefile_path.exists():
        raise FileNotFoundError(f"Shapefile not found: {shapefile_path}")
    
    df = add_county_info(csv_path, shapefile_path, output_path)
    return df


if __name__ == '__main__':
    df = main()

