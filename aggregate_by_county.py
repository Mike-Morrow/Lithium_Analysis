#!/usr/bin/env python3
"""
Aggregate lithium category data by county.
Groups data from lithium_categories_with_counties.csv and calculates
statistics for each county including counts, percentages, and averages
for each lithium category, separated by well type.
"""

import pandas as pd
from pathlib import Path


def aggregate_by_county(input_path, output_path):
    """
    Aggregate lithium category data by county.
    
    Parameters:
    -----------
    input_path : str
        Path to the input CSV file with county information
    output_path : str
        Path to save the aggregated output CSV
    """
    print("Loading input CSV file...")
    df = pd.read_csv(input_path)
    print(f"  Loaded {len(df):,} rows")
    
    # Filter out rows without county codes (empty strings or NaN)
    print("\nFiltering rows with valid county codes...")
    initial_count = len(df)
    # Replace empty strings with NaN for easier filtering
    df['county_code'] = df['county_code'].replace('', pd.NA)
    df = df[df['county_code'].notna()]
    print(f"  Filtered from {initial_count:,} to {len(df):,} rows")
    print(f"  Removed {initial_count - len(df):,} rows without county codes")
    
    # Convert county_code to float to match output format
    df['county_code'] = df['county_code'].astype(float)
    
    # Group by county
    print("\nAggregating data by county...")
    
    # Create base aggregation with county info
    grouped = df.groupby(['county_code', 'county_name'])
    
    # Calculate total counts
    total_cells = grouped.size().reset_index(name='total_grid_cells')
    
    # Count by well type
    well_type_counts = df.groupby(['county_code', 'county_name', 'well_type']).size().unstack(fill_value=0)
    well_type_counts = well_type_counts.reset_index()
    well_type_counts.columns = ['county_code', 'county_name', 'domestic_cells', 'public_cells']
    
    # Count by category (overall)
    cat_counts = df.groupby(['county_code', 'county_name', 'lithium_category']).size().unstack(fill_value=0)
    cat_counts = cat_counts.reset_index()
    # Ensure all categories 1-4 are present
    for cat in [1, 2, 3, 4]:
        if cat not in cat_counts.columns:
            cat_counts[cat] = 0
    cat_counts.columns = ['county_code', 'county_name', 'cells_cat_1', 'cells_cat_2', 'cells_cat_3', 'cells_cat_4']
    
    # Count by category for domestic wells
    dom_cat_counts = df[df['well_type'] == 'domestic'].groupby(
        ['county_code', 'county_name', 'lithium_category']
    ).size().unstack(fill_value=0).reset_index()
    # Ensure all categories 1-4 are present
    for cat in [1, 2, 3, 4]:
        if cat not in dom_cat_counts.columns:
            dom_cat_counts[cat] = 0
    dom_cat_counts.columns = ['county_code', 'county_name', 'dom_cells_cat_1', 'dom_cells_cat_2', 'dom_cells_cat_3', 'dom_cells_cat_4']
    
    # Count by category for public wells
    pub_cat_counts = df[df['well_type'] == 'public'].groupby(
        ['county_code', 'county_name', 'lithium_category']
    ).size().unstack(fill_value=0).reset_index()
    # Ensure all categories 1-4 are present
    for cat in [1, 2, 3, 4]:
        if cat not in pub_cat_counts.columns:
            pub_cat_counts[cat] = 0
    pub_cat_counts.columns = ['county_code', 'county_name', 'pub_cells_cat_1', 'pub_cells_cat_2', 'pub_cells_cat_3', 'pub_cells_cat_4']
    
    # Merge all aggregations
    result = total_cells.merge(well_type_counts, on=['county_code', 'county_name'], how='left')
    result = result.merge(cat_counts, on=['county_code', 'county_name'], how='left')
    result = result.merge(dom_cat_counts, on=['county_code', 'county_name'], how='left')
    result = result.merge(pub_cat_counts, on=['county_code', 'county_name'], how='left')
    
    # Fill any missing values with 0
    result = result.fillna(0)
    
    # Calculate percentages (overall)
    result['pct_cat_1'] = (result['cells_cat_1'] / result['total_grid_cells'] * 100).round(2)
    result['pct_cat_2'] = (result['cells_cat_2'] / result['total_grid_cells'] * 100).round(2)
    result['pct_cat_3'] = (result['cells_cat_3'] / result['total_grid_cells'] * 100).round(2)
    result['pct_cat_4'] = (result['cells_cat_4'] / result['total_grid_cells'] * 100).round(2)
    
    # Calculate average category (weighted average)
    result['avg_category'] = (
        (result['cells_cat_1'] * 1 + 
         result['cells_cat_2'] * 2 + 
         result['cells_cat_3'] * 3 + 
         result['cells_cat_4'] * 4) / result['total_grid_cells']
    ).round(2)
    
    # Calculate dominant category (category with highest count)
    cat_cols = ['cells_cat_1', 'cells_cat_2', 'cells_cat_3', 'cells_cat_4']
    result['dominant_category'] = result[cat_cols].idxmax(axis=1).str.replace('cells_cat_', '').astype(int)
    
    # Calculate percentages for domestic wells
    result['dom_pct_cat_1'] = (result['dom_cells_cat_1'] / result['domestic_cells'] * 100).round(2)
    result['dom_pct_cat_2'] = (result['dom_cells_cat_2'] / result['domestic_cells'] * 100).round(2)
    result['dom_pct_cat_3'] = (result['dom_cells_cat_3'] / result['domestic_cells'] * 100).round(2)
    result['dom_pct_cat_4'] = (result['dom_cells_cat_4'] / result['domestic_cells'] * 100).round(2)
    
    # Calculate average category for domestic wells
    result['dom_avg_category'] = (
        (result['dom_cells_cat_1'] * 1 + 
         result['dom_cells_cat_2'] * 2 + 
         result['dom_cells_cat_3'] * 3 + 
         result['dom_cells_cat_4'] * 4) / result['domestic_cells']
    ).round(2)
    
    # Calculate dominant category for domestic wells
    dom_cat_cols = ['dom_cells_cat_1', 'dom_cells_cat_2', 'dom_cells_cat_3', 'dom_cells_cat_4']
    result['dom_dominant_category'] = result[dom_cat_cols].idxmax(axis=1).str.replace('dom_cells_cat_', '').astype(int)
    
    # Calculate percentages for public wells
    result['pub_pct_cat_1'] = (result['pub_cells_cat_1'] / result['public_cells'] * 100).round(2)
    result['pub_pct_cat_2'] = (result['pub_cells_cat_2'] / result['public_cells'] * 100).round(2)
    result['pub_pct_cat_3'] = (result['pub_cells_cat_3'] / result['public_cells'] * 100).round(2)
    result['pub_pct_cat_4'] = (result['pub_cells_cat_4'] / result['public_cells'] * 100).round(2)
    
    # Calculate average category for public wells
    result['pub_avg_category'] = (
        (result['pub_cells_cat_1'] * 1 + 
         result['pub_cells_cat_2'] * 2 + 
         result['pub_cells_cat_3'] * 3 + 
         result['pub_cells_cat_4'] * 4) / result['public_cells']
    ).round(2)
    
    # Calculate dominant category for public wells
    pub_cat_cols = ['pub_cells_cat_1', 'pub_cells_cat_2', 'pub_cells_cat_3', 'pub_cells_cat_4']
    result['pub_dominant_category'] = result[pub_cat_cols].idxmax(axis=1).str.replace('pub_cells_cat_', '').astype(int)
    
    # Handle division by zero (shouldn't happen, but just in case)
    result = result.replace([float('inf'), float('-inf')], 0)
    
    # Reorder columns to match expected output format
    column_order = [
        'county_code', 'county_name', 'total_grid_cells', 'domestic_cells', 'public_cells',
        'cells_cat_1', 'cells_cat_2', 'cells_cat_3', 'cells_cat_4',
        'pct_cat_1', 'pct_cat_2', 'pct_cat_3', 'pct_cat_4',
        'avg_category', 'dominant_category',
        'dom_cells_cat_1', 'dom_cells_cat_2', 'dom_cells_cat_3', 'dom_cells_cat_4',
        'dom_pct_cat_1', 'dom_pct_cat_2', 'dom_pct_cat_3', 'dom_pct_cat_4',
        'dom_avg_category', 'dom_dominant_category',
        'pub_cells_cat_1', 'pub_cells_cat_2', 'pub_cells_cat_3', 'pub_cells_cat_4',
        'pub_pct_cat_1', 'pub_pct_cat_2', 'pub_pct_cat_3', 'pub_pct_cat_4',
        'pub_avg_category', 'pub_dominant_category'
    ]
    result = result[column_order]
    
    # Sort by county_code
    result = result.sort_values('county_code').reset_index(drop=True)
    
    print(f"\nAggregated to {len(result):,} counties")
    
    # Save to CSV
    print(f"\nSaving output to {output_path}...")
    result.to_csv(output_path, index=False)
    
    print(f"\nSuccess! Created aggregated CSV")
    print(f"  Output file: {output_path}")
    print(f"  Total counties: {len(result):,}")
    
    # Show sample statistics
    print(f"\nSample statistics:")
    print(f"  Total grid cells across all counties: {result['total_grid_cells'].sum():,}")
    print(f"  Average cells per county: {result['total_grid_cells'].mean():.1f}")
    print(f"  Counties with most cells:")
    top_counties = result.nlargest(5, 'total_grid_cells')[['county_name', 'total_grid_cells']]
    for _, row in top_counties.iterrows():
        print(f"    {row['county_name']}: {row['total_grid_cells']:,} cells")
    
    return result


def main():
    """Main function."""
    base_dir = Path(__file__).parent
    input_path = base_dir / 'lithium_categories_with_counties.csv'
    output_path = base_dir / 'lithium_aggregated_by_county.csv'
    
    # Check if input file exists
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    df = aggregate_by_county(input_path, output_path)
    return df


if __name__ == '__main__':
    df = main()
