#!/usr/bin/env python3
"""
Extract lithium concentration category data from TIF raster files.
Reads Li_class_dom.tif and Li_class_pub.tif and creates a CSV with
geospatial coordinates, well type, and lithium category values.
"""

import rasterio
from rasterio.warp import transform as rasterio_transform
import numpy as np
import pandas as pd
from pathlib import Path
from pyproj import CRS


def extract_raster_data(tif_path, well_type):
    """
    Extract geospatial coordinates and category values from a TIF raster file.
    
    Parameters:
    -----------
    tif_path : str
        Path to the TIF file
    well_type : str
        Either 'domestic' or 'public'
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame with columns: longitude, latitude, well_type, lithium_category
    """
    print(f"Reading {tif_path}...")
    
    with rasterio.open(tif_path) as src:
        # Read the raster data
        data = src.read(1)  # Read first band
        
        # Get the transform to convert pixel coordinates to geographic coordinates
        transform = src.transform
        
        # Get CRS (Coordinate Reference System)
        crs = src.crs
        
        # Get NoData value
        nodata = src.nodata
        
        # Get dimensions
        height, width = data.shape
        
        # Create arrays of row and column indices
        rows, cols = np.meshgrid(np.arange(height), np.arange(width), indexing='ij')
        
        # Convert pixel coordinates to geographic coordinates
        # Using the transform to get the center of each pixel
        x_coords, y_coords = rasterio.transform.xy(transform, rows, cols)
        
        # Convert to numpy arrays
        x_array = np.array(x_coords)
        y_array = np.array(y_coords)
        
        # Convert to longitude/latitude if CRS is not geographic (EPSG:4326)
        if crs is not None and not crs.is_geographic:
            # Convert from projected coordinates to lat/lon
            print(f"  Converting from CRS {crs} to WGS84 (EPSG:4326)...")
            # Flatten for transformation
            x_flat = x_array.flatten()
            y_flat = y_array.flatten()
            
            # Transform coordinates
            lon_flat, lat_flat = rasterio_transform(
                crs, 
                CRS.from_epsg(4326), 
                x_flat, 
                y_flat
            )
            
            # Reshape back
            lon_array = np.array(lon_flat).reshape(x_array.shape)
            lat_array = np.array(lat_flat).reshape(y_array.shape)
        else:
            # Already in geographic coordinates
            lon_array = x_array
            lat_array = y_array
        
        # Flatten all arrays
        data_flat = data.flatten()
        lon_flat = lon_array.flatten()
        lat_flat = lat_array.flatten()
        
        # Filter out NoData values
        if nodata is not None:
            valid_mask = data_flat != nodata
        else:
            # If no NoData value is set, filter out NaN values
            valid_mask = ~np.isnan(data_flat)
        
        # Apply mask
        valid_data = data_flat[valid_mask]
        valid_lon = lon_flat[valid_mask]
        valid_lat = lat_flat[valid_mask]
        
        # Create DataFrame
        df = pd.DataFrame({
            'longitude': valid_lon,
            'latitude': valid_lat,
            'well_type': well_type,
            'lithium_category': valid_data.astype(int)
        })
        
        print(f"  Extracted {len(df)} valid grid cells from {well_type} wells")
        
        return df


def main():
    """Main function to extract data from both TIF files and create CSV."""
    
    # Define file paths
    base_dir = Path(__file__).parent
    dom_file = base_dir / 'Li_class_dom.tif'
    pub_file = base_dir / 'Li_class_pub.tif'
    output_csv = base_dir / 'lithium_categories.csv'
    
    # Check if files exist
    if not dom_file.exists():
        raise FileNotFoundError(f"File not found: {dom_file}")
    if not pub_file.exists():
        raise FileNotFoundError(f"File not found: {pub_file}")
    
    # Extract data from both rasters
    print("Extracting data from raster files...")
    df_dom = extract_raster_data(dom_file, 'domestic')
    df_pub = extract_raster_data(pub_file, 'public')
    
    # Combine both datasets vertically
    print("\nCombining datasets...")
    df_combined = pd.concat([df_dom, df_pub], ignore_index=True)
    
    # Sort by latitude and longitude for better organization
    df_combined = df_combined.sort_values(['latitude', 'longitude']).reset_index(drop=True)
    
    # Save to CSV
    print(f"\nSaving to {output_csv}...")
    df_combined.to_csv(output_csv, index=False)
    
    print(f"\nSuccess! Created CSV with {len(df_combined)} rows")
    print(f"  - Domestic wells: {len(df_dom)} rows")
    print(f"  - Public wells: {len(df_pub)} rows")
    print(f"\nLithium category values:")
    print(df_combined['lithium_category'].value_counts().sort_index())
    
    return df_combined


if __name__ == '__main__':
    df = main()

