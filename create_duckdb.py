#!/usr/bin/env python3
"""
Script to convert CSV and XLS (TSV) files into a DuckDB database.
"""

import duckdb
import pandas as pd
import os
from pathlib import Path

def sanitize_table_name(filename):
    """Convert filename to a valid table name."""
    # Remove extension and replace spaces/special chars with underscores
    name = Path(filename).stem
    name = name.replace(' ', '_').replace('-', '_').replace(',', '')
    name = ''.join(c if c.isalnum() or c == '_' else '_' for c in name)
    # Ensure it starts with a letter or underscore
    if name and name[0].isdigit():
        name = '_' + name
    return name if name else 'table'

def read_tsv_file(filepath):
    """Read a TSV file (even if it has .xls extension)."""
    # Try reading as TSV first
    try:
        df = pd.read_csv(filepath, sep='\t', encoding='utf-8', low_memory=False)
        return df
    except Exception as e:
        print(f"Error reading {filepath} as TSV: {e}")
        # Try with different encoding
        try:
            df = pd.read_csv(filepath, sep='\t', encoding='latin-1', low_memory=False)
            return df
        except Exception as e2:
            print(f"Error reading {filepath} with latin-1 encoding: {e2}")
            raise

def main():
    # Database file path
    db_path = 'data.duckdb'
    
    # Remove existing database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed existing database: {db_path}")
    
    # Connect to DuckDB database
    conn = duckdb.connect(db_path)
    print(f"Created DuckDB database: {db_path}")
    
    # Get all CSV and XLS files in the current directory
    # Exclude large raw lithium data files (use aggregated version instead)
    excluded_files = {'lithium_categories.csv', 'lithium_categories_with_counties.csv'}
    
    files_to_process = []
    for file in os.listdir('.'):
        if (file.endswith('.csv') or file.endswith('.xls')) and file not in excluded_files:
            files_to_process.append(file)
    
    print(f"\nFound {len(files_to_process)} files to process:")
    for f in files_to_process:
        print(f"  - {f}")
    
    # Process each file
    for filename in sorted(files_to_process):
        print(f"\nProcessing: {filename}")
        
        try:
            # Read the file
            if filename.endswith('.csv'):
                df = pd.read_csv(filename, low_memory=False)
            else:  # .xls files (actually TSV)
                df = read_tsv_file(filename)
                # Filter to only include rows with county codes
                # County Code column may be named "County Code" or "County_Code" after cleaning
                county_code_cols = [col for col in df.columns if 'county' in col.lower() and 'code' in col.lower()]
                if county_code_cols:
                    county_code_col = county_code_cols[0]
                    initial_rows = len(df)
                    # Filter out rows where County Code is null/NaN
                    df = df[df[county_code_col].notna()]
                    # Filter out rows where County Code is 0, empty string, or non-numeric text
                    # County codes should be numeric (typically 5-digit codes like 1001, 1003, etc.)
                    def has_valid_county_code(value):
                        if pd.isna(value):
                            return False
                        # If string, check it's not empty
                        if isinstance(value, str):
                            value = value.strip()
                            if value == '':
                                return False
                        # Try to convert to numeric and check it's > 0
                        try:
                            num_value = float(value)
                            return num_value > 0
                        except (ValueError, TypeError):
                            return False
                    
                    df = df[df[county_code_col].apply(has_valid_county_code)]
                    filtered_rows = len(df)
                    if initial_rows != filtered_rows:
                        print(f"  Filtered from {initial_rows} to {filtered_rows} rows (removed rows without county codes)")
            
            # Drop notes column if it exists (case-insensitive)
            notes_cols = [col for col in df.columns if col.strip().lower() == 'notes']
            if notes_cols:
                df = df.drop(columns=notes_cols)
                print(f"  Dropped notes column(s): {notes_cols}")
            
            # Clean column names for database
            df.columns = [col.strip().replace(' ', '_').replace('-', '_').replace('.', '_') 
                         for col in df.columns]
            df.columns = [''.join(c if c.isalnum() or c == '_' else '_' for c in col) 
                         for col in df.columns]
            
            # Ensure column names are valid and unique
            df.columns = [f'col_{i}' if not col or col[0].isdigit() else col 
                         for i, col in enumerate(df.columns)]
            
            # Create table name
            table_name = sanitize_table_name(filename)
            print(f"  Table name: {table_name}")
            print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
            
            # Write to DuckDB
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.register('temp_df', df)
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
            conn.unregister('temp_df')
            print(f"  ✓ Successfully imported into table '{table_name}'")
            
        except Exception as e:
            print(f"  ✗ Error processing {filename}: {e}")
            import traceback
            traceback.print_exc()
    
    # Show summary
    print("\n" + "="*60)
    print("Database Summary:")
    print("="*60)
    
    tables = conn.execute("SHOW TABLES").fetchall()
    
    for table in tables:
        table_name = table[0]
        count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        count = count_result[0] if count_result else 0
        
        # Get column info
        columns_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
        columns = [col[0] for col in columns_result]
        
        print(f"\nTable: {table_name}")
        print(f"  Rows: {count}")
        print(f"  Columns: {len(columns)}")
        print(f"  Column names: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
    
    conn.close()
    print(f"\n✓ DuckDB database created successfully: {db_path}")

if __name__ == '__main__':
    main()



