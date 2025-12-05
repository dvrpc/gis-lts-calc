#!/usr/bin/env python3
"""
Setup database, load Overture data, load model network,
conflate speed data, and calculate LTS.
"""
import os
import subprocess
import sys
from dotenv import load_dotenv
import geopandas as gpd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()


def get_db_url():
    """Get database URL from environment variables."""
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        host = os.getenv('POSTGRES_HOST')
        port = os.getenv('POSTGRES_PORT')
        user = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')
        db_name = os.getenv('POSTGRES_DB')

        if not password:
            raise ValueError("POSTGRES_PASSWORD must be set in .env file")

        db_url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

    return db_url


def setup_database():
    """Create database and enable PostGIS extension if needed."""
    print("\n" + "="*60)
    print("STEP 0: Database Setup")
    print("="*60 + "\n")

    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD')
    db_name = os.getenv('POSTGRES_DB', 'lts')

    if not password:
        raise ValueError("POSTGRES_PASSWORD must be set in .env file")

    print(f"Connecting to PostgreSQL at {host}:{port}...")

    # Connect to PostgreSQL server (default 'postgres' database)
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database='postgres'
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    # Check if database exists
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
    exists = cursor.fetchone()

    if exists:
        print(f"Database '{db_name}' already exists.")
    else:
        print(f"Creating database '{db_name}'...")
        cursor.execute(f'CREATE DATABASE {db_name}')
        print(f"Database '{db_name}' created successfully.")

    cursor.close()
    conn.close()

    # Connect to the database to enable PostGIS
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db_name
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    # Enable PostGIS extension
    print("Enabling PostGIS extension...")
    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    # Verify PostGIS is installed
    cursor.execute("SELECT PostGIS_version()")
    version = cursor.fetchone()
    print(f"PostGIS version: {version[0]}")

    cursor.close()
    conn.close()

    print("Database setup complete!")


def download_overture_roads(geojson_file="overture_roads.geojson"):
    """Download Overture Maps road data if not already present."""
    # Check if file already exists
    if os.path.exists(geojson_file):
        print(f"{geojson_file} already exists. Skipping download.")
        return geojson_file

    try:
        import overturemaps
        import geopandas as gpd
    except ImportError as e:
        print(f"Error: Required package not found: {e}")
        print("Please install: pip install overturemaps geopandas")
        sys.exit(1)

    # Get bounding box from environment
    west = float(os.getenv('BBOX_WEST', -76.210785))
    south = float(os.getenv('BBOX_SOUTH', 39.478606))
    east = float(os.getenv('BBOX_EAST', -73.885803))
    north = float(os.getenv('BBOX_NORTH', 40.601963))

    # Get Overture Maps version from environment
    version = os.getenv('OVERTURE_VERSION')

    print(f"Downloading Overture Maps road data:")
    print(f"  Version: {version}")
    print(f"  Bounding box: West={west}, South={south}, East={east}, North={north}")
    print(f"\nDownloading to {geojson_file}...")
    print("This may take several minutes...")

    # Download roads using overturemaps package
    reader = overturemaps.record_batch_reader(
        overture_type="segment",
        bbox=(west, south, east, north),
        release=version
    )

    if reader is None:
        raise Exception("Failed to get data from Overture Maps")

    # Convert to GeoDataFrame
    gdf = gpd.GeoDataFrame.from_arrow(reader.read_all())

    # Save to GeoJSON
    gdf.to_file(geojson_file, driver="GeoJSON")

    print(f"Download complete! ({len(gdf)} features)")
    return geojson_file


def load_overture_roads(geojson_file="overture_roads.geojson"):
    """Download (if needed) and load Overture Maps road data into PostgreSQL."""
    print("\n" + "="*60)
    print("STEP 1: Loading Overture Roads Data")
    print("="*60 + "\n")

    # Download if not present
    download_overture_roads(geojson_file)

    if not os.path.exists(geojson_file):
        print(f"Error: {geojson_file} not found after download attempt.")
        sys.exit(1)

    db_url = get_db_url()

    print(f"Reading GeoJSON file: {geojson_file}")
    gdf = gpd.read_file(geojson_file)

    print(f"Loaded {len(gdf)} road segments")

    # Create database engine
    print(f"Connecting to database...")
    engine = create_engine(db_url)

    # Drop dependent objects first
    print("Cleaning up existing data...")
    with engine.connect() as conn:
        # Drop conflation schema CASCADE to remove all dependent views
        conn.execute(text("DROP SCHEMA IF EXISTS conflation CASCADE"))
        # Drop overture_roads table if it exists
        conn.execute(text("DROP TABLE IF EXISTS overture_roads CASCADE"))
        conn.commit()

    # Load data to PostgreSQL
    table_name = "overture_roads"
    print(f"Loading data to table '{table_name}'...")

    gdf.to_postgis(
        table_name,
        engine,
        if_exists='replace',
        index=False
    )

    print(f"\nOverture data successfully loaded!")
    print(f"Total records: {len(gdf)}")


def load_network():
    """Load model network shapefile into PostgreSQL using ogr2ogr."""
    print("\n" + "="*60)
    print("STEP 2: Loading Network Data")
    print("="*60 + "\n")

    # Get shapefile path from environment
    shapefile_base = os.getenv('NETWORK_SHAPEFILE', 'input/bike_network_dec1_2025_link')
    shapefile_path = f"{shapefile_base}.SHP"

    # Try lowercase extension if uppercase doesn't exist
    if not os.path.exists(shapefile_path):
        shapefile_path = f"{shapefile_base}.shp"

    if not os.path.exists(shapefile_path):
        print(f"Error: Shapefile not found at {shapefile_base}.SHP or {shapefile_base}.shp")
        print(f"Please set NETWORK_SHAPEFILE in .env file")
        sys.exit(1)

    # Get database connection details
    host = os.getenv('POSTGRES_HOST')
    port = os.getenv('POSTGRES_PORT')
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    db_name = os.getenv('POSTGRES_DB')

    # Build PostgreSQL connection string
    pg_conn = f"PG:host={host} port={port} dbname={db_name} user={user} password={password}"

    print(f"Loading shapefile: {shapefile_path}")

    # Run ogr2ogr to load shapefile
    cmd = [
        'ogr2ogr',
        '-f', 'PostgreSQL',
        pg_conn,
        shapefile_path,
        '-nln', 'model_network',
        '-lco', 'GEOMETRY_NAME=geom',
        '-lco', 'FID=gid',
        '-t_srs', 'EPSG:4326',
        '-overwrite'
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error loading shapefile: {result.stderr}")
        sys.exit(1)

    # Get count of loaded records
    db_url = get_db_url()
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM model_network")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(f"\nNetwork successfully loaded!")
    print(f"Total records: {count}")


def run_conflation(sql_file="conflate_speed_data.sql"):
    """Run the conflation SQL script."""
    print("\n" + "="*60)
    print("STEP 3: Running Speed Data Conflation")
    print("="*60 + "\n")

    if not os.path.exists(sql_file):
        print(f"Error: {sql_file} not found!")
        sys.exit(1)

    # Get database connection details
    host = os.getenv('POSTGRES_HOST')
    user = os.getenv('POSTGRES_USER')
    db_name = os.getenv('POSTGRES_DB')

    print(f"Running SQL script: {sql_file}")
    print("This may take several minutes...")

    # Run psql to execute the SQL file
    cmd = [
        'psql',
        '-U', user,
        '-h', host,
        '-d', db_name,
        '-f', sql_file,
        '-q'  # Quiet mode - only show result tables
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error running conflation: {result.stderr}")
        sys.exit(1)

    print(f"\nConflation completed successfully!")


def calculate_lts(sql_file="calculate_lts.sql"):
    """Calculate Level of Traffic Stress."""
    print("\n" + "="*60)
    print("STEP 4: Calculating Level of Traffic Stress (LTS)")
    print("="*60 + "\n")

    if not os.path.exists(sql_file):
        print(f"Error: {sql_file} not found!")
        sys.exit(1)

    # Get database connection details
    host = os.getenv('POSTGRES_HOST')
    user = os.getenv('POSTGRES_USER')
    db_name = os.getenv('POSTGRES_DB')

    print(f"Running SQL script: {sql_file}")
    print("This may take a minute...")

    # Run psql to execute the SQL file
    cmd = [
        'psql',
        '-U', user,
        '-h', host,
        '-d', db_name,
        '-f', sql_file,
        '-q'  # Quiet mode
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error running LTS calculation: {result.stderr}")
        sys.exit(1)

    # Show summary
    if result.stdout:
        lines = result.stdout.strip().split('\n')
        # Find and print the summary tables
        in_summary = False
        for line in lines:
            if 'LTS Calculation Summary' in line or in_summary:
                in_summary = True
                print(line)

    print(f"\nLTS calculation completed successfully!")


def cleanup_large_files():
    """Remove large temporary files after data is loaded."""
    print("\n" + "="*60)
    print("STEP 5: Cleanup")
    print("="*60 + "\n")

    geojson_file = "overture_roads.geojson"
    if os.path.exists(geojson_file):
        file_size = os.path.getsize(geojson_file) / (1024**3)  # Convert to GB
        print(f"Removing {geojson_file} ({file_size:.2f} GB)...")
        os.remove(geojson_file)
        print("File removed successfully!")
    else:
        print(f"{geojson_file} not found, skipping cleanup.")


def main():
    """Run the complete workflow."""
    print("\n" + "="*60)
    print("GIS LTS Calculation - Complete Workflow")
    print("="*60)

    try:
        setup_database()

        load_overture_roads()

        load_network()

        run_conflation()

        calculate_lts()

        cleanup_large_files()

        print("\n" + "="*60)
        print("ALL STEPS COMPLETE!")
        print("="*60)
        print("\nYour data is ready:")
        print("  - overture_roads table: Overture Maps road data")
        print("  - model network table: model network from shapefile")
        print("  - output.network_with_speed: model network with conflated speed data")
        print("  - output.network_with_lts: Final table with LTS scores")
        print("\nQuery output.network_with_lts to access the final results.")

    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
