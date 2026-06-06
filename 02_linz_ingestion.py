# Databricks notebook source
# MAGIC %md
# MAGIC # NZTA Geospatial Demo - LINZ Data Ingestion
# MAGIC
# MAGIC This notebook ingests New Zealand geospatial data from the LINZ Data Service WFS API:
# MAGIC - **NZ Road Centrelines** (layer-3383)
# MAGIC - **NZ Address Points** (layer-123113)
# MAGIC - **NZ Localities/Suburbs** (layer-113761)
# MAGIC
# MAGIC **Data Coverage:** Wellington region (bbox: 174.5,-41.5,175.0,-41.0)
# MAGIC
# MAGIC The data is loaded into Unity Catalog Delta tables with:
# MAGIC - Geometry stored as WKT (Well-Known Text) strings
# MAGIC - All original LINZ attributes preserved
# MAGIC - Ingestion timestamp for tracking

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import requests
%pip install geopandas

from datetime import datetime
import pandas as pd
from shapely.geometry import shape
import time

# Configuration
catalog = "nzta_geo_demo"
schema = "linz"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# Retrieve LINZ API key
linz_api_key = dbutils.secrets.get(scope="linz", key="api_key")

# Wellington bounding box (EPSG:4326)
bbox = "174.5,-41.5,175.0,-41.0"

# LINZ WFS base URL
wfs_base_url = "https://data.linz.govt.nz/services/wfs"

print(f"✓ Configuration loaded")
print(f"  Target: {catalog}.{schema}")
print(f"  Wellington bbox: {bbox}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def fetch_linz_layer(layer_id, layer_name, bbox, api_key, max_features=1000):
    """
    Fetch a complete LINZ WFS layer with pagination handling.

    Args:
        layer_id: LINZ layer identifier (e.g., 'layer-3383')
        layer_name: Descriptive name for logging
        bbox: Bounding box string (minx,miny,maxx,maxy in EPSG:4326 lon/lat order)
        api_key: LINZ API key
        max_features: Features per page (WFS limit is typically 1000)

    Returns:
        GeoDataFrame with all features from the layer
    """

    all_features = []
    start_index = 0
    page = 1

    # Strip whitespace/newlines from API key (secrets may have trailing newline)
    api_key = api_key.strip()

    # LINZ requires key in the URL path, not as a query parameter
    layer_url = f"https://data.linz.govt.nz/services;key={api_key}/wfs/{layer_id}"

    # WFS 2.0.0 with EPSG:4326 uses lat/lon axis order for bbox
    # Input bbox is minx,miny,maxx,maxy (lon/lat) -> convert to miny,minx,maxy,maxx (lat/lon)
    parts = bbox.split(',')
    bbox_latlon = f"{parts[1]},{parts[0]},{parts[3]},{parts[2]}"

    print(f"\nFetching {layer_name} ({layer_id})...")

    while True:
        # Construct WFS request parameters
        params = {
            'service': 'WFS',
            'version': '2.0.0',
            'request': 'GetFeature',
            'typeNames': f'data.linz.govt.nz:{layer_id}',
            'outputFormat': 'application/json',
            'srsName': 'EPSG:4326',
            'bbox': bbox_latlon,
            'startIndex': start_index,
            'count': max_features
        }

        try:
            response = requests.get(layer_url, params=params, timeout=30)
            response.raise_for_status()

            geojson = response.json()
            features = geojson.get('features', [])

            if not features:
                print(f"  Page {page}: No more features (total: {len(all_features)})")
                break

            all_features.extend(features)
            print(f"  Page {page}: Retrieved {len(features)} features (total: {len(all_features)})")

            # Check if we've received fewer features than requested (last page)
            if len(features) < max_features:
                print(f"  Completed: {len(all_features)} total features")
                break

            start_index += max_features
            page += 1

            # Small delay to be respectful to the API
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            print(f"  Error fetching page {page}: {e}")
            if all_features:
                print(f"  Returning {len(all_features)} features retrieved so far")
                break
            else:
                raise

    # Convert to GeoDataFrame
    if all_features:
        gdf = gpd.GeoDataFrame.from_features(all_features, crs="EPSG:4326")
        return gdf
    else:
        print(f"  Warning: No features retrieved for {layer_name}")
        return None

# COMMAND ----------

def save_to_delta(gdf, table_name, catalog, schema):
    """
    Save GeoDataFrame to Delta table with geometry as WKT and ingestion timestamp.

    Args:
        gdf: GeoDataFrame to save
        table_name: Name of the target Delta table
        catalog: Unity Catalog name
        schema: Schema name
    """

    if gdf is None or len(gdf) == 0:
        print(f"  Skipping {table_name}: No data to save")
        return

    # Create a copy to avoid modifying original
    df = gdf.copy()

    # Convert geometry to WKT string
    df['geom_wkt'] = df.geometry.apply(lambda geom: geom.wkt if geom else None)

    # Drop the original geometry column (can't serialize to Spark directly)
    df = df.drop(columns=['geometry'])

    # Add ingestion timestamp
    df['ingestion_timestamp'] = datetime.now()

    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(df)

    # Write to Delta table (overwrite mode for this demo)
    full_table_name = f"{catalog}.{schema}.{table_name}"
    spark_df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

    row_count = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}").collect()[0]['count']
    print(f"✓ Saved {row_count} rows to {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest NZ Road Centrelines

# COMMAND ----------

# Fetch road centrelines (layer-3383)
import geopandas as gpd

api_key = linz_api_key.strip()
layer_id = 'layer-3383'
layer_url = f"https://data.linz.govt.nz/services;key={api_key}/wfs/{layer_id}"

# WFS 2.0.0 with EPSG:4326 uses lat/lon axis order for bbox
bbox_latlon = '-41.5,174.5,-41.0,175.0'

all_features = []
start_index = 0
max_features = 1000
page = 1

print(f"Fetching NZ Road Centrelines ({layer_id})...")

while True:
    params = {
        'service': 'WFS',
        'version': '2.0.0',
        'request': 'GetFeature',
        'typeNames': f'data.linz.govt.nz:{layer_id}',
        'outputFormat': 'application/json',
        'srsName': 'EPSG:4326',
        'bbox': bbox_latlon,
        'startIndex': start_index,
        'count': max_features
    }

    response = requests.get(layer_url, params=params, timeout=30)
    response.raise_for_status()

    geojson = response.json()
    features = geojson.get('features', [])

    if not features:
        print(f"  Page {page}: No more features (total: {len(all_features)})")
        break

    all_features.extend(features)
    print(f"  Page {page}: Retrieved {len(features)} features (total: {len(all_features)})")

    if len(features) < max_features:
        print(f"  Completed: {len(all_features)} total features")
        break

    start_index += max_features
    page += 1
    time.sleep(0.5)

roads_gdf = gpd.GeoDataFrame.from_features(all_features, crs="EPSG:4326") if all_features else None

# Save to Delta
if roads_gdf is not None:
    print(f"\nRoad centrelines columns: {list(roads_gdf.columns)}")
    save_to_delta(roads_gdf, 'road_centrelines', catalog, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest NZ Address Points

# COMMAND ----------

# Fetch address points (layer-123113)
addresses_gdf = fetch_linz_layer(
    layer_id='layer-123113',
    layer_name='NZ Address Points',
    bbox=bbox,
    api_key=linz_api_key
)

# Save to Delta
if addresses_gdf is not None:
    print(f"\nAddress points columns: {list(addresses_gdf.columns)}")
    save_to_delta(addresses_gdf, 'address_points', catalog, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest NZ Localities (Suburbs)

# COMMAND ----------

# Fetch localities (layer-113764)
localities_gdf = fetch_linz_layer(
    layer_id='layer-113764',
    layer_name='NZ Suburbs and Localities',
    bbox=bbox,
    api_key=linz_api_key
)

# Save to Delta
if localities_gdf is not None:
    print(f"\nLocalities columns: {list(localities_gdf.columns)}")
    save_to_delta(localities_gdf, 'localities', catalog, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Display final row counts
print("=" * 60)
print("INGESTION COMPLETE")
print("=" * 60)

tables = ['road_centrelines', 'address_points', 'localities']

for table in tables:
    try:
        count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog}.{schema}.{table}").collect()[0]['count']
        print(f"✓ {catalog}.{schema}.{table}: {count:,} rows")
    except:
        print(f"✗ {catalog}.{schema}.{table}: Table not found")

print("=" * 60)
print("\nNext step: Run notebook 03_spatial_sql to query this data")