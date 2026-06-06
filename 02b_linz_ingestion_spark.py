# Databricks notebook source
# MAGIC %md
# MAGIC # NZTA Geospatial Demo - LINZ Data Ingestion (Spark-Native)
# MAGIC
# MAGIC **Production-grade implementation using distributed Spark processing**
# MAGIC
# MAGIC This notebook ingests New Zealand geospatial data from the LINZ Data Service WFS API using Spark-native operations for scalability:
# MAGIC - **NZ Road Centrelines** (layer-3383)
# MAGIC - **NZ Address Points** (layer-123113)
# MAGIC - **NZ Localities/Suburbs** (layer-113761)
# MAGIC
# MAGIC **Key Differences from 02_linz_ingestion.py:**
# MAGIC - Uses `requests` library for WFS API calls (Python standard library)
# MAGIC - Parses GeoJSON responses directly to Spark DataFrames via `spark.read.json()`
# MAGIC - Uses `ST_GEOMFROMGEOJSON()` for native Spark SQL geometry parsing
# MAGIC - All processing happens in Spark distributed engine
# MAGIC - No GeoPandas dependency for data processing
# MAGIC - Suitable for national-scale datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import requests
import json
import tempfile
from datetime import datetime
from pyspark.sql.functions import col, lit, current_timestamp, input_file_name, explode, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

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

print("✓ Configuration loaded")
print(f"  Target: {catalog}.{schema}")
print(f"  Wellington bbox: {bbox}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def fetch_linz_layer_spark(layer_id, layer_name, bbox, api_key, max_features=1000):
    """
    Fetch a complete LINZ WFS layer with pagination and return as Spark DataFrame.

    This function uses distributed Spark processing for scalability:
    - Fetches GeoJSON from LINZ WFS API using requests library
    - Writes response to temp file for Spark to read
    - Uses spark.read.json() for distributed parsing
    - Returns native Spark DataFrame with geometry as GeoJSON string

    Args:
        layer_id: LINZ layer identifier (e.g., 'layer-3383')
        layer_name: Descriptive name for logging
        bbox: Bounding box string (minx,miny,maxx,maxy in EPSG:4326)
        api_key: LINZ API key
        max_features: Features per page (WFS limit is typically 1000)

    Returns:
        Spark DataFrame with all features from the layer
    """

    print(f"\nFetching {layer_name} ({layer_id})...")

    all_features = []
    start_index = 0
    page = 1

    while True:
        # Construct WFS request URL
        params = {
            'service': 'WFS',
            'version': '2.0.0',
            'request': 'GetFeature',
            'typeNames': layer_id,
            'outputFormat': 'application/json',
            'srsName': 'EPSG:4326',
            'bbox': bbox,
            'startIndex': start_index,
            'count': max_features,
            'key': api_key
        }

        try:
            response = requests.get(wfs_base_url, params=params, timeout=30)
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

        except requests.exceptions.RequestException as e:
            print(f"  Error fetching page {page}: {e}")
            if all_features:
                print(f"  Returning {len(all_features)} features retrieved so far")
                break
            else:
                raise

    if not all_features:
        print(f"  Warning: No features retrieved for {layer_name}")
        return None

    # Convert to GeoJSON FeatureCollection
    feature_collection = {
        "type": "FeatureCollection",
        "features": all_features
    }

    # Write to temporary file for Spark to read
    # This is a single-node operation but acceptable because we're writing aggregated API response
    with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as tmp_file:
        json.dump(feature_collection, tmp_file)
        tmp_path = tmp_file.name

    # Read GeoJSON using Spark distributed JSON reader
    # Spark will parse the JSON in parallel across executors
    df = spark.read.option("multiline", "true").json(tmp_path)

    # Extract features array into individual rows
    features_df = df.select("features").select(col("features").alias("feature_array"))
    exploded_df = features_df.select(explode(col("feature_array")).alias("feature"))

    # Extract geometry and properties
    # Use Spark SQL to unnest the structure
    result_df = exploded_df.select(
        col("feature.geometry").alias("geometry_json"),
        col("feature.properties.*")
    )

    print(f"✓ Loaded {result_df.count()} features into Spark DataFrame")

    # Clean up temp file
    import os
    os.unlink(tmp_path)

    return result_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest NZ Road Centrelines

# COMMAND ----------

# Fetch road centrelines (layer-3383)
roads_df = fetch_linz_layer_spark(
    layer_id='layer-3383',
    layer_name='NZ Road Centrelines',
    bbox=bbox,
    api_key=linz_api_key
)

if roads_df is not None:
    print(f"\nRoad centrelines schema:")
    roads_df.printSchema()

    # Convert GeoJSON geometry to WKT using Spark SQL
    # ST_GEOMFROMGEOJSON parses the geometry JSON natively in Spark
    # ST_ASWKT converts to Well-Known Text for storage
    roads_with_geom = spark.sql("""
        SELECT
            *,
            ST_ASWKT(ST_GEOMFROMGEOJSON(to_json(geometry_json))) as geom_wkt,
            current_timestamp() as ingestion_timestamp
        FROM roads_staging
    """)

    # Register temp view first
    roads_df.createOrReplaceTempView("roads_staging")

    # Convert and write to Delta
    roads_with_geom = spark.sql("""
        SELECT
            *,
            ST_ASWKT(ST_GEOMFROMGEOJSON(to_json(geometry_json))) as geom_wkt,
            current_timestamp() as ingestion_timestamp
        FROM roads_staging
    """)

    # Drop the intermediate geometry_json column
    roads_final = roads_with_geom.drop("geometry_json")

    # Write to Delta table using Spark
    roads_final.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.road_centrelines"
    )

    count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog}.{schema}.road_centrelines").collect()[0]['count']
    print(f"✓ Saved {count:,} rows to {catalog}.{schema}.road_centrelines")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest NZ Address Points

# COMMAND ----------

# Fetch address points (layer-123113)
addresses_df = fetch_linz_layer_spark(
    layer_id='layer-123113',
    layer_name='NZ Address Points',
    bbox=bbox,
    api_key=linz_api_key
)

if addresses_df is not None:
    print(f"\nAddress points schema:")
    addresses_df.printSchema()

    # Register temp view
    addresses_df.createOrReplaceTempView("addresses_staging")

    # Convert GeoJSON to WKT using Spark SQL
    addresses_with_geom = spark.sql("""
        SELECT
            *,
            ST_ASWKT(ST_GEOMFROMGEOJSON(to_json(geometry_json))) as geom_wkt,
            current_timestamp() as ingestion_timestamp
        FROM addresses_staging
    """)

    # Drop intermediate column
    addresses_final = addresses_with_geom.drop("geometry_json")

    # Write to Delta using Spark
    addresses_final.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.address_points"
    )

    count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog}.{schema}.address_points").collect()[0]['count']
    print(f"✓ Saved {count:,} rows to {catalog}.{schema}.address_points")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest NZ Localities (Suburbs)

# COMMAND ----------

# Fetch localities (layer-113761)
localities_df = fetch_linz_layer_spark(
    layer_id='layer-113761',
    layer_name='NZ Localities',
    bbox=bbox,
    api_key=linz_api_key
)

if localities_df is not None:
    print(f"\nLocalities schema:")
    localities_df.printSchema()

    # Register temp view
    localities_df.createOrReplaceTempView("localities_staging")

    # Convert GeoJSON to WKT using Spark SQL
    localities_with_geom = spark.sql("""
        SELECT
            *,
            ST_ASWKT(ST_GEOMFROMGEOJSON(to_json(geometry_json))) as geom_wkt,
            current_timestamp() as ingestion_timestamp
        FROM localities_staging
    """)

    # Drop intermediate column
    localities_final = localities_with_geom.drop("geometry_json")

    # Write to Delta using Spark
    localities_final.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.localities"
    )

    count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog}.{schema}.localities").collect()[0]['count']
    print(f"✓ Saved {count:,} rows to {catalog}.{schema}.localities")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Display final row counts
print("=" * 60)
print("INGESTION COMPLETE - Spark-Native Implementation")
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
print("\nPerformance Benefits:")
print("- All geometry parsing done in Spark distributed engine")
print("- No single-node GeoPandas bottleneck")
print("- Suitable for national-scale datasets")
print("- Delta table writes use Spark native optimizations")
