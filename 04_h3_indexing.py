# Databricks notebook source
# MAGIC %md
# MAGIC # NZTA Geospatial Demo - H3 Spatial Indexing
# MAGIC
# MAGIC This notebook adds H3 spatial indexing to the LINZ road and address data for scalable spatial analytics.
# MAGIC
# MAGIC **What is H3?**
# MAGIC - H3 is a hexagonal hierarchical geospatial indexing system developed by Uber
# MAGIC - It provides a uniform grid that enables fast spatial joins without complex geometry operations
# MAGIC - Resolution 9 hexagons are ~0.1 km² - suitable for urban analysis
# MAGIC
# MAGIC **Why H3 matters for NZTA:**
# MAGIC - Traditional spatial joins (ST_INTERSECTS) are computationally expensive at scale
# MAGIC - H3 indexing enables fast aggregation and joining of millions of records
# MAGIC - Ideal for time-series analysis (e.g., traffic volume, incident density over time)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import geopandas as gpd
import h3
import pandas as pd
from pyspark.sql.functions import col, udf, explode, array
from pyspark.sql.types import StringType, ArrayType, DoubleType, StructType, StructField
from shapely.wkt import loads as wkt_loads
from shapely.geometry import LineString, Point
import time

# Set catalog and schema
catalog = "nzta_geo_demo"
schema = "linz"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print("✓ Environment configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. H3 Index Address Points

# COMMAND ----------

# Load address points from Delta
addresses_df = spark.table(f"{catalog}.{schema}.address_points")
print(f"Loaded {addresses_df.count()} address points")

# COMMAND ----------

# Define UDF to extract lat/lon from WKT point and compute H3 index
def wkt_to_h3(wkt_string, resolution=9):
    """Convert WKT Point to H3 cell at specified resolution"""
    try:
        geom = wkt_loads(wkt_string)
        if geom.geom_type == 'Point':
            lat, lon = geom.y, geom.x
            h3_index = h3.geo_to_h3(lat, lon, resolution)
            return h3_index
        return None
    except:
        return None

# Register UDF
wkt_to_h3_udf = udf(wkt_to_h3, StringType())

# Add H3 index column
addresses_h3_df = addresses_df.withColumn("h3_r9", wkt_to_h3_udf(col("geom_wkt")))

# Filter out any null H3 values and save
addresses_h3_df = addresses_h3_df.filter(col("h3_r9").isNotNull())

addresses_h3_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{schema}.address_points_h3"
)

count = spark.table(f"{catalog}.{schema}.address_points_h3").count()
print(f"✓ Created address_points_h3 table with {count} rows")

# COMMAND ----------

# Show sample data
display(spark.table(f"{catalog}.{schema}.address_points_h3").select(
    "full_address", "suburb_locality", "geom_wkt", "h3_r9"
).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. H3 Index Road Centrelines (Tessellation)
# MAGIC
# MAGIC For linestrings, we need to tessellate them into multiple H3 cells that the road passes through.

# COMMAND ----------

# Load road centrelines
roads_df = spark.table(f"{catalog}.{schema}.road_centrelines")
print(f"Loaded {roads_df.count()} road centreline segments")

# COMMAND ----------

def linestring_to_h3_cells(wkt_string, resolution=9):
    """
    Tessellate a WKT LineString into H3 cells.
    Returns a list of H3 cell IDs that the linestring passes through.
    """
    try:
        geom = wkt_loads(wkt_string)
        if geom.geom_type != 'LineString':
            return []

        h3_cells = set()

        # Sample points along the linestring
        # Use a distance step of ~100m for resolution 9
        length = geom.length  # in degrees
        num_samples = max(int(length / 0.001), 10)  # At least 10 samples

        for i in range(num_samples + 1):
            point = geom.interpolate(float(i) / num_samples, normalized=True)
            h3_index = h3.geo_to_h3(point.y, point.x, resolution)
            h3_cells.add(h3_index)

        # Also index the start and end points
        start_h3 = h3.geo_to_h3(geom.coords[0][1], geom.coords[0][0], resolution)
        end_h3 = h3.geo_to_h3(geom.coords[-1][1], geom.coords[-1][0], resolution)
        h3_cells.add(start_h3)
        h3_cells.add(end_h3)

        return list(h3_cells)
    except Exception as e:
        return []

# Register UDF that returns array of H3 cells
linestring_to_h3_udf = udf(linestring_to_h3_cells, ArrayType(StringType()))

# COMMAND ----------

# Add H3 cells column (array)
roads_with_h3 = roads_df.withColumn("h3_cells", linestring_to_h3_udf(col("geom_wkt")))

# Explode to one row per H3 cell
roads_h3_exploded = roads_with_h3.withColumn("h3_r9", explode(col("h3_cells"))) \
    .drop("h3_cells") \
    .filter(col("h3_r9").isNotNull())

# Save to Delta
roads_h3_exploded.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{schema}.road_centrelines_h3"
)

count = spark.table(f"{catalog}.{schema}.road_centrelines_h3").count()
print(f"✓ Created road_centrelines_h3 table with {count} rows (exploded by H3 cell)")

# COMMAND ----------

# Show sample data
display(spark.table(f"{catalog}.{schema}.road_centrelines_h3").select(
    "road_name", "road_type", "h3_r9", "geom_wkt"
).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. H3 Aggregation Query - Address Density per Cell

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count addresses per H3 cell - instant aggregation
# MAGIC SELECT
# MAGIC   h3_r9,
# MAGIC   COUNT(*) as address_count
# MAGIC FROM address_points_h3
# MAGIC GROUP BY h3_r9
# MAGIC ORDER BY address_count DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Performance Comparison: H3 Join vs ST_INTERSECTS
# MAGIC
# MAGIC Compare join performance between H3-based join and traditional spatial intersection.

# COMMAND ----------

# MAGIC %md
# MAGIC ### H3-based Join (Fast)

# COMMAND ----------

start_time = time.time()

# H3 join - simple equality join
h3_join_result = spark.sql("""
    SELECT
        a.full_address,
        a.suburb_locality,
        r.road_name,
        r.road_type,
        a.h3_r9
    FROM address_points_h3 a
    INNER JOIN road_centrelines_h3 r
        ON a.h3_r9 = r.h3_r9
    WHERE r.road_name IS NOT NULL
""")

h3_count = h3_join_result.count()
h3_duration = time.time() - start_time

print(f"H3 Join Results: {h3_count} matches")
print(f"H3 Join Duration: {h3_duration:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Traditional Spatial Join (Slower)

# COMMAND ----------

start_time = time.time()

# Traditional ST_INTERSECTS with buffer
# Note: We'll use a small sample for this demo to keep it fast
spatial_join_result = spark.sql("""
    SELECT
        a.full_address,
        a.suburb_locality,
        r.road_name,
        r.road_type
    FROM address_points a
    INNER JOIN road_centrelines r
        ON ST_INTERSECTS(
            ST_BUFFER(ST_GEOMFROMTEXT(r.geom_wkt), 0.001),  -- ~100m buffer
            ST_GEOMFROMTEXT(a.geom_wkt)
        )
    WHERE r.road_name IS NOT NULL
    LIMIT 1000
""")

spatial_count = spatial_join_result.count()
spatial_duration = time.time() - start_time

print(f"Spatial Join Results: {spatial_count} matches (limited to 1000)")
print(f"Spatial Join Duration: {spatial_duration:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Summary

# COMMAND ----------

print("=" * 60)
print("H3 vs Traditional Spatial Join Performance")
print("=" * 60)
print(f"H3 Join:        {h3_count:,} results in {h3_duration:.2f} seconds")
print(f"Spatial Join:   {spatial_count:,} results in {spatial_duration:.2f} seconds (limited)")
print(f"\nSpeedup:        {spatial_duration / h3_duration:.1f}x faster with H3")
print("=" * 60)
print("\nKey Takeaway:")
print("H3 indexing enables fast spatial joins at scale by converting complex")
print("geometry operations into simple equality joins on hexagon IDs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook demonstrated H3 spatial indexing for NZTA use cases:
# MAGIC
# MAGIC 1. **Point indexing** - Address points assigned to H3 cells (resolution 9)
# MAGIC 2. **Linestring tessellation** - Roads decomposed into H3 cells they traverse
# MAGIC 3. **Fast aggregation** - Count addresses per cell without geometry operations
# MAGIC 4. **Performance gains** - H3 joins are significantly faster than ST_INTERSECTS
# MAGIC
# MAGIC **When to use H3:**
# MAGIC - Large datasets (millions of records)
# MAGIC - Time-series spatial analysis (e.g., traffic patterns over months)
# MAGIC - Grid-based aggregation (heatmaps, density analysis)
# MAGIC
# MAGIC **When to use ST_ functions:**
# MAGIC - Precise distance calculations
# MAGIC - Complex geometric operations (buffers, unions, etc.)
# MAGIC - Small datasets where exact geometry matters
# MAGIC
# MAGIC **Next step:** Run notebook `05_dynamic_segmentation` to see asset attribute joining
