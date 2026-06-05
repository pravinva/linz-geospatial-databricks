# Databricks notebook source
# MAGIC %md
# MAGIC # NZTA Geospatial Demo - Kepler.gl Visualization
# MAGIC
# MAGIC This notebook visualizes the NZTA demo data using Kepler.gl - an open-source geospatial visualization tool built by Uber.
# MAGIC
# MAGIC **Kepler.gl Features:**
# MAGIC - Interactive web-based maps with large dataset support
# MAGIC - Multiple layer types (points, lines, polygons, hexbins, arcs)
# MAGIC - Customizable styling and filtering
# MAGIC - Renders directly in Databricks notebooks
# MAGIC
# MAGIC **Layers in this demo:**
# MAGIC 1. Road centrelines (colored by road type)
# MAGIC 2. Address points
# MAGIC 3. Road condition by locality (colored by condition score)
# MAGIC
# MAGIC This is the same visualization pattern used in the TfNSW vehicle delay analysis showcased in Databricks' Geospatial Overview deck.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Install keplergl if not already installed
%pip install keplergl --quiet

# COMMAND ----------

# Restart Python to load the installed package
dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import geopandas as gpd
from keplergl import KeplerGl
from shapely.wkt import loads as wkt_loads

# Set catalog and schema
catalog = "nzta_geo_demo"
schema = "linz"

print("✓ Libraries loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Data from Delta Tables

# COMMAND ----------

# Load road centrelines
print("Loading road centrelines...")
roads_spark = spark.table(f"{catalog}.{schema}.road_centrelines")
roads_pd = roads_spark.select("road_name", "road_type", "geom_wkt").limit(2000).toPandas()

# Convert WKT to geometry
roads_pd['geometry'] = roads_pd['geom_wkt'].apply(lambda x: wkt_loads(x) if x else None)
roads_gdf = gpd.GeoDataFrame(roads_pd, geometry='geometry', crs="EPSG:4326")
roads_gdf = roads_gdf[['road_name', 'road_type', 'geometry']]  # Keep only needed columns

print(f"✓ Loaded {len(roads_gdf)} road centreline features")

# COMMAND ----------

# Load address points
print("Loading address points...")
addresses_spark = spark.table(f"{catalog}.{schema}.address_points")
addresses_pd = addresses_spark.select("full_address", "suburb_locality", "geom_wkt").limit(5000).toPandas()

# Convert WKT to geometry
addresses_pd['geometry'] = addresses_pd['geom_wkt'].apply(lambda x: wkt_loads(x) if x else None)
addresses_gdf = gpd.GeoDataFrame(addresses_pd, geometry='geometry', crs="EPSG:4326")
addresses_gdf = addresses_gdf[['full_address', 'suburb_locality', 'geometry']]

print(f"✓ Loaded {len(addresses_gdf)} address point features")

# COMMAND ----------

# Load road condition by locality
print("Loading road condition data...")
condition_spark = spark.table(f"{catalog}.{schema}.road_condition_by_locality")
condition_pd = condition_spark.select("road_name", "road_type", "locality_name", "condition_score", "geom_wkt").limit(2000).toPandas()

# Convert WKT to geometry
condition_pd['geometry'] = condition_pd['geom_wkt'].apply(lambda x: wkt_loads(x) if x else None)
condition_gdf = gpd.GeoDataFrame(condition_pd, geometry='geometry', crs="EPSG:4326")
condition_gdf = condition_gdf[['road_name', 'road_type', 'locality_name', 'condition_score', 'geometry']]

print(f"✓ Loaded {len(condition_gdf)} road condition features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Kepler.gl Map

# COMMAND ----------

# Initialize Kepler.gl map with custom configuration
config = {
    'version': 'v1',
    'config': {
        'mapState': {
            'latitude': -41.28,
            'longitude': 174.78,
            'zoom': 11
        }
    }
}

# Create map instance
map_1 = KeplerGl(height=800, config=config)

# Add layers to the map
map_1.add_data(data=roads_gdf, name='Road Centrelines')
map_1.add_data(data=addresses_gdf, name='Address Points')
map_1.add_data(data=condition_gdf, name='Road Condition')

print("✓ Map created with 3 layers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Display Interactive Map

# COMMAND ----------

# Display the map inline
# Note: Kepler.gl renders in the notebook output
map_1

# COMMAND ----------

# Alternative: Display using displayHTML for better notebook compatibility
# Uncomment the following if the above doesn't render properly

# html_content = map_1._repr_html_()
# displayHTML(html_content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Layer Configuration Guide
# MAGIC
# MAGIC **To customize the map layers after rendering:**
# MAGIC
# MAGIC ### Layer 1: Road Centrelines
# MAGIC - **Type**: LineString
# MAGIC - **Color**: By `road_type` field (categorical)
# MAGIC   - Motorway: Red
# MAGIC   - Arterial: Orange
# MAGIC   - Local: Gray
# MAGIC - **Width**: 2-3 pixels
# MAGIC - **Opacity**: 80%
# MAGIC
# MAGIC ### Layer 2: Address Points
# MAGIC - **Type**: Point
# MAGIC - **Color**: Blue (uniform)
# MAGIC - **Radius**: 3-5 pixels
# MAGIC - **Opacity**: 60%
# MAGIC - Use **Cluster** mode to aggregate dense areas
# MAGIC
# MAGIC ### Layer 3: Road Condition
# MAGIC - **Type**: LineString
# MAGIC - **Color**: By `condition_score` field (quantitative)
# MAGIC   - Score 1-2 (Poor): Red
# MAGIC   - Score 3 (Fair): Yellow
# MAGIC   - Score 4-5 (Good): Green
# MAGIC - **Width**: 3-4 pixels
# MAGIC - **Opacity**: 90%
# MAGIC
# MAGIC **Interactive features:**
# MAGIC - Click features to see attributes in tooltip
# MAGIC - Use filters to show/hide by road type or condition score
# MAGIC - Export map as HTML or PNG for reports

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Save Map Configuration (Optional)

# COMMAND ----------

# Save the map configuration for reuse
# This captures the current layer styling and filters
map_config = map_1.config

# You can save this config to a file or database for later use
print("Map configuration:")
print(map_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connection to TfNSW Use Case
# MAGIC
# MAGIC **This visualization pattern is identical to the TfNSW (Transport for NSW) vehicle delay analysis shown in the Databricks Geospatial Overview presentation:**
# MAGIC
# MAGIC | TfNSW Use Case | NZTA Demo Equivalent |
# MAGIC |----------------|---------------------|
# MAGIC | Road network layer | NZ road centrelines |
# MAGIC | Vehicle probe data (GPS points) | NZ address points |
# MAGIC | Delay/congestion scores by road segment | Pavement condition scores by road segment |
# MAGIC | Color coding: red=delay, green=free-flow | Color coding: red=poor, green=good |
# MAGIC | Kepler.gl interactive exploration | Same tool, same workflow |
# MAGIC
# MAGIC **Key insight:** The same geospatial stack (Delta Lake + Spatial SQL + Kepler.gl) works for both:
# MAGIC - **Operational analytics** (real-time traffic congestion)
# MAGIC - **Asset management** (pavement condition tracking)
# MAGIC
# MAGIC NZTA can use this pattern for multiple use cases:
# MAGIC - Traffic flow analysis (like TfNSW)
# MAGIC - Crash hotspot mapping
# MAGIC - Road surface condition trends
# MAGIC - Bridge and culvert asset inventory

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook demonstrated Kepler.gl visualization for NZTA geospatial data:
# MAGIC
# MAGIC 1. **Data loading** - Converted Delta tables to GeoDataFrames
# MAGIC 2. **Multi-layer map** - Roads, addresses, and condition scores
# MAGIC 3. **Interactive exploration** - Click, filter, and style features
# MAGIC 4. **Industry pattern** - Same approach used by TfNSW and other transport agencies
# MAGIC
# MAGIC **Visualization best practices:**
# MAGIC - Limit data to viewport area (use spatial filters in queries)
# MAGIC - Use clustering for dense point layers
# MAGIC - Export static images for reports, keep interactive maps for exploratory analysis
# MAGIC
# MAGIC **Next step:** Run the Genie space setup (Prompt 7) to enable natural language queries over this data
