# Databricks notebook source
# MAGIC %md
# MAGIC # NZTA Geospatial Demo - Dynamic Segmentation
# MAGIC
# MAGIC This notebook demonstrates **dynamic segmentation** - the pattern of joining attribute datasets to road centrelines by spatial overlap.
# MAGIC
# MAGIC **What is Dynamic Segmentation?**
# MAGIC - In traditional GIS (ArcGIS), road attributes are linked to road segments using linear referencing (station/offset)
# MAGIC - In Databricks, we achieve this using spatial joins (ST_INTERSECTS) between road geometries and attribute zones
# MAGIC
# MAGIC **NZTA Use Cases:**
# MAGIC - **Pavement condition**: Assign IRI (roughness) values to road segments based on survey data
# MAGIC - **Speed zones**: Map legal speed limits to road sections
# MAGIC - **Asset inventory**: Link assets (signs, barriers, culverts) to road networks
# MAGIC - **Maintenance zones**: Aggregate maintenance contracts by road segment and locality
# MAGIC
# MAGIC This notebook simulates pavement condition scoring by locality.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import col, rand, round as sql_round, avg, count, collect_list
import random

# Set catalog and schema
catalog = "nzta_geo_demo"
schema = "linz"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print("✓ Environment configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Source Tables

# COMMAND ----------

# Load road centrelines
roads_df = spark.table(f"{catalog}.{schema}.road_centrelines")
print(f"Loaded {roads_df.count()} road centreline segments")

# Load localities
localities_df = spark.table(f"{catalog}.{schema}.localities")
print(f"Loaded {localities_df.count()} locality polygons")

# COMMAND ----------

# Show sample road data
display(roads_df.select("road_name", "road_type", "geom_wkt").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Spatial Join: Roads to Localities
# MAGIC
# MAGIC Identify which roads pass through which localities using ST_INTERSECTS.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Spatial join between roads and localities
# MAGIC CREATE OR REPLACE TEMP VIEW roads_by_locality AS
# MAGIC SELECT
# MAGIC   r.road_name,
# MAGIC   r.road_type,
# MAGIC   r.geom_wkt as road_geom_wkt,
# MAGIC   l.name as locality_name,
# MAGIC   l.geom_wkt as locality_geom_wkt
# MAGIC FROM road_centrelines r
# MAGIC INNER JOIN localities l
# MAGIC   ON ST_INTERSECTS(
# MAGIC     ST_GEOMFROMTEXT(r.geom_wkt),
# MAGIC     ST_GEOMFROMTEXT(l.geom_wkt)
# MAGIC   )
# MAGIC WHERE r.geom_wkt IS NOT NULL
# MAGIC   AND l.geom_wkt IS NOT NULL
# MAGIC   AND r.road_name IS NOT NULL;

# COMMAND ----------

# Count roads per locality
spark.sql("""
    SELECT
        locality_name,
        COUNT(DISTINCT road_name) as road_count
    FROM roads_by_locality
    GROUP BY locality_name
    ORDER BY road_count DESC
""").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Synthesize Pavement Condition Scores
# MAGIC
# MAGIC Create synthetic condition scores (1-5) to simulate pavement roughness data.
# MAGIC
# MAGIC **In Production:** This would be replaced with actual IRI (International Roughness Index) values from NZTA's pavement survey data.

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, expr

# Load roads_by_locality and add a synthetic condition score
roads_with_condition = spark.sql("""
    SELECT
        road_name,
        road_type,
        road_geom_wkt,
        locality_name
    FROM roads_by_locality
""")

# Add random condition score (1-5, where 5 is best condition)
roads_with_condition = roads_with_condition.withColumn(
    "condition_score",
    (rand() * 4 + 1).cast("int")  # Random integer 1-5
)

# Create temp view
roads_with_condition.createOrReplaceTempView("roads_with_condition")

print(f"✓ Added synthetic condition scores to {roads_with_condition.count()} road-locality pairs")

# COMMAND ----------

# Show sample with condition scores
display(roads_with_condition.select(
    "road_name", "road_type", "locality_name", "condition_score"
).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aggregate Condition Scores by Locality
# MAGIC
# MAGIC Calculate mean pavement condition per locality - this is the "gold layer" pattern.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW condition_by_locality AS
# MAGIC SELECT
# MAGIC   locality_name,
# MAGIC   COUNT(DISTINCT road_name) as road_segment_count,
# MAGIC   ROUND(AVG(condition_score), 2) as mean_condition_score,
# MAGIC   MIN(condition_score) as worst_condition,
# MAGIC   MAX(condition_score) as best_condition
# MAGIC FROM roads_with_condition
# MAGIC GROUP BY locality_name
# MAGIC ORDER BY mean_condition_score ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show localities by condition (worst first)
# MAGIC SELECT * FROM condition_by_locality LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Gold Layer: Road Condition by Locality
# MAGIC
# MAGIC This is the final analytical table that NZTA road managers would query for maintenance planning.

# COMMAND ----------

# Create the gold layer table with full detail
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}.road_condition_by_locality AS
    SELECT
        road_name,
        road_type,
        locality_name,
        condition_score,
        road_geom_wkt as geom_wkt,
        current_timestamp() as calculation_timestamp
    FROM roads_with_condition
    WHERE road_name IS NOT NULL
""")

count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog}.{schema}.road_condition_by_locality").collect()[0]['count']
print(f"✓ Created gold layer table: {catalog}.{schema}.road_condition_by_locality")
print(f"  Total records: {count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Analytical Queries on Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 1: Find roads in poor condition (score <= 2) that need urgent maintenance
# MAGIC SELECT
# MAGIC   road_name,
# MAGIC   road_type,
# MAGIC   locality_name,
# MAGIC   condition_score
# MAGIC FROM road_condition_by_locality
# MAGIC WHERE condition_score <= 2
# MAGIC ORDER BY condition_score ASC, road_type
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 2: Count roads by condition category
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN condition_score >= 4 THEN 'Good (4-5)'
# MAGIC     WHEN condition_score = 3 THEN 'Fair (3)'
# MAGIC     ELSE 'Poor (1-2)'
# MAGIC   END as condition_category,
# MAGIC   COUNT(*) as segment_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
# MAGIC FROM road_condition_by_locality
# MAGIC GROUP BY condition_category
# MAGIC ORDER BY
# MAGIC   CASE condition_category
# MAGIC     WHEN 'Poor (1-2)' THEN 1
# MAGIC     WHEN 'Fair (3)' THEN 2
# MAGIC     WHEN 'Good (4-5)' THEN 3
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 3: Maintenance priority by locality (worst average condition)
# MAGIC SELECT
# MAGIC   locality_name,
# MAGIC   COUNT(*) as total_road_segments,
# MAGIC   ROUND(AVG(condition_score), 2) as avg_condition,
# MAGIC   SUM(CASE WHEN condition_score <= 2 THEN 1 ELSE 0 END) as poor_segments,
# MAGIC   ROUND(SUM(CASE WHEN condition_score <= 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as pct_poor
# MAGIC FROM road_condition_by_locality
# MAGIC GROUP BY locality_name
# MAGIC HAVING COUNT(*) >= 5  -- Only localities with 5+ road segments
# MAGIC ORDER BY avg_condition ASC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Export Sample for Visualization

# COMMAND ----------

# Export a sample for the Kepler.gl visualization notebook
sample_for_viz = spark.sql(f"""
    SELECT
        road_name,
        road_type,
        locality_name,
        condition_score,
        geom_wkt
    FROM {catalog}.{schema}.road_condition_by_locality
    WHERE condition_score IS NOT NULL
""")

print(f"✓ Sample prepared: {sample_for_viz.count():,} records")
print("  This table will be visualized in notebook 06_kepler_visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mapping to NZTA's AAM Use Case
# MAGIC
# MAGIC **What we demonstrated:**
# MAGIC - Joined synthetic condition scores to road segments by locality
# MAGIC - Aggregated scores to identify maintenance priorities
# MAGIC - Created a gold layer table for analytical queries
# MAGIC
# MAGIC **How this maps to production:**
# MAGIC
# MAGIC | Demo Component | NZTA Production Equivalent |
# MAGIC |----------------|---------------------------|
# MAGIC | `condition_score` (1-5) | IRI values from pavement surveys (mm/km) |
# MAGIC | `locality_name` spatial join | NZTA road hierarchy (State Highway, region, section) |
# MAGIC | Random score generation | Historical IRI data loaded from RAMM/OneDrive |
# MAGIC | Single timestamp | Time-series data (monthly/quarterly surveys) |
# MAGIC | Wellington sample | National network (100,000+ km) |
# MAGIC
# MAGIC **Next steps for NZTA UC migration:**
# MAGIC 1. **Ingest real IRI data** from pavement survey contractors
# MAGIC 2. **Define road hierarchy** using NZTA's State Highway network classification
# MAGIC 3. **Add temporal dimension** to track pavement deterioration over time
# MAGIC 4. **Build predictive models** using ML to forecast maintenance needs
# MAGIC 5. **Integrate with asset management** to link pavement condition to funding allocations
# MAGIC
# MAGIC This pattern (spatial join → attribute enrichment → aggregation → gold layer) is the foundation of NZTA's Asset Analytics in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook demonstrated dynamic segmentation for NZTA use cases:
# MAGIC
# MAGIC 1. **Spatial join** - Linked road segments to localities
# MAGIC 2. **Attribute enrichment** - Added simulated pavement condition scores
# MAGIC 3. **Aggregation** - Calculated mean condition by locality
# MAGIC 4. **Gold layer** - Created analytical table for downstream queries
# MAGIC 5. **Priority analysis** - Identified roads needing urgent maintenance
# MAGIC
# MAGIC **Next step:** Run notebook `06_kepler_visualization` to visualize the condition data on an interactive map
