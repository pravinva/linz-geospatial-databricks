-- Databricks notebook source
-- MAGIC %md
-- MAGIC # NZTA Geospatial Demo - Spatial SQL Queries
-- MAGIC
-- MAGIC This notebook demonstrates Databricks Spatial SQL capabilities using the LINZ data ingested in the previous step.
-- MAGIC
-- MAGIC **Tables available:**
-- MAGIC - `nzta_geo_demo.linz.road_centrelines` - Road network data
-- MAGIC - `nzta_geo_demo.linz.address_points` - Property addresses
-- MAGIC - `nzta_geo_demo.linz.localities` - Suburb/locality boundaries
-- MAGIC
-- MAGIC **Key Databricks spatial functions used:**
-- MAGIC - `ST_GEOMFROMTEXT()` - Convert WKT string to GEOMETRY type
-- MAGIC - `ST_DISTANCE()` - Calculate distance between geometries
-- MAGIC - `ST_INTERSECTS()` - Test if geometries overlap
-- MAGIC - `ST_LENGTH()` - Calculate length of linestrings
-- MAGIC - `ST_BUFFER()` - Create buffer around geometries

-- COMMAND ----------

-- Set context
USE CATALOG nzta_geo_demo;
USE SCHEMA linz;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 1: Count Roads by Type
-- MAGIC
-- MAGIC **Purpose:** Understand the composition of the road network by road type (motorway, arterial, local, etc.)
-- MAGIC
-- MAGIC **Why it matters for NZTA:** Network classification is fundamental for maintenance planning, funding allocation, and performance monitoring. Different road types have different service levels and maintenance requirements.

-- COMMAND ----------

SELECT
  road_type,
  COUNT(*) as segment_count,
  ROUND(SUM(ST_LENGTH(ST_TRANSFORM(ST_GEOMFROMTEXT(geom_wkt), 'EPSG:4326', 'EPSG:2193'))) / 1000, 2) as total_km
FROM road_centrelines
WHERE geom_wkt IS NOT NULL
  AND road_type IS NOT NULL
GROUP BY road_type
ORDER BY total_km DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 2: Find Addresses Near Roads
-- MAGIC
-- MAGIC **Purpose:** Identify address points within 200 metres of roads in Wellington CBD
-- MAGIC
-- MAGIC **Why it matters for NZTA:** Road authorities need to understand which properties are affected by road works, noise pollution, or access issues. This pattern is critical for stakeholder engagement and environmental impact assessment.

-- COMMAND ----------

-- Wellington CBD approximate centre point
-- Using ST_DISTANCE to find addresses within 200m of any road

WITH wellington_cbd AS (
  SELECT ST_GEOMFROMTEXT('POINT(174.776 -41.287)') as cbd_point
),
nearby_roads AS (
  SELECT
    r.full_road_name as road_name,
    r.road_type,
    ST_GEOMFROMTEXT(r.geom_wkt) as road_geom
  FROM road_centrelines r
  CROSS JOIN wellington_cbd w
  WHERE ST_DISTANCE(ST_GEOMFROMTEXT(r.geom_wkt), w.cbd_point) < 2000
    AND r.geom_wkt IS NOT NULL
)
SELECT
  a.full_address,
  a.suburb_locality,
  r.road_name,
  r.road_type,
  ROUND(ST_DISTANCE(ST_GEOMFROMTEXT(a.geom_wkt), r.road_geom), 1) as distance_metres
FROM address_points a
CROSS JOIN nearby_roads r
WHERE ST_DISTANCE(ST_GEOMFROMTEXT(a.geom_wkt), r.road_geom) <= 200
  AND a.geom_wkt IS NOT NULL
LIMIT 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 3: Count Addresses per Locality
-- MAGIC
-- MAGIC **Purpose:** Spatial join to aggregate address density by suburb/locality
-- MAGIC
-- MAGIC **Why it matters for NZTA:** Understanding population density patterns helps prioritize infrastructure investment. High-density localities may need more frequent public transport, better cycling infrastructure, or upgraded traffic signals.

-- COMMAND ----------

SELECT
  l.name as locality_name,
  COUNT(DISTINCT a.full_address) as address_count,
  ROUND(ST_AREA(ST_TRANSFORM(ST_GEOMFROMTEXT(l.geom_wkt), 'EPSG:4326', 'EPSG:2193')) / 1000000, 2) as area_sq_km,
  ROUND(COUNT(DISTINCT a.full_address) / (ST_AREA(ST_TRANSFORM(ST_GEOMFROMTEXT(l.geom_wkt), 'EPSG:4326', 'EPSG:2193')) / 1000000), 0) as addresses_per_sq_km
FROM localities l
LEFT JOIN address_points a
  ON ST_INTERSECTS(ST_GEOMFROMTEXT(a.geom_wkt), ST_GEOMFROMTEXT(l.geom_wkt))
WHERE l.geom_wkt IS NOT NULL
  AND l.name IS NOT NULL
GROUP BY l.name, l.geom_wkt
HAVING address_count > 0
ORDER BY address_count DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 4: Find Longest Road Segments
-- MAGIC
-- MAGIC **Purpose:** Identify the longest continuous road segments in the network
-- MAGIC
-- MAGIC **Why it matters for NZTA:** Long road segments often represent strategic corridors (State Highways, arterials). These are priority assets for maintenance planning and require coordinated management across multiple regions.

-- COMMAND ----------

SELECT
  full_road_name as road_name,
  road_type,
  ROUND(ST_LENGTH(ST_TRANSFORM(ST_GEOMFROMTEXT(geom_wkt), 'EPSG:4326', 'EPSG:2193')) / 1000, 2) as length_km,
  geom_wkt
FROM road_centrelines
WHERE geom_wkt IS NOT NULL
  AND full_road_name IS NOT NULL
ORDER BY ST_LENGTH(ST_TRANSFORM(ST_GEOMFROMTEXT(geom_wkt), 'EPSG:4326', 'EPSG:2193')) DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 5: Roads Intersecting Multiple Localities
-- MAGIC
-- MAGIC **Purpose:** Identify roads that cross multiple suburb boundaries
-- MAGIC
-- MAGIC **Why it matters for NZTA:** Cross-boundary roads require coordination between multiple local councils and NZTA. Understanding these connections is essential for regional transport planning and governance.

-- COMMAND ----------

WITH road_locality_intersections AS (
  SELECT
    r.full_road_name as road_name,
    r.road_type,
    l.name as locality_name,
    ST_GEOMFROMTEXT(r.geom_wkt) as road_geom
  FROM road_centrelines r
  INNER JOIN localities l
    ON ST_INTERSECTS(ST_GEOMFROMTEXT(r.geom_wkt), ST_GEOMFROMTEXT(l.geom_wkt))
  WHERE r.full_road_name IS NOT NULL
    AND l.name IS NOT NULL
    AND r.geom_wkt IS NOT NULL
)
SELECT
  road_name,
  road_type,
  COUNT(DISTINCT locality_name) as locality_count,
  COLLECT_SET(locality_name) as localities_crossed
FROM road_locality_intersections
GROUP BY road_name, road_type
HAVING locality_count > 1
ORDER BY locality_count DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary
-- MAGIC
-- MAGIC This notebook demonstrated core spatial SQL patterns for road network analysis:
-- MAGIC
-- MAGIC 1. **Network classification** - Understanding road hierarchy
-- MAGIC 2. **Proximity analysis** - Finding nearby features (addresses to roads)
-- MAGIC 3. **Spatial aggregation** - Counting features by area (addresses per locality)
-- MAGIC 4. **Geometric measurement** - Calculating road lengths
-- MAGIC 5. **Spatial joins** - Identifying cross-boundary relationships
-- MAGIC
-- MAGIC **Next steps:**
-- MAGIC - Run notebook `04_h3_indexing` to add H3 spatial indexing for faster queries
-- MAGIC - Run notebook `05_dynamic_segmentation` to see how asset attributes are joined to road networks