# NZTA Geospatial Demo - Training Guide

Welcome to the NZTA Geospatial Demo for Databricks! This repository demonstrates how New Zealand Transport Agency (NZTA) can leverage Databricks Unity Catalog for road network analysis, asset management, and geospatial analytics.

## Overview

This demo showcases a complete geospatial analytics workflow using open data from Land Information New Zealand (LINZ):

- **Data ingestion** from LINZ Data Service WFS API
- **Spatial SQL queries** for road network analysis
- **H3 hexagonal indexing** for scalable spatial joins
- **Dynamic segmentation** for asset attribute management (pavement condition)
- **Interactive visualization** with Kepler.gl
- **Natural language queries** using Databricks Genie

**Why this matters for NZTA:**

NZTA manages 11,000+ km of State Highways and needs to analyze asset condition, traffic patterns, and maintenance priorities across the entire network. This demo shows how Databricks Unity Catalog can replace or complement traditional GIS tools (like ArcGIS) for:

- Pavement condition tracking and deterioration modeling
- Crash hotspot analysis and safety interventions
- Traffic flow analysis and congestion management
- Bridge and culvert asset inventory
- Capital works planning and budgeting

---

## Prerequisites

### 1. Databricks Workspace Access

You need access to a Databricks workspace (e2 field engineering workspace or equivalent) with:

- Unity Catalog enabled
- Ability to create catalogs and schemas
- Python notebook support
- SQL notebook support

If you don't have access, contact your Databricks administrator or solution architect.

### 2. LINZ API Key Setup

The demo ingests data from Land Information New Zealand (LINZ). You'll need a free API key:

**Step 1: Create LINZ Account**
1. Go to [https://data.linz.govt.nz](https://data.linz.govt.nz)
2. Click **Sign Up** (top right)
3. Create a free account using your work email

**Step 2: Generate API Key**
1. Log in to your LINZ account
2. Click your profile icon → **My API Keys**
3. Click **Create API Key**
4. Give it a name (e.g., "Databricks NZTA Demo")
5. Copy the generated key (you'll need this in Step 3)

**Step 3: Store in Databricks Secrets**

You need to store the API key securely in Databricks. Choose **one** of these methods:

**Option A: Using Databricks CLI** (recommended)
```bash
# Install Databricks CLI if you haven't already
pip install databricks-cli

# Configure authentication to your workspace
databricks configure --token

# Create secrets scope
databricks secrets create-scope linz

# Store your API key
databricks secrets put-secret linz api_key
# This will open a text editor - paste your API key, save, and close
```

**Option B: Using Workspace UI**
1. In Databricks workspace, click **Settings** (gear icon in sidebar)
2. Click **Secrets** → **Create Scope**
3. Scope name: `linz`
4. Click **Create**
5. Click **Add Secret**
6. Key name: `api_key`
7. Paste your LINZ API key in the value field
8. Click **Add**

✅ Once complete, your API key is securely stored and ready to use!

---

## Notebook Run Order

Run the notebooks in this sequence. Each notebook builds on the previous one:

| # | Notebook | Description | Time |
|---|----------|-------------|------|
| 1 | `01_workspace_setup.py` | Create Unity Catalog structure (catalog/schema), validate secrets, install libraries | 3 min |
| 2 | `02_linz_ingestion.py` | Ingest NZ roads, addresses, and localities from LINZ WFS API into Delta tables | 5-10 min |
| 3 | `03_spatial_sql.sql` | Run spatial queries using ST_ functions (distance, intersection, length) | 5 min |
| 4 | `04_h3_indexing.py` | Add H3 spatial indexing for fast aggregation and joins | 5 min |
| 5 | `05_dynamic_segmentation.py` | Simulate pavement condition scoring by locality (asset attribute joining) | 5 min |
| 6 | `06_kepler_visualization.py` | Visualize roads and condition scores on an interactive Kepler.gl map | 5 min |
| 7 | `07_genie_space_setup.md` | Instructions to set up Databricks Genie for natural language queries | 10 min |

**Total time:** ~40 minutes

---

## Key Concepts Explained (for GIS Analysts)

### Delta Tables

**What:** Delta Lake is an open-source storage format that brings reliability and performance to data lakes.

**GIS equivalent:** Like a file geodatabase in ArcGIS, but:
- Supports ACID transactions (no corrupt files from failed writes)
- Handles time-travel (query data as it existed yesterday, last week, etc.)
- Scales to petabyte-size datasets without performance degradation

**Example:** Your road centrelines are stored as a Delta table at `nzta_geo_demo.linz.road_centrelines`.

### Unity Catalog

**What:** A unified governance layer for data, ML models, and AI assets across Databricks.

**GIS equivalent:** Like an Enterprise Geodatabase with added data lineage, access control, and auditing.

**Structure:**
```
Catalog (e.g., nzta_geo_demo)
  └── Schema (e.g., linz)
      └── Tables (e.g., road_centrelines, address_points)
```

**Benefits:** Fine-grained permissions, automatic data lineage, cross-workspace sharing.

### ST_ Spatial Functions

**What:** SQL functions for spatial operations like distance, intersection, buffering, length.

**GIS equivalent:** Like geoprocessing tools in ArcGIS (Buffer, Intersect, Spatial Join), but expressed as SQL.

**Common functions:**
- `ST_GEOMFROMTEXT(wkt)` - Convert WKT string to geometry object
- `ST_DISTANCE(geom1, geom2)` - Calculate distance between two geometries
- `ST_INTERSECTS(geom1, geom2)` - Test if geometries overlap
- `ST_LENGTH(geom)` - Calculate length of a linestring
- `ST_BUFFER(geom, distance)` - Create buffer around a geometry

**Example:**
```sql
-- Find addresses within 200m of a road
SELECT a.full_address, r.road_name
FROM address_points a
INNER JOIN road_centrelines r
  ON ST_DISTANCE(
      ST_GEOMFROMTEXT(a.geom_wkt),
      ST_GEOMFROMTEXT(r.geom_wkt)
     ) <= 200
```

### H3 Hexagonal Indexing

**What:** A spatial indexing system that divides the Earth into hexagonal cells at multiple resolutions.

**GIS equivalent:** Like a fishnet grid in ArcGIS, but with hexagons (better for distance analysis) and hierarchical (cells nest perfectly at different scales).

**Why use H3:**
- **Fast spatial joins:** Instead of computing intersections between complex geometries, you just match hexagon IDs (simple equality join)
- **Consistent area:** All hexagons at the same resolution have roughly equal area
- **Aggregation:** Easy to count features per cell for heatmaps

**Resolution guide:**
- Resolution 5: ~250 km² per hexagon (regional scale)
- Resolution 7: ~5 km² per hexagon (city scale)
- Resolution 9: ~0.1 km² per hexagon (neighborhood scale) ← **We use this**
- Resolution 11: ~0.002 km² per hexagon (block scale)

**Example:**
```python
# Assign each address to a hexagon
h3_index = h3.geo_to_h3(latitude, longitude, resolution=9)
```

### Dynamic Segmentation

**What:** The process of linking attribute data to linear features (roads) based on spatial relationships.

**GIS equivalent:** In ArcGIS, you'd use linear referencing (route/measure) or spatial joins to assign attributes like pavement condition to road segments.

**NZTA use cases:**
- Pavement IRI (roughness) values → road segments
- Speed limits → road sections
- Crash records → road locations
- Maintenance contracts → road zones

**Pattern:**
1. Spatial join: Match attribute data to road segments (ST_INTERSECTS)
2. Enrich: Add attribute values to road geometries
3. Aggregate: Roll up statistics by road, locality, or contract area
4. Gold layer: Store enriched data for analysis

### Kepler.gl Visualization

**What:** An open-source web-based tool for visualizing large geospatial datasets.

**GIS equivalent:** Like ArcGIS Online or QGIS, but designed for big data and interactive exploration in notebooks.

**Features:**
- Multiple layer types (points, lines, polygons, hexbins, arcs)
- Color coding by attributes (e.g., condition score: red=poor, green=good)
- Filtering and clustering
- Exports to HTML, PNG, or JSON

**When to use:**
- Exploratory analysis (understanding spatial patterns)
- Presentations (interactive demos for stakeholders)
- Data quality checks (spot anomalies visually)

---

## How to Extend This Demo

### 1. Change Geographic Extent

The demo uses Wellington as the sample area. To analyze another region:

**In notebook `02_linz_ingestion.py`**, update the bounding box:

```python
# Current Wellington bbox
bbox = "174.5,-41.5,175.0,-41.0"

# Example: Auckland bbox
bbox = "174.5,-37.0,175.0,-36.7"

# Example: Christchurch bbox
bbox = "172.4,-43.6,172.8,-43.4"
```

### 2. Add Real NZTA Pavement Data

The demo uses synthetic condition scores (random 1-5 values). To use real IRI data:

**In notebook `05_dynamic_segmentation.py`**, replace the synthetic score generation with:

```python
# Load actual IRI data from your source (e.g., CSV, database, RAMM export)
iri_data = spark.read.csv("path/to/iri_data.csv", header=True)

# Join IRI to road centrelines by road ID or spatial join
roads_with_iri = roads_df.join(iri_data, on="road_id")

# Map IRI values to condition scores
# Example: IRI < 2 = excellent (5), IRI 2-4 = good (4), etc.
roads_with_condition = roads_with_iri.withColumn(
    "condition_score",
    when(col("iri") < 2, 5)
    .when(col("iri") < 4, 4)
    .when(col("iri") < 6, 3)
    .when(col("iri") < 8, 2)
    .otherwise(1)
)
```

### 3. Add Time-Series Analysis

To track pavement deterioration over time:

1. Add a `survey_date` column to your IRI data
2. Ingest multiple survey periods into separate tables or partitions
3. Query trends:

```sql
-- Show pavement deterioration for a specific road
SELECT
  survey_date,
  AVG(condition_score) as avg_score
FROM road_condition_history
WHERE road_name = 'State Highway 1'
GROUP BY survey_date
ORDER BY survey_date
```

4. Build predictive models using Databricks ML to forecast future condition

### 4. Add More LINZ Layers

The LINZ Data Service has 1000+ layers. To add more:

**Popular layers for NZTA:**
- Bridges (layer ID TBD)
- Railway centerlines (layer-50329)
- Hydrology/culverts (layer-50258)
- Property boundaries (layer-50804)

**In notebook `02_linz_ingestion.py`**, duplicate the ingestion code block and change the layer ID:

```python
# Fetch railway centrelines
railways_gdf = fetch_linz_layer(
    layer_id='layer-50329',
    layer_name='NZ Railway Centrelines',
    bbox=bbox,
    api_key=linz_api_key
)

save_to_delta(railways_gdf, 'railway_centrelines', catalog, schema)
```

---

## Mapping to NZTA Production (AAM UC Migration)

This demo is a prototype. Here's how it maps to NZTA's production Asset Analytics Manager (AAM) migration to Unity Catalog:

| Demo Component | Production Equivalent |
|----------------|----------------------|
| Wellington sample (2,000 roads) | National network (11,000+ km State Highways) |
| LINZ road centrelines | NZTA road hierarchy + RAMM centerline data |
| Synthetic condition scores | Historical IRI data from pavement surveys |
| Single timestamp | Time-series data (quarterly surveys over 10+ years) |
| 3 Delta tables | 100+ tables (crashes, traffic, assets, maintenance) |
| Kepler.gl visualization | Power BI / Databricks SQL dashboards |
| Genie demo space | Production Genie for regional managers |

**Production requirements:**
1. **Data ingestion pipelines:** Automate ingestion from RAMM, OneDrive, contractor systems
2. **Data governance:** Implement row-level security (regional managers see only their roads)
3. **Quality checks:** Validate geometry, detect outliers in IRI values
4. **Integration:** Connect to Power BI, ArcGIS Online, or other BI tools
5. **Scheduling:** Run nightly batch jobs to refresh gold tables
6. **ML models:** Predict pavement deterioration, optimize maintenance schedules

**Timeline:** This demo takes 40 minutes. Production migration typically takes 3-6 months depending on data complexity and governance requirements.

---

## Troubleshooting

### Problem: "Secrets scope 'linz' does not exist"

**Solution:** You haven't created the Databricks secrets scope yet. Follow **Prerequisites → Step 3** above.

---

### Problem: "WFS request failed" or "HTTP 403 Forbidden"

**Solution:** Your LINZ API key is invalid or expired.
1. Log in to [data.linz.govt.nz](https://data.linz.govt.nz)
2. Check if your API key is still active
3. Regenerate it if needed
4. Update the secret: `databricks secrets put-secret linz api_key`

---

### Problem: "No features retrieved" for a layer

**Solution:** The bounding box may not overlap with that layer's data. Try:
1. Expanding the bbox (e.g., `174.0,-41.5,175.5,-41.0`)
2. Using a different region (Auckland, Christchurch)
3. Checking the layer ID is correct on LINZ website

---

### Problem: Notebook times out during H3 indexing

**Solution:** The dataset is large. Try:
1. Using a larger cluster (more cores)
2. Reducing the sample size in notebook 02 (add `.limit(1000)` after reading data)
3. Using a coarser H3 resolution (7 instead of 9)

---

### Problem: Kepler.gl map doesn't render

**Solution:**
1. Make sure `keplergl` is installed: `%pip install keplergl`
2. Restart Python: `dbutils.library.restartPython()`
3. If still broken, use the alternate display method:
   ```python
   html = map_1._repr_html_()
   displayHTML(html)
   ```

---

## Additional Resources

- **LINZ Data Service:** [https://data.linz.govt.nz](https://data.linz.govt.nz)
- **Databricks Geospatial Guide:** [https://docs.databricks.com/sql/language-manual/sql-ref-geospatial.html](https://docs.databricks.com/sql/language-manual/sql-ref-geospatial.html)
- **H3 Documentation:** [https://h3geo.org](https://h3geo.org)
- **Kepler.gl Documentation:** [https://docs.kepler.gl](https://docs.kepler.gl)
- **Databricks Genie:** [https://docs.databricks.com/genie/](https://docs.databricks.com/genie/)

---

## Support

For questions about this demo, contact:

- **Databricks Solution Architect:** Your assigned SA for the NZTA account
- **Field Engineering Team:** Contact through your SA
- **LINZ Data Support:** [https://data.linz.govt.nz/about/contact/](https://data.linz.govt.nz/about/contact/)

For issues with this repository, open an issue on GitHub or contact your Databricks representative.

---

## License

This demo uses open data from Land Information New Zealand licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

Databricks notebooks and code in this repository are provided for demonstration purposes. Adapt as needed for your organization.

---

## Next Steps

1. ✅ Complete this demo end-to-end
2. 📅 Schedule a follow-up session with NZTA stakeholders to review results
3. 🗺️ Identify 2-3 pilot use cases for production (e.g., pavement condition, crash analysis)
4. 📊 Map NZTA's existing data sources (RAMM, traffic counters, contractors)
5. 🏗️ Design production architecture with data governance and security
6. 🚀 Pilot implementation in Databricks production environment
7. 📈 Scale to full national network

**Ready to transform NZTA's asset analytics? Let's build on Databricks Unity Catalog!**
