# NZTA Geospatial Demo - Genie Space Setup Guide

This guide provides step-by-step instructions for setting up a **Databricks Genie Space** over the NZTA demo gold tables, enabling natural language queries for road asset managers and analysts.

## What is Databricks Genie?

Databricks Genie is an AI-powered conversational interface that allows business users to ask questions in natural language and automatically generates SQL queries to answer them. Think "ChatGPT for your data warehouse."

**Benefits for NZTA:**
- Road managers can query data without writing SQL
- Reduces dependency on data engineers for routine analysis
- Accelerates decision-making for maintenance planning
- Provides audit trail of all questions and answers

---

## Prerequisites

1. **Unity Catalog tables created** by running notebooks 01-05
2. **Databricks workspace access** with Genie enabled (contact your workspace admin)
3. **Permissions**: You need `USE CATALOG`, `USE SCHEMA`, and `SELECT` on all tables

---

## Step 1: Add Table Descriptions in Unity Catalog

Before creating the Genie space, add friendly descriptions to your tables so Genie understands what data they contain.

### 1.1 Road Centrelines Table

**Navigate to:** Data Explorer → `nzta_geo_demo` → `linz` → `road_centrelines`

**Table Description:**
```
Road centreline network for Wellington region sourced from LINZ. Contains road names, types (motorway, arterial, local), and geometry as WKT. Use this table to analyze the road network structure, count roads by type, or find roads by name.
```

**Column Descriptions:**
- `road_name`: Official name of the road (e.g., "State Highway 1", "Willis Street")
- `road_type`: Classification of the road (motorway, arterial, collector, local)
- `geom_wkt`: Road geometry as Well-Known Text (LineString). Do not display this column directly; use it only for spatial functions like ST_LENGTH or ST_INTERSECTS.
- `ingestion_timestamp`: Date and time when this data was ingested from LINZ

### 1.2 Road Condition by Locality Table

**Navigate to:** Data Explorer → `nzta_geo_demo` → `linz` → `road_condition_by_locality`

**Table Description:**
```
Pavement condition scores for road segments by locality. Condition scores range from 1 (poor) to 5 (excellent). Use this table to identify roads needing maintenance, analyze condition trends by locality, or prioritize maintenance budgets.
```

**Column Descriptions:**
- `road_name`: Name of the road segment
- `road_type`: Type/classification of the road
- `locality_name`: Suburb or locality name where this road segment is located
- `condition_score`: Pavement condition rating (1=poor, 2=fair, 3=moderate, 4=good, 5=excellent). Lower scores indicate urgent maintenance needs.
- `geom_wkt`: Road segment geometry as WKT. Do not display; use only for spatial analysis.
- `calculation_timestamp`: When this condition score was calculated

### 1.3 Address Points H3 Table

**Navigate to:** Data Explorer → `nzta_geo_demo` → `linz` → `address_points_h3`

**Table Description:**
```
Property address points for Wellington region with H3 spatial indexing. Each address is assigned to a hexagonal grid cell (h3_r9) for fast spatial aggregation. Use this table to count addresses by area, analyze population density, or perform spatial joins with road data.
```

**Column Descriptions:**
- `full_address`: Complete property address (e.g., "123 Willis Street, Wellington")
- `suburb_locality`: Suburb or locality name
- `h3_r9`: H3 hexagon cell ID at resolution 9 (~0.1 km² per cell). Use for aggregating addresses by area.
- `geom_wkt`: Address point geometry as WKT. Do not display directly.
- `ingestion_timestamp`: When this address was ingested from LINZ

---

## Step 2: Create a Genie Space

1. **Navigate to Genie:**
   - In your Databricks workspace, click **"Genie"** in the left sidebar
   - Click **"Create Genie Space"**

2. **Configure the space:**
   - **Name:** `NZTA Road Asset Analytics`
   - **Description:** `Natural language queries for NZTA road network, pavement condition, and address data. Ask questions about road types, condition scores, maintenance priorities, or address density.`

3. **Add tables:**
   - Click **"Add data"**
   - Select these three tables:
     - `nzta_geo_demo.linz.road_centrelines`
     - `nzta_geo_demo.linz.road_condition_by_locality`
     - `nzta_geo_demo.linz.address_points_h3`

4. **Add instructions for Genie:**

   In the **"Instructions"** section, add the following guidance:

   ```
   This space contains NZTA road asset and geospatial data. Follow these rules when generating queries:

   1. GEOMETRY COLUMNS: Never display geom_wkt columns directly in results. These contain long text strings that are not human-readable. Use them only for spatial functions like ST_LENGTH, ST_DISTANCE, or ST_INTERSECTS.

   2. CONDITION SCORES: When analyzing condition_score:
      - 1-2 = Poor condition (urgent maintenance needed)
      - 3 = Fair condition (monitor closely)
      - 4-5 = Good condition (routine maintenance)

   3. ROAD TYPES: Common road_type values include:
      - motorway (State Highways, controlled access)
      - arterial (major through-roads)
      - collector (medium-capacity roads)
      - local (residential streets)

   4. H3 SPATIAL INDEX: The h3_r9 column contains hexagon IDs for spatial aggregation. Use GROUP BY h3_r9 to count features by area. Each hexagon is ~0.1 km².

   5. LOCALITY NAMES: Use locality_name to filter or group by suburb/area.

   6. When asked about "maintenance priorities", sort by condition_score ASC (worst first).

   7. When asked about "road lengths", use ST_LENGTH(ST_GEOMFROMTEXT(geom_wkt)) and divide by 1000 to get kilometers.
   ```

5. **Save the space**

---

## Step 3: Sample Questions for NZTA Business Analysts

### For Road Network Analysis

**Question 1:** *"How many kilometers of each road type do we have?"*

**Expected SQL:**
```sql
SELECT
  road_type,
  COUNT(*) as segment_count,
  ROUND(SUM(ST_LENGTH(ST_GEOMFROMTEXT(geom_wkt))) / 1000, 2) as total_km
FROM nzta_geo_demo.linz.road_centrelines
WHERE geom_wkt IS NOT NULL
GROUP BY road_type
ORDER BY total_km DESC;
```

**Explanation:** Aggregates road segments by type and calculates total length in kilometers using spatial function ST_LENGTH.

---

**Question 2:** *"Show me all roads that contain 'Highway' in the name"*

**Expected SQL:**
```sql
SELECT
  road_name,
  road_type,
  ROUND(ST_LENGTH(ST_GEOMFROMTEXT(geom_wkt)) / 1000, 2) as length_km
FROM nzta_geo_demo.linz.road_centrelines
WHERE road_name LIKE '%Highway%'
ORDER BY length_km DESC;
```

**Explanation:** Filters roads by name pattern and shows their lengths. Useful for finding all State Highways.

---

**Question 3:** *"What are the 10 longest roads in our network?"*

**Expected SQL:**
```sql
SELECT
  road_name,
  road_type,
  ROUND(ST_LENGTH(ST_GEOMFROMTEXT(geom_wkt)) / 1000, 2) as length_km
FROM nzta_geo_demo.linz.road_centrelines
WHERE geom_wkt IS NOT NULL AND road_name IS NOT NULL
ORDER BY ST_LENGTH(ST_GEOMFROMTEXT(geom_wkt)) DESC
LIMIT 10;
```

**Explanation:** Ranks roads by geometric length. Strategic corridors are often the longest segments.

---

### For Pavement Condition Analysis

**Question 4:** *"Which roads are in poor condition and need urgent maintenance?"*

**Expected SQL:**
```sql
SELECT
  road_name,
  road_type,
  locality_name,
  condition_score
FROM nzta_geo_demo.linz.road_condition_by_locality
WHERE condition_score <= 2
ORDER BY condition_score ASC, road_type;
```

**Explanation:** Filters for condition scores ≤2 (poor), which represent urgent maintenance priorities.

---

**Question 5:** *"What's the average pavement condition by locality?"*

**Expected SQL:**
```sql
SELECT
  locality_name,
  COUNT(*) as total_segments,
  ROUND(AVG(condition_score), 2) as avg_condition,
  SUM(CASE WHEN condition_score <= 2 THEN 1 ELSE 0 END) as poor_segments
FROM nzta_geo_demo.linz.road_condition_by_locality
GROUP BY locality_name
ORDER BY avg_condition ASC;
```

**Explanation:** Aggregates condition scores by locality to identify areas with the worst overall pavement quality.

---

**Question 6:** *"How many road segments are in good vs poor condition?"*

**Expected SQL:**
```sql
SELECT
  CASE
    WHEN condition_score >= 4 THEN 'Good (4-5)'
    WHEN condition_score = 3 THEN 'Fair (3)'
    ELSE 'Poor (1-2)'
  END as condition_category,
  COUNT(*) as segment_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
FROM nzta_geo_demo.linz.road_condition_by_locality
GROUP BY condition_category;
```

**Explanation:** Categorizes all road segments into condition bins and shows distribution as percentages.

---

### For Address/Population Density Analysis

**Question 7:** *"Which H3 hexagons have the most addresses?"*

**Expected SQL:**
```sql
SELECT
  h3_r9,
  COUNT(*) as address_count
FROM nzta_geo_demo.linz.address_points_h3
GROUP BY h3_r9
ORDER BY address_count DESC
LIMIT 20;
```

**Explanation:** Aggregates addresses by H3 cell to identify high-density areas (e.g., Wellington CBD).

---

**Question 8:** *"How many addresses are in each suburb?"*

**Expected SQL:**
```sql
SELECT
  suburb_locality,
  COUNT(*) as address_count
FROM nzta_geo_demo.linz.address_points_h3
WHERE suburb_locality IS NOT NULL
GROUP BY suburb_locality
ORDER BY address_count DESC;
```

**Explanation:** Groups addresses by suburb name. Useful for prioritizing infrastructure investment in high-population areas.

---

**Question 9:** *"Show me addresses in Te Aro"* (specific Wellington suburb)

**Expected SQL:**
```sql
SELECT
  full_address,
  suburb_locality
FROM nzta_geo_demo.linz.address_points_h3
WHERE suburb_locality = 'Te Aro'
LIMIT 100;
```

**Explanation:** Filters addresses by locality name. Change 'Te Aro' to any other suburb as needed.

---

## Step 4: Test the Genie Space

1. **Open the Genie space** you just created
2. **Start with simple questions** from the examples above
3. **Review the generated SQL** to ensure it's correct
4. **Refine instructions** if Genie misunderstands certain questions
5. **Save useful queries** as "Trusted Assets" for reuse

---

## Step 5: Share with NZTA Team

1. **Add users to the space:**
   - Click **"Share"** in the Genie space
   - Add NZTA road managers, analysts, or planners
   - Grant **"Can run queries"** permission

2. **Train your team:**
   - Share this guide with new users
   - Encourage them to start with the sample questions
   - Remind them to review generated SQL before running large queries

3. **Monitor usage:**
   - Check the **"Activity"** tab to see what questions are being asked
   - Identify common patterns to create trusted assets

---

## Step 6: Advanced - Create Trusted Assets

**What are Trusted Assets?**
Trusted assets are pre-written SQL queries or instructions that Genie can reference when answering questions. They ensure consistent, correct answers for frequently asked questions.

**Example Trusted Asset: "Maintenance Priority List"**

**Asset Name:** `maintenance_priority_list`

**SQL:**
```sql
SELECT
  road_name,
  road_type,
  locality_name,
  condition_score,
  CASE
    WHEN condition_score <= 2 THEN 'Urgent'
    WHEN condition_score = 3 THEN 'Monitor'
    ELSE 'Routine'
  END as priority
FROM nzta_geo_demo.linz.road_condition_by_locality
WHERE condition_score <= 3
ORDER BY condition_score ASC, road_type;
```

**Usage:**
When a user asks "Show me the maintenance priority list", Genie will use this trusted asset instead of generating SQL from scratch.

---

## Tips for Business Analysts Using Genie

1. **Start simple:** Begin with questions like "How many roads are there?" before moving to complex aggregations.

2. **Be specific:** Instead of "Show me roads", ask "Show me motorways longer than 5 km".

3. **Use domain terms:** Genie understands NZTA terminology like "State Highway", "arterial", "condition score".

4. **Review SQL:** Always check the generated SQL before running on large datasets.

5. **Avoid geometry columns:** Don't ask Genie to "show me the geometry" - WKT strings are not readable. Instead ask for road lengths, counts, or names.

6. **Iterate:** If Genie misunderstands, rephrase your question or add more context.

---

## Troubleshooting

**Problem:** Genie displays long WKT strings in results

**Solution:** Add this to Instructions:
```
Never include geom_wkt in SELECT statements unless the user explicitly asks for spatial calculations. These columns contain very long text and should not be displayed.
```

---

**Problem:** Genie doesn't understand "pavement condition"

**Solution:** Use the exact column name in your question: "Show me roads with low condition_score"

---

**Problem:** Query times out on large datasets

**Solution:** Ask Genie to add `LIMIT` clauses, or pre-aggregate data in a new gold table for faster queries.

---

## Next Steps

1. **Run this demo** with NZTA stakeholders to show the value of natural language queries
2. **Migrate real NZTA data** (IRI values, State Highway network, crash data) into Unity Catalog
3. **Create production Genie space** with actual asset management tables
4. **Train NZTA analysts** on Genie best practices
5. **Integrate Genie** into NZTA's reporting workflows (e.g., monthly condition reports)

---

## Summary

This guide showed how to set up a Databricks Genie Space for NZTA road asset analytics:

1. ✓ Added table and column descriptions in Unity Catalog
2. ✓ Created a Genie space with three gold tables
3. ✓ Provided instructions to handle geometry columns and domain terms
4. ✓ Listed 9 sample questions with expected SQL for testing
5. ✓ Explained trusted assets for common queries

**Genie democratizes data access** - road managers can now answer their own questions without waiting for data engineers, accelerating decision-making for maintenance planning and budget allocation.

For questions or issues, contact your Databricks Solution Architect or refer to the [Databricks Genie documentation](https://docs.databricks.com/genie/).
