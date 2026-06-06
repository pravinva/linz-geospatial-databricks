# NZTA Geospatial Analytics on Databricks

## Executive Summary

This reference implementation demonstrates geospatial analytics capabilities for New Zealand Transport Agency (NZTA) using Databricks Lakehouse Platform. The solution showcases Unity Catalog for enterprise data governance, Delta Lake for reliable storage, and SQL-based spatial operations for road network analysis and asset management.

**Solution Components:**
- Automated data ingestion from LINZ Data Service WFS API
- Spatial SQL analytics using Databricks ST_ functions
- H3 hexagonal spatial indexing for performant large-scale joins
- Dynamic segmentation pattern for asset attribute enrichment
- Interactive visualization using Kepler.gl
- Natural language query interface via Databricks Genie

**Business Value:**

NZTA manages over 11,000 km of State Highway network requiring continuous monitoring of asset condition, traffic patterns, and maintenance priorities. This solution demonstrates how Databricks can augment or replace traditional GIS tooling for:

- Pavement condition tracking and predictive deterioration modeling
- Safety analytics including crash hotspot identification
- Traffic flow analysis and congestion pattern detection
- Asset inventory management (bridges, culverts, signage)
- Capital expenditure planning and budget allocation
- Contractor performance monitoring and compliance

---

## Architecture Overview

**Data Sources:**
- Land Information New Zealand (LINZ) WFS API for road network, address points, and administrative boundaries

**Platform:**
- Databricks Lakehouse on Azure/AWS/GCP
- Unity Catalog for data governance and lineage
- Delta Lake for ACID-compliant storage
- Photon-accelerated SQL warehouses for query performance

**Key Technologies:**
- Databricks SQL with geospatial functions (ST_DISTANCE, ST_INTERSECTS, ST_LENGTH)
- H3 spatial indexing library for hierarchical hexagonal grids
- Python libraries: GeoPandas, Shapely, Requests
- Visualization: Kepler.gl for interactive geospatial exploration
- AI: Databricks Genie for conversational analytics

---

## Prerequisites

### Databricks Workspace Requirements

Access to a Databricks workspace with the following capabilities:
- Unity Catalog enabled with CREATE CATALOG permissions
- Serverless SQL warehouse or Pro SQL warehouse
- Notebook execution environment (Python and SQL)
- Databricks Assistant/Genie enabled (for natural language queries)

Contact your Databricks account team if workspace provisioning is required.

### External Data Access

**LINZ API Credentials:**

This solution ingests open geospatial data from Land Information New Zealand. Registration is free.

1. Create account at https://data.linz.govt.nz
2. Navigate to profile settings and generate API key
3. Store credentials in Databricks Secrets:

```bash
# Using Databricks CLI
databricks secrets create-scope linz
databricks secrets put-secret linz api_key
```

Alternatively, configure secrets via Workspace UI under Settings > Secrets.

---

## Implementation Guide

### Notebook Execution Sequence

Execute notebooks in the following order. Each notebook implements a specific stage of the analytics pipeline.

| Sequence | Notebook | Purpose | Expected Runtime |
|----------|----------|---------|------------------|
| 1 | 01_workspace_setup.py | Unity Catalog initialization, library installation, credential validation | 3 minutes |
| 2 | 02_linz_ingestion.py | ETL pipeline for road centrelines, address points, localities from LINZ WFS | 5-10 minutes |
| 3 | 03_spatial_sql.sql | Spatial query examples using ST_ functions | 5 minutes |
| 4 | 04_h3_indexing.py | H3 spatial index generation and performance comparison | 5 minutes |
| 5 | 05_dynamic_segmentation.py | Asset attribute enrichment using spatial joins (pavement condition use case) | 5 minutes |
| 6 | 06_kepler_visualization.py | Interactive map generation with multi-layer rendering | 5 minutes |
| 7 | 07_genie_space_setup.md | Conversational AI configuration for business user self-service | 10 minutes |

**Total implementation time:** Approximately 40 minutes

---

## Technical Architecture Components

### Unity Catalog Data Organization

```
Catalog: nzta_geo_demo
  |
  +-- Schema: linz
      |
      +-- road_centrelines (source layer)
      +-- address_points (source layer)
      +-- localities (source layer)
      +-- road_centrelines_h3 (enriched with spatial index)
      +-- address_points_h3 (enriched with spatial index)
      +-- road_condition_by_locality (gold layer)
```

**Governance Features:**
- Fine-grained access control via Unity Catalog permissions
- Automatic data lineage tracking
- Audit logging for compliance
- Cross-workspace data sharing capability

### Delta Lake Storage

All tables implement Delta Lake format providing:
- ACID transaction guarantees
- Schema evolution and enforcement
- Time travel for historical analysis
- Efficient upsert/merge operations
- Automatic file compaction

### Spatial Analytics Capabilities

**Databricks Spatial SQL Functions:**
- ST_GEOMFROMTEXT: Parse WKT geometry strings
- ST_DISTANCE: Calculate geodetic distance between features
- ST_INTERSECTS: Spatial predicate testing
- ST_LENGTH: Measure linestring geometry
- ST_BUFFER: Generate proximity zones
- ST_AREA: Calculate polygon area

**H3 Spatial Indexing:**

Uber H3 provides hierarchical hexagonal grid system for efficient spatial operations:

| Resolution | Cell Area | Use Case |
|------------|-----------|----------|
| 5 | ~250 km² | Regional aggregation |
| 7 | ~5 km² | Metropolitan analysis |
| 9 | ~0.1 km² | Urban/neighborhood scale (implemented) |
| 11 | ~0.002 km² | Block-level precision |

**Performance Benefits:**
- Replaces expensive ST_INTERSECTS operations with simple equality joins
- Enables distributed spatial aggregation at scale
- Consistent cell area across geographies
- Hierarchical relationship between resolutions

### Dynamic Segmentation Pattern

Linear referencing implementation for asset management:

1. **Spatial Join:** Associate attribute data with road segments via ST_INTERSECTS
2. **Attribute Enrichment:** Append asset characteristics to geometry
3. **Aggregation:** Calculate statistics by road section, locality, or administrative boundary
4. **Gold Layer:** Persist enriched dataset for downstream consumption

**NZTA Application Examples:**
- IRI (International Roughness Index) pavement condition scores
- Posted speed limit zones
- Crash incident attribution
- Maintenance contract zones
- Environmental constraint overlays

---

## Extending the Solution

### Geographic Expansion

Default implementation covers Wellington metropolitan area. To expand coverage:

**Modify bounding box in 02_linz_ingestion.py:**

```python
# Wellington (current)
bbox = "174.5,-41.5,175.0,-41.0"

# Auckland
bbox = "174.5,-37.0,175.0,-36.7"

# Christchurch
bbox = "172.4,-43.6,172.8,-43.4"

# National coverage (full NZ)
bbox = "165.0,-48.0,179.0,-33.0"
```

Note: National-scale ingestion requires larger cluster and extended runtime.

### Integrating Production NZTA Data

**Pavement Condition Data:**

Replace synthetic condition scores with actual IRI measurements:

```python
# Load IRI survey data from source system
iri_df = spark.read.format("delta").table("nzta_production.surveys.iri_measurements")

# Spatial join to road network
roads_with_iri = spark.sql("""
    SELECT
        r.road_id,
        r.road_name,
        r.geom_wkt,
        i.iri_value,
        i.survey_date,
        CASE
            WHEN i.iri_value < 2.0 THEN 5  -- Excellent
            WHEN i.iri_value < 4.0 THEN 4  -- Good
            WHEN i.iri_value < 6.0 THEN 3  -- Fair
            WHEN i.iri_value < 8.0 THEN 2  -- Poor
            ELSE 1  -- Very Poor
        END as condition_score
    FROM road_centrelines r
    JOIN iri_measurements i ON ST_INTERSECTS(
        ST_GEOMFROMTEXT(r.geom_wkt),
        ST_GEOMFROMTEXT(i.geom_wkt)
    )
""")
```

**Additional LINZ Data Layers:**

The LINZ Data Service provides 1000+ authoritative datasets. Recommended additions:

- Railway centrelines (layer-50329)
- Hydrology/drainage networks (layer-50258)
- Property boundaries (layer-50804)
- Topographic contours
- Building footprints

Duplicate the ingestion pattern in notebook 02 with appropriate layer identifiers.

### Time-Series Analysis

Implement temporal analytics for condition deterioration modeling:

```sql
-- Trend analysis: pavement degradation by road segment
SELECT
    road_id,
    road_name,
    survey_date,
    AVG(iri_value) as avg_iri,
    AVG(iri_value) - LAG(AVG(iri_value)) OVER (
        PARTITION BY road_id ORDER BY survey_date
    ) as iri_delta
FROM iri_measurements
WHERE road_type = 'State Highway'
GROUP BY road_id, road_name, survey_date
ORDER BY road_id, survey_date
```

Apply ML forecasting using Databricks AutoML or MLflow for predictive maintenance scheduling.

---

## Production Migration Considerations

This demonstration provides a functional prototype. Production deployment requires additional enterprise capabilities:

| Demo Component | Production Requirement |
|----------------|------------------------|
| Wellington sample dataset | National road network (11,000+ km State Highways) |
| Manual notebook execution | Automated Delta Live Tables pipelines with SLA monitoring |
| Public LINZ data | Integration with RAMM, traffic counters, contractor systems, weather data |
| Synthetic condition scores | Historical IRI/SCRIM survey data with 10+ years temporal depth |
| Single timestamp | Time-series partitioning strategy (monthly/quarterly) |
| Basic Unity Catalog | Row-level security, attribute-based access control for regional visibility |
| Ad-hoc queries | Scheduled dashboards, alerting, Power BI integration |
| Development workspace | Production workspace with DR/HA, backup strategy |

**Typical Migration Timeline:**
- Planning and design: 4-6 weeks
- Data pipeline development: 8-12 weeks
- Governance implementation: 4-6 weeks
- UAT and validation: 4-6 weeks
- Production deployment: 2-4 weeks

**Total:** 6-9 months for full production implementation

---

## Troubleshooting

### Secrets Configuration Issues

**Error:** "Secrets scope 'linz' does not exist"

**Resolution:** Configure Databricks Secrets following Prerequisites section above. Verify scope creation:

```bash
databricks secrets list-scopes --profile DEFAULT
```

### API Authentication Failures

**Error:** "HTTP 403 Forbidden" or "Invalid API key"

**Resolution:**
1. Validate LINZ account status at data.linz.govt.nz
2. Regenerate API key if expired
3. Update secret value:
   ```bash
   databricks secrets put-secret linz api_key --profile DEFAULT
   ```

### Geometry Ingestion Issues

**Error:** "No features retrieved" for specific layer

**Resolution:**
1. Verify bounding box overlaps layer coverage area
2. Check layer availability on LINZ platform
3. Validate layer ID in ingestion parameters
4. Expand bbox if needed or select alternative region

### Performance Optimization

**Issue:** H3 indexing timeout on large datasets

**Resolution:**
1. Scale cluster to larger node type or increase worker count
2. Implement row sampling during development: `.limit(10000)`
3. Reduce H3 resolution (e.g., resolution 7 vs 9)
4. Enable Photon acceleration on cluster configuration

### Visualization Rendering

**Issue:** Kepler.gl map not displaying

**Resolution:**
1. Verify library installation: `%pip install keplergl`
2. Execute Python restart: `dbutils.library.restartPython()`
3. Use alternative rendering method:
   ```python
   html_output = map_1._repr_html_()
   displayHTML(html_output)
   ```

---

## Reference Resources

**Databricks Documentation:**
- Geospatial Analytics: https://docs.databricks.com/sql/language-manual/sql-ref-geospatial.html
- Unity Catalog: https://docs.databricks.com/data-governance/unity-catalog/
- Delta Live Tables: https://docs.databricks.com/workflows/delta-live-tables/
- Databricks Genie: https://docs.databricks.com/genie/

**External Resources:**
- LINZ Data Service: https://data.linz.govt.nz
- H3 Spatial Index: https://h3geo.org
- Kepler.gl: https://docs.kepler.gl
- GeoPandas: https://geopandas.org

**Transport Sector Case Studies:**
- Transport for NSW: Traffic congestion analytics
- DOT: Highway safety analysis
- National road agencies: Pavement management systems

---

## Support and Engagement

**Technical Support:**
- Databricks Solution Architect (assigned to NZTA account)
- Field Engineering team (via SA escalation)
- Customer Success team (post-production deployment)

**LINZ Data Platform:**
- Support portal: https://data.linz.govt.nz/about/contact/
- Developer documentation: https://github.com/linz

**Repository Issues:**
- GitHub issues for this reference implementation
- Contact Databricks account team for private repositories or customization requests

---

## License and Attribution

**Data License:**
This solution uses open data from Land Information New Zealand (LINZ) licensed under Creative Commons Attribution 4.0 International (CC BY 4.0).

**Code License:**
Databricks notebooks and Python code in this repository are provided for demonstration and reference purposes. Organizations may adapt and modify for internal use. Commercial redistribution requires separate licensing agreement.

---

## Next Steps

### Immediate Actions (Week 1-2)
1. Execute complete notebook sequence end-to-end
2. Validate data quality and spatial operations accuracy
3. Document performance metrics and cluster configurations
4. Present findings to NZTA stakeholder group

### Short-term Planning (Month 1-2)
1. Conduct requirements workshop with NZTA business owners
2. Identify 2-3 pilot use cases for production implementation
3. Inventory existing data sources (RAMM, contractors, traffic systems)
4. Design target-state data architecture with governance framework
5. Develop project charter and resource plan

### Production Implementation (Month 3-9)
1. Establish production Databricks workspace with enterprise security
2. Build automated data ingestion pipelines using Delta Live Tables
3. Implement Unity Catalog governance policies and access controls
4. Develop gold layer analytics tables and materialized views
5. Create executive dashboards and operational reports
6. Deploy predictive ML models for maintenance optimization
7. Conduct user acceptance testing with regional managers
8. Execute phased production rollout with monitoring

### Scaling and Optimization (Month 10+)
1. Expand to national road network coverage
2. Integrate additional data sources (weather, traffic cameras, IoT sensors)
3. Implement real-time analytics for incident response
4. Deploy advanced ML capabilities (computer vision for road defects)
5. Establish center of excellence for geospatial analytics

---

**For detailed implementation support or production deployment planning, contact your Databricks Solutions Architect.**
