# Databricks notebook source
# MAGIC %md
# MAGIC # NZTA Geospatial Demo - Workspace Setup
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Create a free account at data.linz.govt.nz
# MAGIC - Generate an API key under your profile settings
# MAGIC - Store the API key in Databricks secrets:
# MAGIC   - Via CLI: `databricks secrets create-scope linz && databricks secrets put-secret linz api_key`
# MAGIC   - Via UI: Settings > Secrets > Create scope 'linz', key 'api_key'
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Creates Unity Catalog structure (catalog + schema)
# MAGIC 2. Validates LINZ API key access
# MAGIC 3. Installs required Python libraries
# MAGIC 4. Confirms environment is ready

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Unity Catalog Structure

# COMMAND ----------

# Create catalog and schema for the demo
catalog_name = "nzta_geo_demo"
schema_name = "linz"

# Create catalog if it doesn't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
print(f"✓ Catalog '{catalog_name}' created or already exists")

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
print(f"✓ Schema '{catalog_name}.{schema_name}' created or already exists")

# Set as current catalog and schema for this session
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")
print(f"✓ Using catalog '{catalog_name}' and schema '{schema_name}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate LINZ API Key Access

# COMMAND ----------

# Read LINZ API key from Databricks secrets
try:
    linz_api_key = dbutils.secrets.get(scope="linz", key="api_key")
    print("✓ Successfully retrieved LINZ API key from secrets scope 'linz'")
    print(f"✓ API key length: {len(linz_api_key)} characters")
    print(f"✓ API key preview: {linz_api_key[:8]}..." if len(linz_api_key) > 8 else "✓ API key retrieved")
except Exception as e:
    print(f"✗ ERROR: Could not retrieve LINZ API key from secrets")
    print(f"  Error message: {e}")
    print("\n  Please set up the secret using one of these methods:")
    print("  1. CLI: databricks secrets create-scope linz")
    print("          databricks secrets put-secret linz api_key")
    print("  2. UI: Settings > Secrets > Create scope 'linz', add key 'api_key'")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Install Required Libraries

# COMMAND ----------

# Install geospatial and visualization libraries
%pip install geopandas keplergl h3 requests --quiet

# COMMAND ----------

# Restart Python to load newly installed packages
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify Installation and List Current Tables

# COMMAND ----------

# Verify imports work
import geopandas as gpd
import requests
import h3
import keplergl

print("✓ Successfully imported required libraries:")
print(f"  - geopandas version: {gpd.__version__}")
print(f"  - h3 version: {h3.__version__}")
print(f"  - keplergl version: {keplergl.__version__}")

# COMMAND ----------

# List existing tables in the schema
tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()

if len(tables) == 0:
    print(f"ℹ No tables exist yet in {catalog_name}.{schema_name}")
    print("  Tables will be created in subsequent notebooks")
else:
    print(f"✓ Existing tables in {catalog_name}.{schema_name}:")
    for table in tables:
        print(f"  - {table.tableName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete
# MAGIC
# MAGIC Your environment is ready! Next steps:
# MAGIC 1. Run notebook `02_linz_ingestion` to load NZ road and address data
# MAGIC 2. Run notebook `03_spatial_sql` to query the data using Databricks spatial functions
# MAGIC 3. Continue with H3 indexing, dynamic segmentation, and visualization notebooks