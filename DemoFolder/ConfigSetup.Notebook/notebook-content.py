# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c6294b46-db00-4363-9769-12453b4d0506",
# META       "default_lakehouse_name": "BCStats_pop_lh_test",
# META       "default_lakehouse_workspace_id": "16412aaa-da2d-423f-95b7-96c4190e00c4",
# META       "known_lakehouses": [
# META         {
# META           "id": "c6294b46-db00-4363-9769-12453b4d0506"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.sql("SHOW DATABASES").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS meta

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS meta.sql_load_config (
# MAGIC   load_id BIGINT,
# MAGIC   is_active BOOLEAN,
# MAGIC   load_order INT,
# MAGIC   job_type STRING,
# MAGIC   job_name STRING,
# MAGIC   sql_text STRING,
# MAGIC   sql_file_path STRING,
# MAGIC   created_utc TIMESTAMP,
# MAGIC   updated_utc TIMESTAMP
# MAGIC )
# MAGIC USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DESCRIBE meta.sql_load_config").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- ethnicity_ct
# MAGIC INSERT INTO meta.sql_load_config
# MAGIC (load_id, is_active, load_order, job_type, job_name, sql_text, sql_file_path, created_utc, updated_utc)
# MAGIC VALUES
# MAGIC (11, true, 110, 'json_to_table', 'Load ethnicity_ct',
# MAGIC '{"source_path":"Files/initial_tables_json_files/ethnicity_ct.json","target_table":"ethnicity_ct","write_mode":"overwrite"}',
# MAGIC NULL, current_timestamp(), current_timestamp());
# MAGIC 
# MAGIC -- gender_ct
# MAGIC INSERT INTO meta.sql_load_config
# MAGIC (load_id, is_active, load_order, job_type, job_name, sql_text, sql_file_path, created_utc, updated_utc)
# MAGIC VALUES
# MAGIC (12, true, 120, 'json_to_table', 'Load gender_ct',
# MAGIC '{"source_path":"Files/initial_tables_json_files/gender_ct.json","target_table":"gender_ct","write_mode":"overwrite"}',
# MAGIC NULL, current_timestamp(), current_timestamp());
# MAGIC 
# MAGIC -- marital_status_ct
# MAGIC INSERT INTO meta.sql_load_config
# MAGIC (load_id, is_active, load_order, job_type, job_name, sql_text, sql_file_path, created_utc, updated_utc)
# MAGIC VALUES
# MAGIC (13, true, 130, 'json_to_table', 'Load marital_status_ct',
# MAGIC '{"source_path":"Files/initial_tables_json_files/marital_status_ct.json","target_table":"marital_status_ct","write_mode":"overwrite"}',
# MAGIC NULL, current_timestamp(), current_timestamp());
# MAGIC 
# MAGIC -- outcome_ct
# MAGIC INSERT INTO meta.sql_load_config
# MAGIC (load_id, is_active, load_order, job_type, job_name, sql_text, sql_file_path, created_utc, updated_utc)
# MAGIC VALUES
# MAGIC (14, true, 140, 'json_to_table', 'Load outcome_ct',
# MAGIC '{"source_path":"Files/initial_tables_json_files/outcome_ct.json","target_table":"outcome_ct","write_mode":"overwrite"}',
# MAGIC NULL, current_timestamp(), current_timestamp());
# MAGIC 
# MAGIC -- roh_category_ct
# MAGIC INSERT INTO meta.sql_load_config
# MAGIC (load_id, is_active, load_order, job_type, job_name, sql_text, sql_file_path, created_utc, updated_utc)
# MAGIC VALUES
# MAGIC (15, true, 150, 'json_to_table', 'Load roh_category_ct',
# MAGIC '{"source_path":"Files/initial_tables_json_files/roh_category_ct.json","target_table":"roh_category_ct","write_mode":"overwrite"}',
# MAGIC NULL, current_timestamp(), current_timestamp());
# MAGIC 
# MAGIC -- workflow_hierarchy_ct
# MAGIC INSERT INTO meta.sql_load_config
# MAGIC (load_id, is_active, load_order, job_type, job_name, sql_text, sql_file_path, created_utc, updated_utc)
# MAGIC VALUES
# MAGIC (16, true, 160, 'json_to_table', 'Load workflow_hierarchy_ct',
# MAGIC '{"source_path":"Files/initial_tables_json_files/workflow_hierarchy_ct.json","target_table":"workflow_hierarchy_ct","write_mode":"overwrite"}',
# MAGIC NULL, current_timestamp(), current_timestamp());

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT load_id, load_order, job_type, job_name, sql_text
# MAGIC FROM meta.sql_load_config
# MAGIC ORDER BY load_order;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC TRUNCATE TABLE meta.sql_load_config;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM meta.sql_load_config
# MAGIC WHERE
# MAGIC   (sql_file_path IS NOT NULL AND (sql_file_path LIKE '%/demo/%' OR sql_file_path LIKE 'Files/demo%'))
# MAGIC   OR
# MAGIC   (sql_text IS NOT NULL AND (sql_text LIKE '%/demo/%' OR sql_text LIKE '%Files/demo%'));

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
