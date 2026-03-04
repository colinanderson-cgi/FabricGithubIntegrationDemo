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

# Welcome to your new notebook
# Type here in the cell editor to add code!
#Create a notebook attached to the Lakehouse. Add Notebook parameters with these names:
#load_id, job_type, job_name, sql_text, sql_file_path, run_id, pipeline_name
import json
from datetime import datetime, timezone
load_id = 12345          # <-- set your value here
load_id = int(load_id)
job_type="manual load"
job_type = job_type
job_name = job_name
sql_text = sql_text
sql_file_path = sql_file_path
run_id = run_id
pipeline_name = pipeline_name

print(f"[START] {datetime.now(timezone.utc).isoformat()} load_id={load_id} job={job_name} type={job_type}")
print(f"run_id={run_id} pipeline={pipeline_name}")

def load_json_to_table(cfg_str: str):
    cfg = json.loads(cfg_str)
    src = "/lakehouse/default/" + cfg["source_path"].lstrip("/")
    tgt = cfg["target_table"]
    mode = cfg.get("write_mode", "overwrite")

    df = spark.read.json(src)
    df.write.mode(mode).format("delta").saveAsTable(tgt)
    print(f"[OK] wrote {tgt} from {src} mode={mode} rows={df.count()}")

if job_type == "json_to_table":
    if not sql_text:
        raise ValueError("sql_text must contain JSON config with source_path/target_table")
    load_json_to_table(sql_text)
else:
    raise ValueError(f"Unsupported job_type for this demo: {job_type}")

print("[DONE]")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import datetime, timezone

# -------------------------------------------------------------------
# Notebook parameters (set these in Fabric notebook parameters)
# - load_id: optional (run one row)
# - run_id, pipeline_name: optional (for logging)
# -------------------------------------------------------------------
try:
    load_id
except NameError:
    load_id = ""   # blank means "run all"

try:
    run_id
except NameError:
    run_id = ""

try:
    pipeline_name
except NameError:
    pipeline_name = ""

CONFIG_TABLE = "nb_Config_Runner"   # change if it’s schema-qualified, e.g. "dbo.nb_Config_Runner"

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def load_json_to_table(cfg_str: str):
    cfg = json.loads(cfg_str)
    if "source_path" not in cfg or "target_table" not in cfg:
        raise ValueError("sql_text JSON must include source_path and target_table")

    src = "/lakehouse/default/" + cfg["source_path"].lstrip("/")
    tgt = cfg["target_table"]
    mode = cfg.get("write_mode", "overwrite")

    df = spark.read.json(src)
    df.write.mode(mode).format("delta").saveAsTable(tgt)
    rows = df.count()
    return {"target_table": tgt, "source": src, "mode": mode, "rows": rows}

print(f"[START] {utc_now()} run_id={run_id} pipeline={pipeline_name} load_id={load_id}")

# -------------------------------------------------------------------
# Fetch config rows
# If load_id is provided => run only that row
# Else => run all rows ordered by load_order
# -------------------------------------------------------------------
where_clause = ""
if str(load_id).strip() != "":
    where_clause = f"WHERE load_id = {int(load_id)}"

cfg_df = spark.sql(f"""
SELECT load_id, load_order, job_type, job_name, sql_text
FROM {CONFIG_TABLE}
{where_clause}
ORDER BY load_order
""")

rows = cfg_df.collect()
if not rows:
    raise ValueError(f"No rows found in {CONFIG_TABLE} {where_clause or '(all rows)'}")

# -------------------------------------------------------------------
# Execute
# -------------------------------------------------------------------
ok = 0
fail = 0
results = []

for r in rows:
    row_load_id = r["load_id"]
    load_order = r["load_order"]
    job_type = (r["job_type"] or "").strip()
    job_name = (r["job_name"] or "").strip()
    sql_text = r["sql_text"] or ""

    print(f"\n[JOB START] {utc_now()} load_id={row_load_id} order={load_order} job_name={job_name} type={job_type}")

    try:
        if job_type == "json_to_table":
            if not str(sql_text).strip():
                raise ValueError("sql_text is empty; expected JSON with source_path/target_table")
            info = load_json_to_table(sql_text)
            ok += 1
            print(f"[JOB OK] {utc_now()} load_id={row_load_id} job_name={job_name} -> {info}")
            results.append({"load_id": row_load_id, "load_order": load_order, "job_name": job_name, "status": "OK", **info})

        else:
            raise ValueError(f"Unsupported job_type: {job_type}")

    except Exception as e:
        fail += 1
        print(f"[JOB FAIL] {utc_now()} load_id={row_load_id} job_name={job_name} error={type(e).__name__}: {e}")
        results.append({"load_id": row_load_id, "load_order": load_order, "job_name": job_name, "status": "FAIL", "error": str(e)})

print(f"\n[SUMMARY] {utc_now()} total={len(rows)} ok={ok} fail={fail}")

# Optional: show a summary table at the end
try:
    display(spark.createDataFrame(results).orderBy("load_order"))
except Exception:
    pass

if fail > 0:
    raise RuntimeError(f"{fail} job(s) failed. See logs/summary above.")

print("[DONE]")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timezone

src = "abfss://16412aaa-da2d-423f-95b7-96c4190e00c4@onelake.dfs.fabric.microsoft.com/c6294b46-db00-4363-9769-12453b4d0506/Files/initial_tables_json_files/ethnicity_ct.json"
tgt = "bronze_ethnicity_ct"
mode = "overwrite"

print(f"[START] {datetime.now(timezone.utc).isoformat()} src={src} tgt={tgt} mode={mode}")

df = (
    spark.read
         .option("multiline", "true")  # safe for single- or multi-line JSON
         .json(src)
)

df.write.mode(mode).format("delta").saveAsTable(tgt)

print(f"[OK] wrote {tgt} rows={df.count()}")
print("[DONE]")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils

mssparkutils.fs.ls("Files/initial_tables_json_files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timezone
from notebookutils import mssparkutils

FOLDER = "abfss://16412aaa-da2d-423f-95b7-96c4190e00c4@onelake.dfs.fabric.microsoft.com/c6294b46-db00-4363-9769-12453b4d0506/Files/initial_tables_json_files"
MODE = "overwrite"  # or "append"

def utc_now():
    return datetime.now(timezone.utc).isoformat()

print(f"[START] {utc_now()} folder={FOLDER} mode={MODE}")

files = mssparkutils.fs.ls(FOLDER)
json_files = [f for f in files if f.name.lower().endswith(".json")]

if not json_files:
    raise ValueError(f"No .json files found in {FOLDER}")

ok, fail = 0, 0
results = []

for f in sorted(json_files, key=lambda x: x.name.lower()):
    src = f.path
    base = f.name[:-5]  # strip ".json"
    tgt = f"bronze_{base}"

    print(f"\n[JOB START] {utc_now()} src={src} tgt={tgt}")

    try:
        df = (
            spark.read
                 .option("multiline", "true")  # safe default
                 .json(src)
        )

        df.write.mode(MODE).format("delta").saveAsTable(tgt)
        rows = df.count()

        ok += 1
        results.append({"file": f.name, "table": tgt, "status": "OK", "rows": rows})
        print(f"[JOB OK] {utc_now()} table={tgt} rows={rows}")

    except Exception as e:
        fail += 1
        results.append({"file": f.name, "table": tgt, "status": "FAIL", "error": str(e)})
        print(f"[JOB FAIL] {utc_now()} file={f.name} error={type(e).__name__}: {e}")

print(f"\n[SUMMARY] {utc_now()} total={len(json_files)} ok={ok} fail={fail}")

# Optional: show summary table
try:
    display(spark.createDataFrame(results).orderBy("file"))
except Exception:
    pass

if fail > 0:
    raise RuntimeError(f"{fail} file(s) failed. See summary above.")

print("[DONE]")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
