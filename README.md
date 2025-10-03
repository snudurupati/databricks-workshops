# databricks-workshops
Building Data Engineering &amp; Data Science Pipelines with Databricks

## Production Job: Applying Schemas to JSON Data

This repo includes a production-ready Spark job and a Databricks notebook for schema-applied JSON ingestion and Delta writes.

- Scala content has been deprecated in favor of Python modules under `python_src/`.

Both versions:
- Use explicit schemas (no inference) for performance and consistency
- Handle corrupt records explicitly and filter them out
- Mitigate small files by repartitioning before writes
- Write to Delta tables `curated.zips` and `curated.smartphone_logs`

### Run as Notebook Task (via Jobs API/CLI)

1. Import/sync the repo into your Databricks workspace.
2. Create a new Job with a Notebook task pointing to the notebook path above.
3. Choose a cluster (DBR 12.2+ recommended), set permissions, and run.

Example Jobs JSON (replace `<workspace-path>` and cluster fields):

```json
{
  "name": "Apply Schemas Production",
  "tasks": [
    {
      "task_key": "apply-schemas",
      "notebook_task": {
        "notebook_path": "/Repos/<workspace-path>/databricks-workshops/1. data-engineering/03-Applying-Schemas-to-JSON-Data-Production-Notebook",
        "source": "WORKSPACE"
      },
      "existing_cluster_id": "<cluster-id>"
    }
  ]
}
```

CLI submission:

```bash
databricks jobs create --json-file jobs.apply-schemas.json
```

### Run Python Job (spark-submit)

Python modules live under `python_src/data_engineering`.

Example invocation:

```bash
spark-submit \
  python_src/data_engineering/jobs/apply_schemas.py \
  --zips-path <path-to-zips.json> \
  --show
```

Notes:
- Provide a valid JSON path. Avoid `/mnt/training` in restricted workspaces; prefer UC Volumes or `dbfs:/databricks-datasets/`.
- The job prints schemas and sample rows when `--show` is passed.
