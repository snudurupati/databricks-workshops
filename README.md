# databricks-workshops
Building Data Engineering &amp; Data Science Pipelines with Databricks

## Production Job: Applying Schemas to JSON Data

This repo includes a production-ready Spark job and a Databricks notebook for schema-applied JSON ingestion and Delta writes.

- Scala app: `1. data-engineering/03-Applying-Schemas-to-JSON-Data-Production.scala`
- Notebook: `1. data-engineering/03-Applying-Schemas-to-JSON-Data-Production-Notebook.scala`

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

### Run Scala App

Package and submit the Scala object `ApplySchemasJob` as a JAR task (configure your build accordingly) or run it from a Databricks `%scala` cell by pasting the object code.
