---

## title: Databricks Workflow Export Enhancements

owner: Shubham Jain status: In Progress last\_updated: 2025-07-24

## Overview

This PRD defines enhancements to an existing Python-based tool responsible for exporting Databricks job workflows using `databricks bundle generate`. The goal is to support various task types (`notebook_task`, `spark_python_task`, `python_wheel_task`, `sql_task`), maintain workspace folder hierarchy, eliminate code redundancy, and ensure code stability against CLI changes by relying on filesystem scanning post-export instead of CLI stdout parsing.

The document also includes detailed examples of task types and their expected YAML transformations.

## Goals

1. **Support export of additional task types:** `spark_python_task`, `python_wheel_task`, `sql_task`.
2. **Maintain proper notebook/code hierarchy** as per original workspace.
3. **Handle both job cluster and serverless references** for WHL libraries.
4. **Minimize code duplication** by introducing modular architecture.
5. **Eliminate dependency on CLI stdout parsing** by switching to a filesystem-driven file discovery approach post-export.

## Non-goals

- UI-based export functionality.
- Real-time sync or event-driven export.
- Replacing `bundle generate` with a custom SDK-based exporter.

## Existing Functionality ✅

- ✔️ Export YAML via `databricks bundle generate --job_id`.
- ✔️ Refactor notebook paths and place notebooks according to actual workspace hierarchy by calling the job export API.
- ✔️ Replace variables in YAML using config files.
- ❌ Only supports `notebook_task` currently.
- ❌ Relies on CLI stdout parsing for identifying exported files.

## Examples of Current Issues and Desired Outcomes

### Notebook Task (Working Example)

**Generated YAML:**

```yaml
tasks:
  - task_key: NB_TASK
    job_cluster_key: Job_cluster
    notebook_task:
      notebook_path: ../src/test.py
      source: WORKSPACE
```

**Expected YAML after Path Refactor:**

```yaml
tasks:
  - task_key: NB_TASK
    job_cluster_key: Job_cluster
    notebook_task:
      notebook_path: ../correct/path/test.py
      source: WORKSPACE
```

### Python Script Task

**Generated YAML:**

```yaml
tasks:
  - task_key: py_Task
    job_cluster_key: Job_cluster
    libraries:
      - whl: /Volume/abc.whl
    spark_python_task:
      python_file: /Workspace/Users/shubham.j@databricks.com/wf_exporter/run.py
```

**Expected YAML after Export & Refactor:**

```yaml
tasks:
  - task_key: py_Task
    job_cluster_key: Job_cluster
    libraries:
      - whl: ../libs/abc.whl
    spark_python_task:
      python_file: ../Users/shubham.j@databricks.com/wf_exporter/run.py
```

### Python Wheel Task

**Generated YAML:**

```yaml
tasks:
  - task_key: whl_TASK
    job_cluster_key: Job_cluster
    libraries:
      - whl: /Volume/abc.whl
    python_wheel_task:
      entry_point: /Volume/abc.whl
      package_name: /Volume/abc.whl
```

**Expected YAML after Export & Refactor:**

```yaml
tasks:
  - task_key: whl_TASK
    job_cluster_key: Job_cluster
    libraries:
      - whl: ../libs/abc.whl
    python_wheel_task:
      entry_point: /Volume/abc.whl
      package_name: /Volume/abc.whl
```

### SQL File Task

**Generated YAML:**

```yaml
tasks:
  - task_key: sql_file_task
    sql_task:
      file:
        path: /Workspace/Users/shubham.j@databricks.com/wf_exporter/sql_file.sql
        source: WORKSPACE
```

**Expected YAML after Export & Refactor:**

```yaml
tasks:
  - task_key: sql_file_task
    sql_task:
      file:
        path: ../Users/shubham.j@databricks.com/wf_exporter/sql_file.sql
        source: WORKSPACE
```

### Serverless WHL Reference (Environment Key)

```yaml
resources:
  jobs:
    sj_test_export:
      tasks:
        - task_key: whl_TASK
          environment_key: Default
          python_wheel_task:
            entry_point: /Volume/abc.whl
            package_name: /Volume/abc.whl
      environments:
        - environment_key: Default
          spec:
            dependencies:
              - /Volume/abc.whl
```

**Expected Refactor:**

```yaml
resources:
  jobs:
    sj_test_export:
      tasks:
        - task_key: whl_TASK
          environment_key: Default
          python_wheel_task:
            entry_point: /Volume/abc.whl
            package_name: /Volume/abc.whl
      environments:
        - environment_key: Default
          spec:
            dependencies:
              - ../libs/abc.whl
```

## Task List with Status

| Task ID | Description                                                   | Depends On        | Status         |
| ------- | ------------------------------------------------------------- | ----------------- | -------------- |
| T1      | Export notebooks and refactor paths                           | None              | ✅ Completed    |
| T2      | Add config-based YAML variable substitution                   | None              | ✅ Completed    |
| T3      | Identify `spark_python_task` and fetch `.py` file             | T1                | ⏳ In Progress  |
| T4      | Rewrite YAML paths for `spark_python_task`                    | T3                | ⏳ In Progress  |
| T5      | Identify `python_wheel_task` & fetch `.whl`                   | None              | ⏳ In Progress  |
| T6      | Rewrite YAML `libraries` for `.whl`                           | T5                | ⏳ In Progress  |
| T7      | Identify `sql_task` and fetch `.sql` file                     | None              | 🛠 Not Started |
| T8      | Rewrite YAML `sql_task.file.path`                             | T7                | 🛠 Not Started |
| T9      | Detect and handle job cluster/serverless differences          | T6                | 🛠 Not Started |
| T12     | Replace CLI output parsing with filesystem scan post-export   | None              | 🛠 Not Started |
| T13     | Build DataFrame/Dict of exported files with workspace mapping | T12               | 🛠 Not Started |
| T14     | Update YAML file paths using filesystem-based mapping         | T13               | 🛠 Not Started |
| T10     | Refactor code into modules                                    | All feature tasks | 🛠 Not Started |
| T11     | Add logging and dry-run support                               | All feature tasks | 🛠 Not Started |

## Folder Structure (Post-Export)

```text
/databricks-export/
├── resources/
│   └── workflow_name.yml
├── src/
│   ├── correct/path/to/notebook.py
│   ├── correct/path/to/run.py
│   └── correct/path/to/sql_file.sql
└── libs/
    └── abc.whl
```

## Architecture Changes

### Module Plan

- `exporter/main.py`: Entry point with CLI args
- `exporter/yaml_updater.py`: Load, modify and write YAML
- `exporter/artifact_fetcher.py`: Fetch notebooks, py, sql, whl
- `exporter/path_mapper.py`: Translate workspace path to relative
- `exporter/config_loader.py`: Load and merge config into YAML
- `exporter/file_scanner.py`: Perform filesystem scan post-export and build file mapping

## Risks

| Risk                                          | Mitigation                                         |
| --------------------------------------------- | -------------------------------------------------- |
| WHL/SQL/PY files not found due to permissions | Retry with fallback user token, add error handling |
| Large YAML causing memory issues              | Streamline YAML parse and write flow               |
| Unexpected task types in future               | Log and skip unknown task types gracefully         |
| CLI command export file structure changes     | Handle with flexible directory scanning logic      |

## Milestones

| Date       | Milestone                                       |
| ---------- | ----------------------------------------------- |
| 2025-07-24 | Initial support for `spark_python_task`         |
| 2025-07-25 | Support for `.whl` and `sql_task` files         |
| 2025-07-26 | Filesystem-based export mapping implementation  |
| 2025-07-27 | Full refactor, config integration, and DRY code |

## Success Criteria

- ✅ All task types supported with correct artifact export
- ✅ All YAML paths converted to correct relative references
- ✅ CLI output parsing dependency fully removed
- ✅ Test suite validating 10+ job scenarios
- ✅ No code duplication > 20 lines
- ✅ Modular structure for maintainability

