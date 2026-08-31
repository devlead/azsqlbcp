# azsqlbcp

Azure SQL Bulk Copy .NET global tool. Copy a table between two Azure SQL databases (optionally partitioned and parallel) using `SqlBulkCopy` and Microsoft Entra authentication.

## Install

```bash
dotnet tool install -g azsqlbcp
```

Requires the **.NET 10** runtime (or SDK).

## Usage

```bash
azsqlbcp \
  --source-server <host> \
  --source-database <db> \
  --source-table <schema.table> \
  --target-server <host> \
  --target-database <db> \
  --target-table <schema.table> \
  --partition-column <bigintIdentityColumn>
```

### Options

| Option                              | Description                                                                   |
| ----------------------------------- | ----------------------------------------------------------------------------- |
| `--source-server`                   | Source Azure SQL logical server                                               |
| `--source-database`                 | Source database                                                               |
| `--source-table`                    | Source table (`schema.table` or `table` → `dbo`)                              |
| `--source-port`                     | Source TCP port (default: 1433)                                               |
| `--source-trust-server-certificate` | Trust source server certificate (default: false)                              |
| `--target-server`                   | Target Azure SQL logical server                                               |
| `--target-database`                 | Target database                                                               |
| `--target-table`                    | Target table                                                                  |
| `--target-port`                     | Target TCP port (default: 1433)                                               |
| `--target-trust-server-certificate` | Trust target server certificate (default: false)                              |
| `--partition-column`                | Bigint identity column used to split work                                     |
| `--no-partition-column`             | Single-stream copy (`SELECT *`); do not pass `--partition-column`             |
| `--source-read-only`                | Set `ApplicationIntent=ReadOnly` on source (Business Critical read scale-out) |
| `--parallelism`                     | Parallel partitions (default: 6)                                              |
| `--batch-size`                      | `SqlBulkCopy` batch size (default: 2000000)                                   |

### Examples

Partitioned parallel copy:

> Requires a **unique** partition column (typically a `bigint` **identity**), and a **consistent source** for the duration of the export (no inserts/updates/deletes in the copied id range while the job runs), otherwise partitions can miss or double-count rows.

```powershell
azsqlbcp `
  --source-server prod.database.windows.net `
  --source-database SourceDb `
  --source-table dbo.MyTable `
  --target-server staging.database.windows.net `
  --target-database TargetDb `
  --target-table bak.MyTable_Copy `
  --partition-column Id `
  --source-read-only
```

Single stream (no partition column):

```powershell
azsqlbcp `
  --source-server prod.database.windows.net `
  --source-database SourceDb `
  --source-table dbo.MyTable `
  --target-server staging.database.windows.net `
  --target-database TargetDb `
  --target-table bak.MyTable_Copy `
  --no-partition-column
```

## What it does

1. Authenticates with Microsoft Entra ID via `DefaultAzureCredential` (token cached)
2. Truncates the target table
3. Reads row count (and min/max of the partition column when partitioning)
4. Runs one or more parallel `SqlBulkCopy` streams with `KeepIdentity` and streaming
5. Shows Spectre progress (%, remaining, elapsed, rows/s)

## Authentication

Uses `DefaultAzureCredential` (Azure CLI, Visual Studio, managed identity, etc.). Locally, `az login` is typically enough. The identity needs read on the source and truncate/insert on the target.

## Permissions

- Source: `SELECT` (and preferably a Business Critical replica when using `--source-read-only`)
- Target: `ALTER`/`TRUNCATE` and `INSERT` (including identity insert via bulk copy)

## Icon

Package icon: [Duplicate Spreadsheet Icon #7721123](https://thenounproject.com/) by Miftakhul Rizky (Noun Project), royalty-free. See [`icon/LICENSE.md`](icon/LICENSE.md).

## License

MIT
