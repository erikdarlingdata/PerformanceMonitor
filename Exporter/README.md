# Darling export

`darling-export` reads the PostgreSQL store of a Darling monitoring service and writes Parquet files plus one JSON manifest per run.
It uploads the files to Amazon S3, to Azure Blob Storage or to a local folder.
It does not load the files anywhere. Whatever reads them is up to you.

The exporter only reads the store. It never writes to it.

## Commands

```
darling-export run --config <file> [--plan-only] [--now <ISO UTC>]
darling-export export [--config <file>] --store <name> --from <ISO UTC> --to <ISO UTC> --out <dir> [options]
darling-export check-config --config <file>
darling-export describe-tables [--json]
```

### `run`

`run` is the scheduler. Start it once an hour from Task Scheduler or any other scheduler.
Each run does these steps:

1. It reads the state file and works out which windows are due.
2. It calls `export` once per due window and per family.
3. It uploads the Parquet files first and the manifest last.
4. It writes a daily log and removes old logs, output and temporary files.

`--plan-only` prints and logs what a run would do. It does not open the store and does not upload anything.
`--now` sets the clock for a plan.

The three families are `fresh_fast` (the fast window), `fresh` (hourly) and `query_store` (Query Store, a few hours behind).
The config sets the windows, the lag and the limits. See the sample file.

### `export`

`export` writes one window for one store. `run` calls it for you, so you rarely call it by hand.
Use it for a one-off catch-up or to test.

```
darling-export export --config darling-export.json --from 2026-09-01T00:00:00Z --to 2026-09-01T01:00:00Z --out ./out
```

Any flag you give overrides the config. Run `darling-export` with no verb to print the usage.

### `check-config`

Loads the config, resolves the credential and prints the result with the secret hidden.
It exits 2 when the config is not valid.

### `describe-tables`

Prints one line per table: the origin, the kind, the time column, the grain and the key columns.
`--json` prints the same facts as JSON.

## The config file

Copy `darling-export.sample.json` and edit it.
`darling-export check-config --config <file>` tells you what is wrong.
These sections matter most:

- `store`: the name, the host, the port, the database, the login and where the password comes from.
  The name is lower case, starts with a letter and has at most 31 characters. It becomes a folder name and a key in the files.
- `paths`: the local folders for output, state and logs. All three are required.
- `destination`: `s3`, `azure_blob` or `local`. With no destination, `run` writes to a `landing` folder next to `paths.out`.
- `families`, `schedule`, `export`, `memory_guard`, `restart_delay` and `retention`: tuning. Every key has a default.
- `post_run`: an optional command that runs after each run, for example a health check.

`store.application_name` sets the PostgreSQL `application_name` of the exporter's sessions. The default is `darling-export`.
The straggler guard ignores sessions with the same name, so keep it different from the names of your other tools.

## The store login

The exporter connects with an ordinary PostgreSQL login. Give it a role that can read the tables it exports and nothing else.
It does not need to write, and it does not need to be a superuser.
The role's grants are the boundary. The session guards below limit what a bug can cost, but they do not replace a read-only role.

The password comes from one place that the config names:

| `store.password.source` | Meaning |
|---|---|
| `env` | An environment variable. The sample uses `DARLING_EXPORT_PG_PASSWORD`. |
| `file` | A plain text file. Keep its permissions tight. |
| `dpapi_file` | A Windows DPAPI file written by Darling. It works on Windows only. |

You can use `store.connection_string_env` instead. It names an environment variable that holds a full Npgsql connection string.
The session guards below still apply when you do.

There is no fallback. If the named source is empty or missing, the run stops with exit code 2.

### Session guards

Every connection sets these limits before it reads anything:

- `default_transaction_read_only = on`. This is a session default, not a lock: each transaction starts read-only unless a statement turns that off, and the exporter never does. The role's grants are what stop a write.
- `statement_timeout`, from `store.statement_timeout_seconds`. The default is 120 seconds with a config file and 600 seconds when `export` runs on flags alone. `--statement-timeout-seconds` sets it either way.
- `lock_timeout = 5s`.
- `work_mem = 16MB`.
- `max_parallel_workers_per_gather = 0`.
- `temp_file_limit = 256MB`. Only a superuser can set this. If your role cannot, set it on the role with `ALTER ROLE <role> SET temp_file_limit = '256MB'`. Otherwise the exporter prints a warning and goes on.

A limit that the role already has at least as strict is left alone.

Every statement the exporter sends starts with a comment such as `/* darling-export:query_stats_hourly */`.
Use it to find the exporter's statements in `pg_stat_statements` and in the PostgreSQL log.

## Destinations

- **Amazon S3.** Set `type` to `s3` with `bucket`, `prefix` and `region`. Credentials come from the standard AWS chain.
  If you leave `region` out, the AWS SDK picks it from its own sources (the environment, the shared config file or instance metadata).
  The `export` command can also use S3-compatible stores and explicit credentials. See `destination` in the config.
- **Azure Blob Storage.** Set `type` to `azure_blob` with a container and an `auth` method (connection string, SAS URL or default credential).
- **Local folder.** Set `type` to `local` with a `path`.

`run` supports the S3 and local forms with a bucket, a prefix and a region. For other forms, call `export` yourself.

The Parquet files go up first and the manifest goes up last. A manifest means every file it lists is complete.

## State and files

The exporter keeps small state files in `paths.state`. They remember which windows ran, which dimension keys were already written and what the plan-reuse cache holds.
The state is internal. The format can change in any release. Do not edit it, and do not build anything on it.
Delete a state file only if you accept that the next run writes everything it holds again.

`run` takes a lock in the state folder, so two runs of the same store cannot overlap.

## What the files look like

`file-contract.md` describes the file names, the manifest, the tables and the units.
Read it before you build a consumer.

## Build and test

```
dotnet build Exporter/PerformanceMonitor.Exporter -c Release
dotnet test Exporter/PerformanceMonitor.Exporter.Tests -c Release
```

Some tests start PostgreSQL in Docker. They skip themselves when Docker is not there.
The tests with the `Slow` trait take longer.
The release build is `dotnet publish Exporter/PerformanceMonitor.Exporter -c Release -r win-x64 --self-contained`.
Each release ships two zips: `PerformanceMonitorDarlingExport-<version>-win-x64.zip`, which is signed, and
`PerformanceMonitorDarlingExport-<version>-linux-x64.zip`, which is not. `SHA256SUMS.txt` and `SHA256SUMS-linux.txt` hold their checksums.
