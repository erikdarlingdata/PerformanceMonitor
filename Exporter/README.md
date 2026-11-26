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
darling-export explain-gate --tables <a,b,c> --from <ISO UTC> --to <ISO UTC> [--store <name>]
```

Run `darling-export` with no verb, or `darling-export --help`, to print the usage with every `export` option.
`--verbose` on `export` and `run` also prints the session-guard lines and, on an interactive console, the full stack trace of a failure. Without it a run prints each warning once and a failure on a console as its exception chain. A failure with stdout redirected, as in a scheduled run, always prints the full exception to stdout.

### `run`

`run` is the scheduler. Start it once an hour from Task Scheduler or any other scheduler.
Each run does these steps:

1. It reads the state file and works out which windows are due.
2. It calls `export` once per due window and per family.
3. It uploads the Parquet files first and the manifest last.
4. It writes a daily log and removes old logs, output and temporary files.

`--plan-only` prints and logs what a run would do. It lists every due window per family on the console and writes the same plan to `plan-only-<store>-<date>.jsonl` in the log folder. It does not upload anything and does not start an export.
It opens the store once to check the collation (see "The store collation"). A store it cannot reach or a password it cannot resolve is a warning, and the plan goes on.
After each window `run` prints the exit code and `log:`, the daily log `export-<store>-<date>.log` in the log folder that holds the window's output.
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

Loads the config, resolves the credential, checks the store's collation and prints the result with the secret hidden.
It exits 2 when the config is not valid or the store does not use the C collation.
A store it cannot reach is a warning on stderr, and the exit code stays 0.

### `describe-tables`

Prints one line per table: the origin, the kind, the time column, the grain and the key columns.
`--json` prints the same facts as JSON, with a `blocked` field for the optional tables the registry lists but does not export.

### `explain-gate`

Prints the SQL of the schema gate for the optional tables you name. It does not connect to the store.

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
| `dpapi_file` | A file that holds the base64 text of a Windows DPAPI blob, as Darling writes its own. It works on Windows only. See below. |

You can use `store.connection_string_env` instead. It names an environment variable that holds a full Npgsql connection string.
The session guards below still apply when you do.

There is no fallback. If the named source is empty or missing, the run stops with exit code 2.

### The dpapi_file password file

A `dpapi_file` holds one thing: the base64 text of a DPAPI blob. The blob is the password as UTF-8 text, protected with `DataProtectionScope.LocalMachine` and the entropy string `PerformanceMonitor.Darling.v1`.
The Exporter reads the file as text, trims it, decodes the base64 and decrypts it. A raw binary blob, a blob made with other entropy, text that is not base64 and an empty file are all refused.
Darling's own `pg-credential.dpapi` is this format.

LocalMachine scope means any account on the machine that wrote the file can decrypt it, and no other machine can. The file keeps the password out of a copied disk or a backup. It does not keep it from another local user, and the entropy string is not a secret because it is in this repository.
Limit the file's permissions to the account that runs the Exporter and to administrators.

To make a file on a machine that has no Darling, or to change the password without Darling, run this in PowerShell on the machine that runs the Exporter. It works in Windows PowerShell 5.1 and in PowerShell 7. Set `$path` to the file that `store.password.path` names:

```powershell
$path = 'C:\ProgramData\PerformanceMonitorExporter\store-password.dpapi'
Add-Type -AssemblyName System.Security
New-Item -ItemType Directory -Force -Path (Split-Path $path) | Out-Null
$secure = Read-Host -AsSecureString 'Store password'
$password = [System.Net.NetworkCredential]::new('', $secure).Password
$entropy = [System.Text.Encoding]::UTF8.GetBytes('PerformanceMonitor.Darling.v1')
$blob = [System.Security.Cryptography.ProtectedData]::Protect([System.Text.Encoding]::UTF8.GetBytes($password), $entropy, 'LocalMachine')
[System.IO.File]::WriteAllText($path, [Convert]::ToBase64String($blob))
```

`darling-export check-config --config <file>` resolves the password and tells you whether the file works.
A file that cannot be decrypted stops it with exit code 2 and one line that names the likely causes: the file is not the base64 text of a DPAPI blob, the blob used other entropy than `PerformanceMonitor.Darling.v1`, or it was written on another machine.

### The store collation

The store database must use the C collation. The Query Store keys depend on byte order, and only the C collation gives PostgreSQL the same order as the exporter.
`export` stops on a store with another collation, such as `en_US.utf8`, with exit code 2 and one line that names the collation.
`check-config` and `run --plan-only` run the same check when the store can be reached, so the problem shows up before the first window.

The managed Windows store and a new Darling compose store are created with the C collation. A bring-your-own store and a compose store made before that change keep the collation they were created with.
A database collation cannot be changed in place. To create a C database in a PostgreSQL server you already run:

```sql
CREATE DATABASE darling TEMPLATE template0 ENCODING 'UTF8' LOCALE 'C';
```

For a new cluster, give `initdb` the arguments `--encoding=UTF8 --locale=C`. The compose file does this through `POSTGRES_INITDB_ARGS`, and it applies to a new volume only.
To move an existing store, create the C database, then load a dump of the old one into it. For a TimescaleDB store, use TimescaleDB's dump and restore steps (`timescaledb_pre_restore()` and `timescaledb_post_restore()`), and point the Darling service and `store.database` at the new database.
Or start a new Darling store on a new volume and let it collect from the start.

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

`destination` in the config says where the files go. `check-config`, `run --plan-only` and `run` read it with the same code, so a destination that `check-config` accepts is one that `run` can use.
A field the exporter does not know is an error in all three.

- **Amazon S3.** Set `type` to `s3` with a `bucket`. `prefix` and `region` are optional. Credentials come from the standard AWS chain unless you set `credentials`.
  If you leave `region` out, the AWS SDK picks it from its own sources (the environment, the shared config file or instance metadata).
- **An S3-compatible store**, such as MinIO or Cloudflare R2. Use the `s3` type and add `endpoint_url`. Most of these stores also need `"force_path_style": true`.
- **Azure Blob Storage.** Set `type` to `azure_blob` with a `container` and an `auth` object. `prefix` is optional. The container must exist before the first run.
- **Local folder.** Set `type` to `local` with a `path`. `prefix` is optional.

The `s3` type takes these fields:

| Field | Meaning |
|---|---|
| `bucket` | The bucket. Required. |
| `prefix` | A folder inside the bucket. |
| `region` | The region. With `endpoint_url` it is the signing region, and the default is `us-east-1`. |
| `endpoint_url` | The `http` or `https` URL of an S3-compatible store. |
| `force_path_style` | `true` puts the bucket in the path of the URL and not in the host name. |
| `checksum` | `sha256` (the default) or `none`, for a store that refuses the SHA-256 checksum. |
| `credentials` | Where the keys come from. See the next table. The default is the AWS chain. |

| `credentials` | Meaning |
|---|---|
| `"default"` | The standard AWS chain: the environment, the shared profile or the instance role. |
| `{ "profile": "<name>" }` | A named profile in the shared credentials file. |
| `{ "access_key_id_env": "<variable>", "secret_access_key_env": "<variable>" }` | Keys held in the two environment variables you name. Add `session_token_env` for a session token. |

The `azure_blob` type takes `container`, `prefix`, `account_url` and `auth`. `auth.method` is one of these:

| `auth.method` | Meaning |
|---|---|
| `connection_string_env` | `auth.name` is an environment variable that holds a storage connection string. `account_url` is not needed. |
| `sas_url_env` | `auth.name` is an environment variable that holds a SAS token or a full SAS URL. `account_url` is required. |
| `default_credential` | The Azure default credential chain (the environment, a managed identity or a developer sign-in). `account_url` is required. |

A MinIO destination with keys in two environment variables:

```json
"destination": {
  "type": "s3", "bucket": "exports", "prefix": "landing",
  "endpoint_url": "http://minio.internal:9000", "force_path_style": true,
  "credentials": { "access_key_id_env": "EXPORT_S3_KEY_ID", "secret_access_key_env": "EXPORT_S3_SECRET_KEY" }
}
```

An Azure Blob destination with a connection string in an environment variable:

```json
"destination": {
  "type": "azure_blob", "container": "exports", "prefix": "landing",
  "auth": { "method": "connection_string_env", "name": "EXPORT_AZURE_CONNECTION_STRING" }
}
```

The config holds the names of environment variables and never a key. The exporter does not print a key, and `check-config` prints only the names.
A variable name has only letters, digits and underscores. Anything else is refused, which catches a connection string or a SAS pasted in place of a name.
A key made only of letters, digits and underscores looks like a name, so keep keys in the variable and put only the variable's name in the config.
A URL in the config must not carry a user name, a password, a query string, a fragment or a SAS in the path. A SAS belongs in an environment variable.
`account_url` must be an `https` URL. `http` is accepted only for a loopback address (`localhost`, `127.0.0.0/8` or `::1`), such as Azurite at `http://127.0.0.1:10000/devstoreaccount1`.
`endpoint_url` may be `http` or `https`. Neither URL may point at a link-local address (`169.254.0.0/16` or `fe80::/10`), which is where cloud metadata services live.
`check-config` and `run` stop with exit code 2 when a variable the destination names is not set. `run --plan-only` does not read the variables.
The message names the config key (for example `destination.credentials.secret_access_key_env`) and never the variable or the profile.

`prefix` is a folder path with `/` between folders. A `.` or `..` segment, a backslash and, for a `local` destination, a drive letter are refused, so a prefix cannot leave the destination.
An `azure_blob` `container` follows the Azure naming rules: 3 to 63 characters of lowercase letters, digits and hyphens, starting and ending with a letter or digit, with no two hyphens in a row. The special containers `$root` and `$web` are accepted too.
An `s3` `bucket` cannot contain `/` or `\`, and cannot be `.` or `..`.

A key that appears twice in the same object is an error in `check-config`, `run`, `run --plan-only` and `export --config`. The message names the key (for example `destination.bucket: duplicate key`) and exits with code 2.

`run` uploads every window to the destination. `export` uploads only when you ask it to:

- `export --config <file> --upload-destination` uploads to the config's destination. It works with every form above.
- `export --upload <destination>` takes `s3://bucket/prefix` (with `--aws-region`) or a folder path. It cannot name an endpoint, keys or Azure Blob.
- With neither flag, `export` writes the files to `--out` and uploads nothing.

The Parquet files go up first and the manifest goes up last. A manifest means every file it lists is complete.

### The local folder

The folder of a `local` destination must exist before the first run. The exporter does not create it.
With a `prefix`, the folder is the `path` with the prefix added.
With no `destination`, `run` writes to `landing` next to `paths.out`, and that folder must exist too.
The account that runs the exporter must be able to write to it.

`check-config` and `run --plan-only` check the folder and exit 2 with one line that names it when it is missing or cannot be written to.
`run` makes the same check, and resolves the store credential, once before its first window, and exits 2 the same way.
When an `export` call that `run` started exits 2, `run` exits 2 as well, so a scheduler can tell a configuration error (2) from a window that failed (1).

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
