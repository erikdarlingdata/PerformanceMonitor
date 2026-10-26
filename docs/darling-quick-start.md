# Quick Start: Darling on Linux and the web dashboard

Audience: operators.

Part of the [Performance Monitor documentation](../README.md#documentation).

**Darling** is the always-on edition for teams that want 24/7 collection without a desktop app driving it: a service on Windows or Linux collects from your SQL Server and PostgreSQL servers around the clock into a central PostgreSQL store (TimescaleDB is detected and adopted automatically for compression and chunk-based retention), and a detached WPF viewer reads that store from any Windows seat. It runs the same monitoring brain as Lite — collectors, alert engine, and analysis pipeline shared at the library level — with alerts over email and webhooks (Teams, Slack, PagerDuty, and generic HTTP POST) and the same MCP tool surface available on request.

The optional built-in web dashboard is off by default and uses its own port, 5153. It serves the fleet overview, per-server drill-down, FinOps, the scheduled fleet sweep reports, alert history, job history, and an Admin page to any browser. It reads the collected data. A seat that is allowed to make changes can also do these things:

- save views, and edit alert rules and mute rules
- dismiss alerts
- manage server tags, including the order of sibling tags (Move Up and Move Down in the viewer, Move up and Move down on the web page)
- add or edit a monitored server

Operators can watch the fleet without installing the viewer. See [Tabs](tabs.md#tabs) for the pages.

## Install on Windows

The Windows install has its own page: [Install Darling](darling/install.md). It covers the download and hash check, the install folder `C:\Program Files\PerformanceMonitorDarling`, `install-darling.ps1`, and the rule that `darling.json` is only a seed. This page does not repeat those steps. To run the viewer on another machine, read [The WPF viewer](darling/viewer.md).

For a PostgreSQL server as a monitored target, set `"engine": "postgres"` on its entry in `darling.json` (see [PostgreSQL Targets](darling/postgresql-targets.md#postgresql-targets)).

## Turn on the web dashboard or MCP

Both surfaces are off by default. After the service collects, turn them on with `--enable-web` (the browser dashboard on port 5153) or `--enable-mcp` (the MCP endpoint on port 5152). Both take effect live, with no restart. The steps and proof points are in [The web dashboard](darling/web-dashboard.md) and [MCP server](mcp-server.md#darling).

## Install on Linux

On Linux, run the service under Docker Compose with the official TimescaleDB image. You can also run it under systemd with a PostgreSQL server that you already run, version 16 or later. The bundled store is Windows-only.

On Linux, the service keeps a key pair and seals secrets to it, so it can save and use them. The secrets are a SQL login password, the SMTP password, webhook URLs, the PagerDuty key, and generic webhook headers. `--reset-password-key` makes a new key. See [Run on Linux](#run-on-linux-docker-compose-or-systemd-1804) in the operator guide.

Configuration is a single JSON file with no schedule knobs. One exception: a collector that runs once a day or less often can have a run time. That is a time of day on the monitored server's own clock. Set it in the viewer's Collector Schedules window (the **Run at** column) or with `--set-collector-run-at`. See the **[Darling operator guide](../Darling/README.md)** for the configuration reference, permissions, and operations.

<a id="run-on-linux-docker-compose-or-systemd-1804"></a>
### Run on Linux (Docker Compose or systemd)

The service is cross-platform .NET; only the **bundled zero-admin store** and DPAPI are Windows-specific. On Linux you pair the service with the official TimescaleDB image (compose, the recommended shape) or point it at PostgreSQL you already run (systemd), keeping `postgres.managed = false` either way. The Viewer stays a Windows desktop app — Linux hosts read the **web dashboard**, which the container exposes.

Each shape needs a different file from the [release page](https://github.com/erikdarlingdata/PerformanceMonitor/releases). `SHA256SUMS-linux.txt` on the same page lists the checksums.

- **Docker Compose:** download `PerformanceMonitorDarling-compose-<version>.tar.gz`. Extract it, then `cd` into the folder it makes.
- **Docker Compose, release 3.10.0 or earlier:** these releases have no compose file of their own. Download the release's "Source code" archive instead. It extracts to a `PerformanceMonitor-<version>` folder, and the same files are in its `Darling/compose/` folder. Those releases have no `setup.sh`, so make the secret files by hand from the list in [`compose/secrets/README.md`](../Darling/compose/secrets/README.md).
- **systemd:** download `PerformanceMonitorDarling-linux-x64-<version>.tar.gz`.

**Compose (the whole stack as one deployment).** The files are in the compose archive, or in [`Darling/compose/`](../Darling/compose) of a source checkout:

```bash
tar -xzf PerformanceMonitorDarling-compose-<version>.tar.gz
cd PerformanceMonitorDarling-compose-<version>   # from a source checkout or a "Source code" archive: cd Darling/compose
./setup.sh                                 # makes secrets/ and darling.json
#   edit darling.json: servers, alerting
docker compose up -d
```

`setup.sh` makes the secret files that a default install needs, and it never changes a file that exists. It writes a random store password and the matching store connection string, the web token, and the MCP token. It copies `darling.sample.json` to `darling.json`. It asks for the monitoring login's SQL password with hidden input. To skip the question, set `DARLING_SQL_PASSWORD` and add `--no-prompt`. With `--no-prompt` and no variable, it tells you to make `secrets/sql_password.txt` yourself. It never prints a secret.

The script ends with the dashboard address. To read the web token, run `cat secrets/web_token.txt`. If the script does not start, because the archive lost its execute bit, run `sh setup.sh`.

MCP is off in the sample, so compose does not mount `mcp_token`. To turn MCP on, uncomment `mcp_token` in the two places in `docker-compose.yml`, and set `mcp.enabled` to `true` in `darling.json`. To make the secret files by hand instead of with `setup.sh`, follow the list in [`compose/secrets/README.md`](../Darling/compose/secrets/README.md).

The compose file in the compose archive names the image tag of its own release, for example `performancemonitor-darling:<version>`. It pulls the release it came from. The copy in the source tree names `:latest`, which is the newest release. To pin a release in that copy, change the `darling` service's `image:` line to that version's tag, for example `performancemonitor-darling:3.10.0`. The `:nightly` tag is the newest dev build. Use it only to test unreleased changes.

Web dashboard on `http://<host>:5153` behind its token, MCP (if enabled) on `:5152` behind its bearer token. The port mappings are the exposure boundary: the container-aware bind gate honors `web.network`/`mcp.network` under `managed = false` **inside a container only**, and the tokens are still mandatory. Five rules worth knowing before they bite:

- **The service provisions the store's least-privilege roles itself** ([#3914](https://github.com/erikdarlingdata/PerformanceMonitor/issues/3914)). The compose file's store is the service's own: it logs in as the store's bootstrap superuser (`POSTGRES_USER`), so at every start it creates or refreshes the `admin`, `viewer` and `mcp` roles exactly as managed mode does, and the web dashboard connects as `viewer` and MCP as `mcp`, never as the owner — so a web session or an MCP token-holder gets the secret-column carve, the narrow write grants and the composed-query `statement_timeout` backstop (15 s by default, `compose_statement_timeout_seconds`). Nothing to configure, and do not run `provision-roles.sql` against this store. The generated passwords live one owner-only file per role in the `darling-credentials` volume (`/var/lib/darling/credentials`); `docker compose exec darling cat /var/lib/darling/credentials/pg-admin-credential` shows the `admin` password if you want to point a Darling Viewer or `psql` at the store as `admin` rather than as the owner. A deployment from an older compose file gets the roles on its first start with the new image, and keeps working without the volume (the passwords are regenerated and re-applied on each container recreation); take this release's `docker-compose.yml` to keep them stable. If the store's login is not its bootstrap superuser, if the cluster also holds a database besides the store's own and `postgres` (an operator's own cluster rather than the compose store), or if a role named `admin`/`viewer`/`mcp` already exists that the service did not create, the service leaves the roles alone. On a start like that, or one whose provisioning fails or never runs, the two surfaces keep connecting as `viewer` and `mcp` with the credentials an earlier start wrote; only with none do they fall back to the owner. A startup warning says which login each surface uses and why.

- **Nothing secret goes in darling.json.** Every secret slot — the whole `postgres.connectionString`, server `password`s, `smtp.password`, the tokens — takes an `env:NAME` or `file:/run/secrets/<name>` reference. The compose file mounts each secret from `secrets/`.
- **Start with a fresh store volume per deployment.** The control plane is store-authoritative after the first seed, so a reused volume's enable toggles override darling.json — by design.
- **File permissions are yours on Linux.** The Windows build locks config/credentials down with ACLs; here the container boundary is the isolation, and the `secrets/` directory should be `chmod 700` with `600` files (the systemd shape should do the same for `darling.json` itself).
- **The store's preload list is the compose file's.** The store service passes `shared_preload_libraries` on its command line (TimescaleDB plus `pg_stat_statements`, for `get_store_query_stats`, #3899), and a command-line setting outranks both `postgresql.conf` and `ALTER SYSTEM`. To preload another library in the store, add it to that line in `docker-compose.yml`; an `ALTER SYSTEM` or conf edit of the list is ignored while the line is there.

**A container that has stopped collecting reports unhealthy** ([#4733](https://github.com/erikdarlingdata/PerformanceMonitor/issues/4733)). After a startup failure that stops collection for good (a rejected `darling.json`, a step that can never apply, a refused login), the `darling` container stays up on purpose and its healthcheck reports `unhealthy` in `docker compose ps`, rather than restarting it in a loop. `docker compose logs darling` says why, and so does `/api/ping` when the web dashboard is enabled. A failure the service is still retrying does not count as a stop. This needs the current `docker-compose.yml`: the healthcheck and `DARLING_STOPPED_MARKER` are set there, not in the image, so an older compose file has neither.

**systemd + bring-your-own PostgreSQL.** The tarball is framework-dependent, not self-contained. The host needs the ASP.NET Core 10 runtime and the .NET 10 runtime, from <https://dotnet.microsoft.com/download/dotnet/10.0>. The container image carries both already.

Download `PerformanceMonitorDarling-linux-x64-<version>.tar.gz` from the release, extract to `/opt/darling`, point `DARLING_CONFIG` at your config (connection string to your own PostgreSQL, at or above the minimum version in [Prerequisites](darling/install.md#prerequisites), with TimescaleDB; the service degrades gracefully without TimescaleDB), and run `dotnet PerformanceMonitor.Darling.Service.dll` under a unit like:

```ini
[Unit]
Description=PerformanceMonitor Darling
After=network-online.target

[Service]
ExecStart=/usr/bin/dotnet /opt/darling/PerformanceMonitor.Darling.Service.dll
Environment=DARLING_CONFIG=/etc/darling/darling.json
User=darling
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Use the same `env:`/`file:` secret references (systemd `LoadCredential=` pairs naturally with `file:`), and note `Microsoft.Data.SqlClient` needs `libgssapi-krb5-2` installed (`apt-get install libgssapi-krb5-2`) — the container image carries it already.
