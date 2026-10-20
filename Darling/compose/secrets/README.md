Run `./setup.sh` in the folder above this one, then run `docker compose up -d`. The script makes this folder and every file that a default install needs. It never changes a file that exists. It writes:

- the store password and the matching `store_connection`,
- the web token and the MCP token,
- `../darling.json`, copied from `../darling.sample.json`.

It asks for the SQL login's password with hidden input, or reads it from `DARLING_SQL_PASSWORD`. With `--no-prompt` and no variable, it skips `sql_password.txt` and tells you to make that file. It never prints a secret.

The list below is the reference for making the files by hand. Each secret is one file with no trailing content beyond the value. A trailing newline is fine, because references trim it:

- `store_connection` — the full store connection string, e.g. `Host=store;Port=5432;Username=darling;Password=<store password>;Database=darling`
- `store_password.txt` — the same store password on its own, for the TimescaleDB container's `POSTGRES_PASSWORD_FILE`
- `sql_password.txt` — the monitoring login's SQL Server password
- `web_token.txt`: the dashboard access token (a long random value)
- `mcp_token.txt`: the MCP access token (a long random value). You need it only with MCP on. The `mcp_token` secret is commented out in `docker-compose.yml` until you uncomment it there and set `mcp.enabled` to `true` in `darling.json`
- `web_tls_cert.pem` / `web_tls_key.pem` — optional, and the only thing that keeps `web_token.txt` off the wire ([#2562](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2562)): the dashboard's PEM certificate and its PKCS#8 private key. Both files and the matching `web.network.tls` block are needed; the certificate must name the address browsers use, and the service refuses to expose the dashboard at all rather than fall back to HTTP if it is missing or expired

Nothing here for the store's least-privilege `admin`/`viewer`/`mcp` roles: the service provisions them on the compose store itself and generates their passwords into the `darling-credentials` volume ([#3914](https://github.com/erikdarlingdata/PerformanceMonitor/issues/3914)). The `store_connection` login must be the store's bootstrap superuser (`POSTGRES_USER`, `darling` as shipped) for that to happen.

Nor is the password key kept here. It is the key for the SQL login passwords saved in the store. The service makes it on first start and keeps it as `password-key` in the same `darling-credentials` volume ([#5366](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5366)). Back that volume up after the first start and after every `--reset-password-key`, and keep the backup apart from the store's backups. If the key may have been copied, reset it, change the SQL logins' passwords, and delete `password-key.retired` from the volume.

Keep this directory out of version control and readable only by the deploying user (`chmod 700 secrets`, `chmod 600 secrets/*`).
