One secret per file, no trailing content beyond the value (a trailing newline is fine — references trim):

- `store_connection` — the full store connection string, e.g. `Host=store;Port=5432;Username=darling;Password=<store password>;Database=darling`
- `store_password.txt` — the same store password on its own, for the TimescaleDB container's `POSTGRES_PASSWORD_FILE`
- `sql_password.txt` — the monitoring login's SQL Server password
- `web_token.txt` / `mcp_token.txt` — the dashboard/MCP access tokens (generate long random values)
- `web_tls_cert.pem` / `web_tls_key.pem` — optional, and the only thing that keeps `web_token.txt` off the wire ([#2562](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2562)): the dashboard's PEM certificate and its PKCS#8 private key. Both files and the matching `web.network.tls` block are needed; the certificate must name the address browsers use, and the service refuses to expose the dashboard at all rather than fall back to HTTP if it is missing or expired

Keep this directory out of version control and readable only by the deploying user (`chmod 700 secrets`, `chmod 600 secrets/*`).
