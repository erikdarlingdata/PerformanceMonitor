# Performance Monitor Lite

Lightweight, agentless SQL Server performance monitoring desktop application. Monitors multiple SQL Server instances from a single dashboard without installing anything on target servers. Queries DMVs directly over the network and stores data locally in DuckDB with automatic Parquet archival.

Includes an embedded MCP server for exposing monitoring data to LLM clients (Claude Code, Cursor, etc.) via the Model Context Protocol.

Best for quick triage, Azure SQL Database, restricted environments, and consultant use.

## Install

1. Download [`PerformanceMonitorLite-lite-Setup.exe`](https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest) and run it. It also updates itself.
2. Start Lite from the Start Menu or the Desktop shortcut.
3. Click **+ Add Server**, enter the connection details, test, and save. Then double-click the server in the sidebar to connect.

Data starts flowing within 1-5 minutes. For the authentication modes, the certificate option and the grants that the login needs, read the first-server steps in the [Lite guide](../docs/lite-guide.md).

There is nothing to install first. `Setup.exe` and the portable `PerformanceMonitorLite-<version>.zip` are both self-contained win-x64 builds that carry their own .NET runtime. Windows itself must have its updates installed, or Lite can stop at start. See [Lite stops at start with a CET message](../docs/troubleshooting.md#lite-stops-at-start-with-a-cet-message). Monitored SQL Servers need nothing installed on them.

## More

- [Lite guide](../docs/lite-guide.md): install, upgrade, where Lite keeps its files, configuration, appearance and collectors
- [Run Lite headless](../docs/lite-guide.md#run-lite-headless): `--headless` and `--stop`, for a machine that nobody signs in to
- [Add servers with `--add-server`](../docs/lite-guide.md#add-servers-with---add-server): add servers with no window, and [on a machine with no desktop sign-in](../docs/lite-guide.md#add-servers-on-a-machine-with-no-desktop-sign-in)
- [Check the download](../docs/lite-guide.md#check-the-download): compare the portable ZIP with `SHA256SUMS.txt`
- [Troubleshooting](../docs/troubleshooting.md)
- [Changelog](../CHANGELOG.md)
- [Root README](../README.md) for the full documentation
