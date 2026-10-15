# SQL Server Performance Monitor

<p align="center">
  <a href="https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest"><img src="https://img.shields.io/github/v/release/erikdarlingdata/PerformanceMonitor?style=for-the-badge" alt="Latest Release"></a>
  <a href="https://github.com/erikdarlingdata/PerformanceMonitor/blob/main/LICENSE"><img src="https://img.shields.io/github/license/erikdarlingdata/PerformanceMonitor?style=for-the-badge" alt="License: MIT"></a>
</p>

Free, open-source monitoring that replaces the tools charging you thousands per server per year. It includes specialized collectors, real-time alerts, and a built-in MCP server for AI analysis. Nothing phones home. Your data stays on your server and your machine.

It supports SQL Server 2016 SP2 to 2025. SQL Server 2017 needs CU3 or later. It also supports Azure SQL Managed Instance, AWS RDS for SQL Server, and Azure SQL Database (Lite and Darling). Darling also monitors PostgreSQL, including AWS RDS and Aurora.

![Fleet overview: nine servers at a glance, one flagged Critical with live blocking and deadlocks](Screenshots/fleet-overview.jpg)

## Editions

- **Lite** is a single desktop app that monitors your servers remotely, on demand. Nothing is installed on your server. It stores data in local DuckDB and Parquet files.
- **Darling** is a service on Windows or Linux that collects 24/7 into a central PostgreSQL store. The Windows viewer and the web dashboard read that store from any seat.

The Dashboard edition is deprecated. Existing installs keep working, but we recommend Lite or Darling for new deployments. [Choosing an edition](docs/editions.md) compares all three.

Not sure which one? Start with Lite. It is one download, nothing is installed on your server, and data starts to flow in under 5 minutes.

## Quick start: Lite

1. Download [`PerformanceMonitorLite-win-Setup.exe`](https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest). It is a self-contained build, so you do not install a .NET runtime first.
2. Run the installer. It adds Start Menu and Desktop shortcuts.
3. Start Lite from the Start Menu or the Desktop shortcut.
4. Click **+ Add Server**, enter the connection details, test, and save.
5. Double-click the server in the sidebar to connect.

Data starts to flow within 1 to 5 minutes. The monitoring login needs `VIEW SERVER STATE` ([Permissions](docs/permissions.md)).

Before you upgrade an older install with `Setup.exe`, read the warning in the [Lite guide](docs/lite-guide.md). On versions older than the data-relocation fix, `Setup.exe` deletes the data in the install directory. The Lite guide also covers the portable ZIP, collectors, storage, and configuration.

## Quick start: Darling

1. Download `PerformanceMonitorDarling-<version>.zip` from the [latest release](https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest). The machine needs the .NET Runtime 10 and the ASP.NET Core Runtime 10. The ASP.NET Core Hosting Bundle 10 installs both.
2. Copy `darling.sample.json` to `darling.json` and add your servers.
3. Run the service, in a console for a trial or as a Windows service.
4. Start the **Darling Viewer** from the `viewer\` folder in the same zip.

These steps are for Windows. For Linux, the full steps, and the web dashboard, read the [Darling quick start](docs/darling-quick-start.md). If you have not done this before, [docs/uat-onboarding.md](docs/uat-onboarding.md) is the ordered path from the zip to a working install. The [Darling operator guide](Darling/README.md) is the configuration reference.

## Documentation

| Page | What it covers |
|---|---|
| [Choosing an edition](docs/editions.md) | Lite, Darling, and Dashboard side by side |
| [Features and screenshots](docs/features.md) | What you get, with screenshots |
| [Lite guide](docs/lite-guide.md) | Install, upgrade, collectors, Extended Events sessions, storage, configuration |
| [Darling quick start](docs/darling-quick-start.md) | Install on Windows, Linux notes, web dashboard |
| [Darling operator guide](Darling/README.md) | Darling configuration, permissions, operations |
| [Tabs](docs/tabs.md) | What each tab shows |
| [Alerts and notifications](docs/alerts.md) | Alert types, channels, routes, email |
| [Agent job monitoring](docs/agent-job-monitoring.md) | Running SQL Agent jobs and jobs that run longer than expected |
| [MCP server](docs/mcp-server.md) | Setup and the tool list for AI analysis |
| [Authentication](docs/authentication.md) | Authentication types and credential profiles |
| [Permissions](docs/permissions.md) | Grants for on-premises, Azure, and AWS RDS |
| [Platform notes](docs/platform-notes.md) | AWS RDS and Aurora configuration |
| [Performance impact](docs/performance-impact.md) | Load on monitored servers, and local resources for Lite |
| [Troubleshooting](docs/troubleshooting.md) | Common problems for Lite and Darling |
| [Building from source](docs/building-from-source.md) | Folder structure and build commands |
| [Support and sponsorship](docs/support.md) | Commercial support tiers and sponsorship |
| [Third-party components](docs/third-party-components.md) | Third-party components and their licenses |
| [Changelog](CHANGELOG.md) | Changes in each release |

## Support

This project is free and open source under the MIT License, with no features withheld. Two commercial support tiers are available for organizations that need a vendor agreement or an invoice: **Supported** at $500/year and **Priority** at $2,500/year. [Support and sponsorship](docs/support.md) has the details. You can also [sponsor on GitHub](https://github.com/sponsors/erikdarlingdata).

## Sponsors

<table>
  <tr>
    <td><a href="https://signpath.io"><img src="docs/signpath_logo.svg" alt="SignPath" width="40"></a></td>
    <td>Free code signing on Windows provided by <a href="https://signpath.io">SignPath.io</a>, certificate by <a href="https://signpath.org">SignPath Foundation</a></td>
  </tr>
</table>

## License

Copyright (c) 2026 Darling Data, LLC. Licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Author

Erik Darling: [erikdarling.com](https://erikdarling.com) and [Darling Data, LLC](https://darlingdata.com)
