# Folder structure and building from source

Audience: contributors.

Part of the [Performance Monitor documentation](../README.md#documentation).

## Folder Structure

```
PerformanceMonitor/
│
│   Lite Edition — standalone desktop app, nothing installed on server
├── Lite/                  # Lite desktop application (WPF)
│
│   Darling Edition — headless service + bundled Postgres + viewer
├── Darling/               # Service, storage, analysis, viewer (see Darling/README.md)
│
│   Shared monitoring brain (collectors, alerting, analysis, MCP, UI)
├── PerformanceMonitor.*/  # Shared libraries used by every edition
│
│   Full Edition (deprecated) — server-installed collectors + separate dashboard
├── install/               # SQL installation scripts (Full edition)
├── upgrades/              # Version-specific upgrade scripts (Full edition)
├── deprecated/            # Frozen zip of the Full Dashboard and CLI installer source (see deprecated/README.md)
│
└── README.md              # This file
```

## Building from Source

All projects target .NET 10.0.

```
# Lite Edition
dotnet build Lite/PerformanceMonitorLite.csproj

# Darling Edition (service + viewer)
dotnet build Darling/PerformanceMonitor.Darling.Service/PerformanceMonitor.Darling.Service.csproj
dotnet build Darling/PerformanceMonitor.Darling.Viewer/PerformanceMonitor.Darling.Viewer.csproj
```

The Full Dashboard and the CLI installer (deprecated) are not part of the build. Their source sits in `deprecated/dashboard-installer-source.zip`, and [`deprecated/README.md`](../deprecated/README.md) explains how to build it from the commit it was taken from.
