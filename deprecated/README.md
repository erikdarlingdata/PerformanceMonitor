# Frozen Full Dashboard and CLI installer

`dashboard-installer-source.zip` holds the source of the deprecated Full Edition apps:

| Folder in the zip | What it is |
|---|---|
| `Dashboard/` | The Full Dashboard, a WPF app that reads the `PerformanceMonitor` database on a monitored SQL Server. Its own README is `Dashboard/README.md` |
| `Installer/` | The CLI installer for that database. Its README is `Installer/README.md` |
| `Installer.Core/` | The installation library the CLI installer and the Dashboard share |
| `Dashboard.Tests/`, `Installer.Tests/` | Their test projects |

The zip is a copy of `deprecated/` at commit `b68a28805` (3.12.0 on `dev`). It was made with `git archive`, so it holds tracked files only.

## Frozen

This source is frozen. Nothing in this repository builds it, tests it or releases it, and it is not in `PerformanceMonitor.sln`. The last release with a Dashboard or Installer download is [v3.2.0](https://github.com/erikdarlingdata/PerformanceMonitor/releases/tag/v3.2.0). New deployments use Lite or Darling ([Choosing an edition](../docs/editions.md)).

The T-SQL the installer deploys is not frozen in the zip: `install/` and `upgrades/` stay in the repository.

## Building it

The projects reference the shared `PerformanceMonitor.*` libraries, `Directory.Build.props` and `Directory.Packages.props` by relative path, and the installer embeds `install/` and `upgrades/`. Those files changed after the freeze. The Dashboard-only packages left `Directory.Packages.props`, and the shared projects no longer let the Dashboard see their internals. The zip therefore builds only against the repository at the commit above.

```cmd
git checkout b68a28805
dotnet build deprecated/Dashboard/Dashboard.csproj -c Release
dotnet publish deprecated/Installer/PerformanceMonitorInstaller.csproj -c Release
```

At that commit the folders are already under `deprecated/`. To start from the zip instead, check out the same commit in a clean working copy. Extract the zip into `deprecated/` so that the five folders sit directly under it. Then run the two commands.

Building the zip against a later commit is unsupported. To try it, undo those shared-code changes by hand.
