---
name: build
description: Build and locate the PerformanceMonitor executables
argument-hint: [lite|darling|viewer]
disable-model-invocation: false
---

# Build PerformanceMonitor

Build a component of PerformanceMonitor and report the result. All paths are relative to the repository root.

## Arguments

`$ARGUMENTS` specifies which component to build:

| Argument | Project | Output TFM |
|---|---|---|
| `lite` | `Lite/PerformanceMonitorLite.csproj` | `net10.0-windows` |
| `darling` or `service` | `Darling/PerformanceMonitor.Darling.Service/PerformanceMonitor.Darling.Service.csproj` | `net10.0` |
| `viewer` | `Darling/PerformanceMonitor.Darling.Viewer/PerformanceMonitor.Darling.Viewer.csproj` | `net10.0-windows` |
| (empty) | build whatever is in the current working directory, or ask | |

Lite, the Darling service, and the Darling Viewer are the shipping artifacts. The full Dashboard and the CLI Installer are frozen in `deprecated/dashboard-installer-source.zip`. Nothing builds, tests or releases them from this repository, so there is no `dashboard` or `installer` argument. `deprecated/README.md` says how to build them from the zip.

## Steps

1. Map `$ARGUMENTS` to the project path above.
2. Run `dotnet build <path>`, Debug configuration by default.
3. Report the result:
   - **success**: show the output path (e.g. `Lite/bin/Debug/net10.0-windows/`) and the executable name
   - **failure**: show the error messages clearly and suggest fixes
4. Do NOT automatically launch the executable. Just say where it is.

## Notes

- Use `dotnet build` with paths relative to the repository root. Do NOT `cd` into directories.
- If the build fails because the exe is locked by a running instance, kill it (`taskkill /F /PID <pid>`) and rebuild.
- If a project path cannot be determined, run `git ls-files "*.csproj"` to list the real projects. Stale `bin/` and `obj/` folders are left behind at the old top-level `Dashboard/` and `Installer/` paths and are not projects.
