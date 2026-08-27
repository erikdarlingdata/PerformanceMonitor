# Performance Monitor Lite

Lightweight, agentless SQL Server performance monitoring desktop application. Monitors multiple SQL Server instances from a single dashboard without installing anything on target servers. Queries DMVs directly over the network and stores data locally in DuckDB with automatic Parquet archival.

Includes an embedded MCP server for exposing monitoring data to LLM clients (Claude Code, Cursor, etc.) via the Model Context Protocol.

Best for quick triage, Azure SQL Database, restricted environments, and consultant use.

## Prerequisites

**None.** Both artifacts are self-contained win-x64 builds and carry their own .NET runtime, so a stock Windows Server with no .NET installed runs either one.

| Artifact | What you install first |
|---|---|
| `PerformanceMonitorLite-win-Setup.exe` (recommended) | **Nothing.** Self-contained; it also auto-updates |
| `PerformanceMonitorLite-<version>.zip` (portable) | **Nothing.** Self-contained; unzip and run |

### Why the ZIP changed, and why it got smaller

Through 3.5.0 the portable ZIP was built without a bundled runtime, and it needed two of them: the .NET Desktop runtime for the WPF window, and the ASP.NET Core one — unconditionally, which is the part nobody expects. `PerformanceMonitorLite.csproj` references `ModelContextProtocol.AspNetCore`, and that package brings the `Microsoft.AspNetCore.App` framework reference in transitively, so the built `PerformanceMonitorLite.runtimeconfig.json` named **three** frameworks whether or not the MCP server was ever switched on. Turning MCP off in settings did not change it; it is decided at build time, not at run time.

**If one was missing, nothing of ours was on screen.** The .NET host resolves the frameworks named in the runtimeconfig before a single line of Lite’s code runs, so the failure was the host’s own `You must install .NET to run this application`, with no product branding and no instructions. It also reports only the **first** framework it cannot find, so installing one bought a second copy of the same error. Lite is launched by double-clicking an exe and has no install script, so there was nowhere to put a pre-flight gate the way [Darling’s `install-darling.ps1`](../Darling/tools/install-darling.ps1) does — the host error precedes our code, and nothing in the app can report it.

Pinning the publish to `win-x64` and bundling the runtime removed that failure entirely, and it made the download **smaller**, which is the counter-intuitive part. The old publish was RID-agnostic, so it copied every platform its packages ship: 537&nbsp;MB of `runtimes\` on a 565&nbsp;MB tree — macOS, Linux, ARM64, musl, loongarch, riscv64 — of which only the 52&nbsp;MB `win-x64` folder could ever load. `DuckDB.NET.Bindings.Full` is most of that, with SkiaSharp and SqlClient behind it. Dropping ~485&nbsp;MB of unloadable native payload beat the cost of bundling .NET, WPF and ASP.NET Core by roughly two to one:

| Publish | Tree | Zipped |
|---|---|---|
| portable, no bundled runtime (through 3.5.0) | 565&nbsp;MB | 212.7&nbsp;MB |
| self-contained win-x64 (now) | 277&nbsp;MB | 114.2&nbsp;MB |

`READ-ME-FIRST.txt` still ships in the ZIP beside `PerformanceMonitorLite.exe`, now saying there is nothing to install rather than listing downloads.

Monitored SQL Servers need nothing installed on them either way.

See the [root README](../README.md) for full documentation.
