# Performance Monitor Lite

Lightweight, agentless SQL Server performance monitoring desktop application. Monitors multiple SQL Server instances from a single dashboard without installing anything on target servers. Queries DMVs directly over the network and stores data locally in DuckDB with automatic Parquet archival.

Includes an embedded MCP server for exposing monitoring data to LLM clients (Claude Code, Cursor, etc.) via the Model Context Protocol.

Best for quick triage, Azure SQL Database, restricted environments, and consultant use.

## Prerequisites

**None.** Both artifacts are self-contained win-x64 builds and carry their own .NET runtime, so a stock Windows Server with no .NET installed runs either one. Windows itself must be current on its updates (see below).

| Artifact | What you install first |
|---|---|
| `PerformanceMonitorLite-lite-Setup.exe` (recommended) | **Nothing.** Self-contained; it also auto-updates |
| `PerformanceMonitorLite-<version>.zip` (portable) | **Nothing.** Self-contained; unzip and run |

### Windows must be up to date

.NET 10 apps need current Windows updates to start on a CPU with CET (Control-flow Enforcement Technology, a hardware protection). Without them, Lite stops at start. We saw this on Windows Server 2022 build 20348.1366. The exit code is `0x80131506` and stderr shows this message:

```text
Fatal error. Your Windows doesn't fully support CET. Please install all available Windows updates.
```

`Setup.exe` then ends with "Install Partially Succeeded" and gives no reason, because Velopack shows a fixed message when the app's install step fails. Velopack's velopack.log records the same exit code.

The fix is to install all available Windows updates and restart. If you cannot do that yet, there is a short-term workaround. Run it from an elevated PowerShell before `Setup.exe` or the first start. It turns off one protection (`UserShadowStack`, the hardware-enforced stack protection) for the Lite exe only:

```powershell
Set-ProcessMitigation -Name PerformanceMonitorLite.exe -Disable UserShadowStack
```

This weakens Lite's protection against stack-corruption attacks, so treat it as a stopgap. To undo it once Windows is patched, remove the values the first command wrote:

```powershell
Remove-ItemProperty -Path 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Image File Execution Options\PerformanceMonitorLite.exe' -Name MitigationOptions, MitigationAuditOptions, EAFModules -ErrorAction SilentlyContinue
```

On Windows Server 2022 build 20348.5622, `Set-ProcessMitigation -Remove -Disable UserShadowStack` did not remove the setting (it returned `C000000D`). `Set-ProcessMitigation -Reset` failed too ("Invalid input parameters"). The registry way above worked. The `Set-ProcessMitigation -Disable` command writes three values (`MitigationOptions`, `MitigationAuditOptions`, `EAFModules`) under the exe's key in `Image File Execution Options`. The undo removes only those values and leaves any other value on that key alone. Restart the program afterwards. The first command is described in Microsoft's [Set-ProcessMitigation](https://learn.microsoft.com/en-us/powershell/module/processmitigations/set-processmitigation) reference.

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

## Upgrading an install older than 3.4.0

Do not run `Setup.exe` over a Lite version older than 3.4.0. Before 3.4.0, Lite kept its DuckDB store, Parquet archive, logs, and `settings.json` inside `%LOCALAPPDATA%\PerformanceMonitorLite`. That folder is also the install folder, and `Setup.exe` deletes it before the new build starts. The history is then lost. Lite 3.4.0 and later keep that data in `%LOCALAPPDATA%\PerformanceMonitorLite-Data`, so `Setup.exe` does not touch it.

To find your version, click **About** in the left sidebar.

If your version is older than 3.4.0, use one of these two ways:

1. Click **About**, then install the update from there. This upgrades in place and keeps your data.
2. Close Lite and copy `%LOCALAPPDATA%\PerformanceMonitorLite` to a safe place. Then run `Setup.exe`.

The first start of 3.4.0 or later moves the old data to the new folder. Your server list and your saved passwords are not in the install folder, so a `Setup.exe` upgrade does not delete them.

## Themes and colors

Lite ships three themes — **Dark** (the default), **Light** and **Cool Breeze** — picked from **Settings → Color theme**, applied live. Every surface in the app is painted from one palette of eighteen named colors per theme, and you can adjust twelve of them yourself (#3577), per theme, with a reset to the shipped values.

Open **Settings**, expand **Colors** beneath the theme drop-down, and you get one row per color for the theme currently showing: a swatch, the hex, a picker (hue / saturation / brightness sliders) and the **measured contrast ratio** against the surface that color renders on — page background against text, accent against the ink that sits on it, each status color against the page. Green is 4.50:1 or better (WCAG AA for text), amber is 3.00–4.49 (fine for a marker, hard as text), red is under 3.00. The readout is a measurement, not a gate: **Apply** always works. **Reset to default** returns the current theme to its shipped palette and leaves your other themes alone. **Open file** opens the JSON in your editor; anything you save there applies live too.

The file is `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\theme-overrides.json`, beside `settings.json`. It is keyed by theme, then by color, and holds only what you changed:

```json
{
  "Dark": {
    "AccentColor": "#3A7BD5",
    "AccentForegroundColor": "#FFFFFF"
  },
  "CoolBreeze": {
    "WarningColor": "#B45309"
  }
}
```

Values are `#RRGGBB` (`#AARRGGBB` is accepted). A key that is not one of the twelve, a value that is not a color, a theme that is not one of the three, or a file that is not JSON is skipped with a line in the log and never stops Lite from starting. Deleting the file, or a theme's block, is the same as Reset.

| Key | Group | What it paints |
|---|---|---|
| `BackgroundColor` | Backgrounds | The window and panel background most text sits on |
| `BackgroundLightColor` | Backgrounds | Cards, settings sections, buttons at rest, headers — a step lighter than the page |
| `BackgroundDarkColor` | Backgrounds | The sidebar, grid rows and other recessed areas — a step darker than the page |
| `ForegroundColor` | Text | Primary text: values, headers, labels |
| `ForegroundDimColor` | Text | Captions and secondary labels |
| `AccentColor` | Accent | The selected tab, the selected grid row, highlighted combo items, accent buttons, links |
| `AccentForegroundColor` | Accent | The ink for text and glyphs on an accent fill |
| `AlternatingRowColor` | Rows | Every other grid row |
| `SuccessColor` | Status | Healthy / OK markers and the Done row mark |
| `WarningColor` | Status | Warning markers and the To Do row mark |
| `ErrorColor` | Status | Error / critical markers and the Do Not Do row mark |
| `InfoColor` | Status | Informational markers |

The other six palette colors (`AccentHoverColor`, `AccentPressedColor`, `BackgroundLighterColor`, `ForegroundMutedColor`, `BorderColor`, `BorderLightColor`) are derived tones — a hover is the accent lightened a step, a border is the background lifted a step — and follow the theme author's values; they are not adjustable, and neither is any single control (the selected tab, one button): the knobs are the palette, and every surface built from it follows. A handful of component-specific brushes the themes paint with a fixed value (the plan viewer's panels, the time-range slicer, the deadlock graph) are likewise not derived from the palette and do not move.

See the [root README](../README.md) for full documentation.
