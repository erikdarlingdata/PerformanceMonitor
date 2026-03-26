# Session Context — 2026-03-26 08:15

## Current Task
Starting #689/#691 — Progressive server summary landing view with USE/RED/Golden Signals framing. These are being implemented together. Design sketch is on issue #691.

## Decisions Made

### Session-wide
- Overlay on the slicer canvas, never on ScottPlot charts on other tabs (feedback saved)
- Dots not lines for overlays — every execution gets a dot, no minimum threshold
- Drill-down window ±30 min (±15 was too narrow, missed data due to spike chart zero-baseline rendering)
- Custom date pickers must be populated on drill-down so user can explore other tabs
- `_isRefreshing` guard needed on CustomDateRange_Changed/CustomTimeCombo_Changed/TimeRangeCombo_SelectionChanged to prevent cascading refreshes during programmatic picker updates
- Chart drill-down added to all major charts, skipping Resource Metrics detail charts and Query Performance Trends pending #689/#691 consolidation
- DuckDB path migrated to `%LOCALAPPDATA%\PerformanceMonitorLite\monitor.duckdb` — the old `bin/Debug` path has stale data

### #689/#691 Design
- Combined implementation: #691 provides framework vocabulary, #689 provides the UI
- Replaces default landing tab (Wait Stats in Lite, Resource Overview in Dashboard)
- All data sources already exist — aggregation + presentation only
- Lite first, then Dashboard port
- Investigate buttons reuse #684 pattern
- USE Method sections: CPU, Memory, Disk I/O, TempDB, Workers (each with Utilization/Saturation/Errors)
- RED Method section: Rate, Errors, Duration
- Recent Incidents section: ranked problems with navigate links
- Full design sketch posted on issue #691

## Work Completed This Session

### Issues closed:
- #676 — CREATE DATABASE model DB size fix (PR #678)
- #677 — Azure SQL DB server_id collision (community PR #680)
- #681 — Slicer time range fixes + new slicers (PRs #697, #698)
- #683 — Grid-to-slicer dot overlay (PRs #699, #700, #701)
- #684 — Critical Issues investigate button (PR #702)
- #682 — Chart drill-down (PRs #705, #706, #708, #709, #711, #714, #717)
- #704 — Slicer custom range display fix (PR #707)
- #694 — Support question answered
- #695 — sp_BlitzLock debugging comment posted

### Key PRs:
- All merged to dev via squash+admin
- Branch protection requires PRs — cannot push directly to dev

## Work Remaining

### Immediate: #689/#691
- New `ServerSummaryControl` UserControl for Lite
- Data aggregation queries pulling from existing DuckDB tables
- USE/RED framework categorization
- Severity thresholds per signal
- Navigate-to-tab actions (pattern from #684)
- Dashboard port after Lite validation

### Deferred:
- #686 — Unified query detail panel (too much work, needs #689 first)
- #685 — Inline sparklines in grids
- #687 — Before/after comparison for query grids
- #688 — Correlated timeline lanes
- #690 — Heatmap for query duration
- #692 — Dynamic baselines (foundation for #693)
- #693 — Anomaly detection
- #696 — XE sessions stay running after Lite closes (design choice)
- Dashboard port of remaining Resource Metrics drill-downs (post #689)
- Dashboard chart time display mode for ScottPlot charts (pre-existing issue)

## Important Context

### File paths
- Lite slicer: `Lite/Controls/TimeRangeSlicerControl.xaml.cs`
- Dashboard slicer: `Dashboard/Controls/TimeRangeSlicerControl.xaml.cs`
- Lite ServerTab: `Lite/Controls/ServerTab.xaml.cs` (~4500 lines)
- Dashboard ServerTab: `Dashboard/ServerTab.xaml.cs` (~2000 lines)
- Dashboard QueryPerformanceContent: `Dashboard/Controls/QueryPerformanceContent.xaml.cs`
- DuckDB path: `C:/Users/edarl/AppData/Local/PerformanceMonitorLite/monitor.duckdb` (NOT the bin/Debug path)

### Timezone notes
- Server (sql2022): Pacific time (UTC-7 DST / UTC-8 standard)
- User machine: Eastern time (UTC-4 DST / UTC-5 standard)
- `ServerTimeHelper.UtcOffsetMinutes` is the SERVER's offset from UTC
- Chart X-axis data is in server local time (UTC + UtcOffsetMinutes)
- Hover helper returns server local time
- Date pickers show user's local time
- DuckDB stores collection_time in UTC

### Git workflow
- ALWAYS use feature branches + PRs (branch protection on dev and main)
- `gh pr merge --squash --admin` to merge
- Never push directly to dev — will be rejected

### Build issues
- `taskkill` not reliably killing processes this session — Defender or runtime holding file locks
- User has to manually close apps before rebuild
- Dashboard MCP server can also hold DLL locks
