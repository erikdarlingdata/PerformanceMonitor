# Session Context — 2026-03-25 21:55

## Current Task
Issue #683: Grid-to-slicer overlay for query/procedure/Query Store tabs. Click a row in a grid, see that item's individual trend overlaid on the slicer chart above the grid.

Working on branch `feature/dashboard-grid-overlay-683` which covers BOTH Lite and Dashboard changes.

## Decisions Made
- **Overlay goes on the slicer canvas**, NOT on ScottPlot charts on other tabs. Previous attempt targeted QueryDurationTrendChart (Performance Trends sub-tab) — user rejected this as nonsensical UX. See memory: `feedback_chart_overlay_design.md`.
- **Dots, not lines**: Lines created confusing visual artifacts (spikes through zero-delta points, ascending phantom lines). User wants dots for every execution — even 1 or 2 points.
- **Metric follows slicer sort column**: Sorting by CPU shows CPU overlay, sorting by Duration shows duration, etc. Sort change re-fires the overlay with new metric.
- **Own Y scale**: Overlay is scaled to its own max, independent of the aggregate slicer chart.
- **Lite values are cumulative**: `delta_elapsed_time` in DuckDB `v_query_stats` is actually cumulative from `dm_exec_query_stats`. Must compute actual per-interval change: `history[i].DeltaElapsedUs - history[i-1].DeltaElapsedUs`. Zero deltas filtered out.
- **Dashboard values are real deltas**: `TotalElapsedTimeDelta` etc. are computed server-side. No cumulative math needed.
- **Query Store labels**: Use `ModuleName` when present (e.g., "dbo.neword"), fall back to `Query {id} / Plan {id}`.
- **History methods updated**: All three history methods (QueryStats, ProcedureStats, QueryStore) now accept `fromDate`/`toDate` to match the view's actual time range.

## Approaches Tried and Rejected
1. **ScottPlot overlay on Performance Trends tab** — wrong chart, different tab from the grid
2. **ScottPlot secondary Y-axis** — overlay rendered behind FillY area, invisible at aggregate scale
3. **Lines on slicer** — created confusing visual artifacts, especially with sparse or cumulative data
4. **Minimum 3 data points** — user wants every execution visible, even single dots

## Work Completed

### PRs merged to dev:
- **PR #678** — Fix CREATE DATABASE failure when model DB has large files (#676)
- **PR #680** — Fix Azure SQL DB server_id collision (#677) — community contributor
- **PR #697** — Lite: fix slicer time ranges + add Blocking/Deadlock slicers (#681)
- **PR #698** — Dashboard: same + Default Trace slicer (#681)
- **PR #699** — Lite: grid-to-slicer overlay (#683)

### PR #700 open (feature/dashboard-grid-overlay-683):
- Dashboard overlay implementation (3 grids)
- Dots-instead-of-lines for both Lite and Dashboard
- Int16/Int32 cast fix in Dashboard query stats history reader
- Dot clamping to bucket range
- Lite `_isRefreshing` guard for overlay persistence

### Issues closed: #676, #677, #681
### Issues commented: #694 (support question → recommended Lite), #695 (sp_BlitzLock debugging)

### Commits on current branch:
- `a06abc1` — Dashboard: grid-to-slicer overlay
- `242bacd` — Switch overlay to dots, clamp, no minimum
- `95c433b` — WIP: bound dots to bucket range, refresh guard

## Work Remaining on #683

### Active bugs:
1. **Dots still extend past the blue area chart end** — Clamping to first/last bucket time may not be enough. The NormAtUtc mapping places dots between 0-1 in the full DataStartUtc..DataEndUtc range, but the blue area chart only draws to the last bucket POINT position. Dots between the last bucket and DataEndUtc appear past the drawn area. May need to clamp to `_data[^1].BucketTimeUtc` (without AddHours) as the upper bound.
2. **Dashboard auto-refresh deselects rows** — `_isRefreshing` is in `ServerTab.xaml.cs` but handlers are in `QueryPerformanceContent.xaml.cs`. Need to either expose the flag or add a local one.
3. **Visual noise** — User says "graphs look completely insane" with dots. Hundreds of individual Ellipse elements may be too noisy. May need to reduce dot density or aggregate to sub-hourly buckets.
4. **Dashboard Procedure Stats "No data for selected time range"** — Pre-existing slicer issue, not overlay-related.

### Not started:
- Dashboard port needs the `_isRefreshing` equivalent
- May need to reconsider dot vs line approach based on user feedback about "insane" visuals

## Important Context

### Key files:
- `Lite/Controls/TimeRangeSlicerControl.xaml.cs` — Slicer with overlay drawing (Lite, UTC times)
- `Dashboard/Controls/TimeRangeSlicerControl.xaml.cs` — Same for Dashboard (server local times)
- `Lite/Controls/ServerTab.xaml.cs` — Selection handlers, overlay helpers (~line 2315+)
- `Dashboard/Controls/QueryPerformanceContent.xaml.cs` — Dashboard selection handlers (~line 507+)
- `Lite/Services/LocalDataService.QueryStats.cs` — History methods with fromDate/toDate
- `Lite/Services/LocalDataService.QueryStore.cs` — QueryStore history with fromDate/toDate

### Model differences:
- Lite: `QueryStatsRow`, `ProcedureStatsRow`, `QueryStoreRow` (in LocalDataService files)
- Dashboard: `QueryStatsItem`, `ProcedureStatsItem`, `QueryStoreItem` (in Models/)
- Lite history: `QueryStatsHistoryRow` — cumulative values, compute deltas client-side
- Dashboard history: `QueryStatsHistoryItem` — has `TotalElapsedTimeDelta` etc. (real deltas)
- Dashboard `QueryExecutionHistoryItem` — Query Store history, uses `CountExecutions` not `ExecutionCount`, `AvgDurationMs` is a computed property from `AvgDuration / 1000.0`

### Gotchas:
- Dashboard `GetQueryStatsHistoryAsync` had Int16 cast bug on MinDop/MaxDop (fixed)
- Dashboard `GetProcedureStatsHistoryAsync` takes (database, schema, procedureName) not (database, objectId)
- Slicer `overlayBrush` name conflicts with selection `overlayBrush` — use `dotBrush`
- `NormAtUtc`/`NormAtTime` internally clamp to [0,1] — can't use return value to detect out-of-range
- UX work order at `C:\GitHub\PerformanceMonitor-ux-enhancements\UX_WORK_ORDER.md`
