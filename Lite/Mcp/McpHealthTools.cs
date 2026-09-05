using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpHealthTools
{
    [McpServerTool(Name = "get_server_summary"), Description("Gets a quick health overview for a SQL Server instance: current CPU %, memory usage, recent blocking count, and deadlock count. Use this for a fast health check before drilling into specific areas.")]
    public static async Task<string> GetServerSummary(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name. Optional if only one server is configured.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var summary = await dataService.GetServerSummaryAsync(resolved.ServerId, resolved.ServerName);
            if (summary == null)
            {
                return McpHelpers.Status(
                    "unavailable",
                    $"No data available for {resolved.ServerName}. The collector may not have run yet.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                cpu_percent = summary.CpuPercent,
                memory_mb = summary.MemoryMb,
                blocking_count = summary.BlockingCount,
                deadlock_count = summary.DeadlockCount,
                last_collection = summary.LastCollectionTime?.ToString("o")
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_server_summary", ex);
        }
    }

    [McpServerTool(Name = "get_daily_summary"), Description("Gets a daily health summary: overall composite health band (Healthy/Warning/Critical), total wait time, top wait type, unique query count, deadlocks, blocking events, memory pressure (and severe memory pressure), high-CPU samples, collection errors, and actionable alert count for one day. Use this for a quick overview to decide which areas need investigation.")]
    public static async Task<string> GetDailySummary(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Summary date (yyyy-MM-dd), interpreted as a UTC day. Default is today.")] string? summary_date = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        DateTime? date = null;
        if (!string.IsNullOrEmpty(summary_date))
        {
            if (!DateTime.TryParse(summary_date, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var parsed))
                return $"Invalid date format '{summary_date}'. Use yyyy-MM-dd format (e.g., 2026-07-09).";
            date = parsed;
        }

        try
        {
            var row = await dataService.GetDailySummaryAsync(resolved.ServerId, date);
            if (row == null || !row.HasData)
            {
                var missDate = row?.SummaryDate ?? date ?? DateTime.UtcNow.Date;
                return McpHelpers.Status(
                    "empty",
                    $"No data collected for {resolved.ServerName} on {missDate:yyyy-MM-dd}.",
                    new { summary_date = missDate.ToString("yyyy-MM-dd"), overall_health = row?.OverallHealth });
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                summary_date = row.SummaryDate.ToString("yyyy-MM-dd"),
                overall_health = row.OverallHealth,
                health_band = row.HealthBand.ToString(),
                total_wait_time_sec = row.TotalWaitTimeSec,
                top_wait_type = row.TopWaitType,
                unique_queries = row.UniqueQueries,
                deadlock_count = row.DeadlockCount,
                blocking_events = row.BlockingEvents,
                high_cpu_events = row.HighCpuEvents,
                memory_pressure_events = row.MemoryPressureEvents,
                memory_critical_events = row.MemoryCriticalEvents,
                collection_errors = row.CollectionErrors,
                alert_count = row.AlertCount,
                max_block_duration_ms = row.MaxBlockDurationMs
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_daily_summary", ex);
        }
    }

    /// <summary>
    /// #2484: the daily rollup across a SPAN of days — the Performance Calendar's month grid, which had no
    /// read on either SKU.
    ///
    /// <para>A SIBLING of get_daily_summary rather than a wider version of it, and Darling's twin says the
    /// same in more detail: the single-day tool returns a flat object of scalars, a range returns rows, and
    /// one tool that returned either depending on whether a span argument arrived would make every consumer
    /// branch on a parameter it may not have sent. They share the ONE aggregate underneath, which is what
    /// stops them ever disagreeing about a day.</para>
    /// </summary>
    [McpServerTool(Name = "get_daily_summary_range"), Description("Gets the daily health summary for a SPAN of days rather than one: one row per collected day, each with its composite health band (Healthy/Warning/Critical), total wait time, top wait type, unique query count, deadlocks, blocking events with the peak block wait, high-CPU samples, memory pressure, collection errors and actionable alert count. This is what the desktop viewer's Performance Calendar month grid draws, and it is the read to use when the question is WHICH day rather than how one day went — scan the bands, then call get_daily_summary for the day that stands out. A day on which anything at all was collected appears here even if every signal was quiet (that day is Healthy, not missing), so a gap in the returned days is a gap in COLLECTION.")]
    public static async Task<string> GetDailySummaryRange(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Days of history, ending on the anchor day (inclusive). Default 30; max 366 (a year).")] int days_back = 30,
        [Description(McpHelpers.AsOfDaysDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        /* The ceiling is SHARED with Darling so the two SKUs cannot accept different spans. */
        if (days_back <= 0 || days_back > McpHelpers.MaxDailySummaryDaysBack)
            return $"Invalid days_back value '{days_back}'. Must be a positive integer (1-{McpHelpers.MaxDailySummaryDaysBack}).";

        /* The anchor is the ONLY source of "now" in this body — see AsOfWindowAnchorTests, which fails a
           tool that advertises as_of and then reads the process clock anyway. (That check is a source scan
           and the rule is absolute, so this comment cannot name the property either.) */
        var anchorError = McpHelpers.ResolveAsOf(as_of, out var windowEnd);
        if (anchorError != null) return anchorError;

        try
        {
            /* Days, not hours: the anchor names a DAY here and only its UTC date is used, because the
               aggregate buckets on whole days. The range is half-open [from, to), so `days_back` days
               ending ON the anchor day means the anchor day is the last one included. */
            var lastDay = windowEnd.Date;
            var fromDate = lastDay.AddDays(-(days_back - 1));
            var toDate = lastDay.AddDays(1);

            var rows = await dataService.GetDailySummaryRangeAsync(resolved.ServerId, fromDate, toDate);

            if (rows.Count == 0)
            {
                /*
                    Zero DAYS is two facts. The aggregate's day spine is a UNION over nine sources and one of
                    them is the collection log, where ANY run marks the day collected — that is why a quiet
                    but monitored day comes back Healthy rather than absent. So a range with no rows at all
                    cannot be "the server was quiet".

                    The denominator is therefore the DATA, probed on the collection log — the spine member
                    that guarantees a collected day appears. It is PERIODIC: every collector run writes a row
                    whatever it found, so its presence is proof somebody looked, and unlike an edge table it
                    cannot report a healthy server as uncollected. Darling's twin uses the same words.
                */
                var everCollected = await dataService.HasAnyCollectionLogAsync(resolved.ServerId);
                return everCollected
                    ? McpHelpers.Status(
                        "empty",
                        $"No collected days for {resolved.ServerName} between {fromDate:yyyy-MM-dd} and {lastDay:yyyy-MM-dd}. A day with ANY collection appears here even when every signal was quiet, so this range is outside what the store holds for this server rather than a stretch of quiet days — widen days_back, or move as_of.",
                        new { from_date = fromDate.ToString("yyyy-MM-dd"), to_date = lastDay.ToString("yyyy-MM-dd") })
                    : McpHelpers.Status(
                        "unavailable",
                        $"No collector runs have EVER been recorded for {resolved.ServerName}, so the calendar is empty because nothing has been collected — not because those days were quiet. Check that the service is running and that the server is enabled for collection.",
                        new { from_date = fromDate.ToString("yyyy-MM-dd"), to_date = lastDay.ToString("yyyy-MM-dd") });
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                days_back,
                /* The bounds the read actually used, echoed back: with an anchor in play, a caller cannot
                   otherwise tell which days they were given from the days they got. */
                from_date = fromDate.ToString("yyyy-MM-dd"),
                to_date = lastDay.ToString("yyyy-MM-dd"),
                /* Days WITH data, not days in the span. The two differ exactly where collection has a hole,
                   and that difference is the most useful thing on this payload. */
                day_count = rows.Count,
                days = rows.Select(row => new
                {
                    summary_date = row.SummaryDate.ToString("yyyy-MM-dd"),
                    overall_health = row.OverallHealth,
                    health_band = row.HealthBand.ToString(),
                    total_wait_time_sec = row.TotalWaitTimeSec,
                    top_wait_type = row.TopWaitType,
                    unique_queries = row.UniqueQueries,
                    deadlock_count = row.DeadlockCount,
                    blocking_events = row.BlockingEvents,
                    high_cpu_events = row.HighCpuEvents,
                    memory_pressure_events = row.MemoryPressureEvents,
                    memory_critical_events = row.MemoryCriticalEvents,
                    collection_errors = row.CollectionErrors,
                    alert_count = row.AlertCount,
                    max_block_duration_ms = row.MaxBlockDurationMs,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_daily_summary_range", ex);
        }
    }

    [McpServerTool(Name = "get_collection_health"), Description("Shows the health status of all data collectors for a server — whether they're running successfully, failing, or stale. A collector reads STOPPED rather than FAILING when it has attempted nothing at all — no success, no error, nothing — for longer than the FAILING cutoff, despite a history of runs: that is a collector whose gate (AppliesTo) flipped off for this target rather than one that keeps running and erroring, and it does not count toward a server's failing-collector total. Check this before investigating data to ensure collectors are working properly. Each row also carries last_note/note_count: what a NON-failing run reported, e.g. an enumeration that came back with 0 items. note_count equal to total_runs means the collector has been collecting nothing all window — not a fault (the target may be legitimately empty), but the reason a HEALTHY collector can still have no data. target_has_user_databases tells those two apart: true means the target DID have user databases in the same window, so an all-window empty enumeration is worth investigating (a login that cannot enter them, an exclusion filter that matched everything); false means either no user databases or no inventory to go on. Each row also carries abandoned and abandon_rate_pct: cycles the 120-second whole-server wall-clock budget gave up on, which stored nothing and advanced no watermark. Unlike a yield, which retries, an abandoned cycle is collected data you do not have. A rate above 0.5% bands the collector WARNING, so a WARNING here may have nothing to do with errors - read abandoned beside errors to attribute it. CRITICAL for reading last_error: it is a single slot carrying the newest ERROR or PERMISSIONS message in the whole window, and a message in it is NOT evidence that the condition is current. Read last_error_at for when it happened, last_denied_at for when the newest DENIAL specifically happened, and denied_since_last_success for the derived answer - true means a denial is the collector's current state, false means every denial in the window predates a later success and the collector is reading fine now. A fault recorded before a code path changed will sit in last_error for the rest of the window while every cycle since succeeds: pg_deadlocks moved from an in-database route to an AWS API route, and six days later this tool still showed HEALTHY, errors 0, a reassuring note and a stale permission denial together - a combination that describes a state which cannot occur, and which produced a bug report claiming a fleet-wide denial when the collector had been succeeding on all 50 targets. Do not infer a live condition from last_error alone. Total abandonment still reads FAILING through staleness; the rate exists for the partial case, where a collector abandons some cycles and succeeds often enough to stay fresh, which otherwise read HEALTHY with errors 0 indefinitely. The sweep_pressure block is the server-level roll-up: it compares the collectors' combined execution demand (average duration amortized by cadence) against the minute the fastest cadence holds. SATURATED means the collection body cannot fit inside its cadence, so relaunches are skipped and the server collects at a multiple of its configured interval while every collector still reads healthy — heaviest_collectors names where that budget goes. That verdict is the SUSTAINED answer only. peak_cycle_risk is the separate single-sweep answer: peak_cycle_ms is what the body costs on the cycle where every scheduled cadence comes due together, and BODY_OVERRUN means that one body cannot fit the budget even when the verdict reads OK — the signature of one infrequent heavy collector, which amortization hides and heaviest_collectors therefore ranks out of sight. peak_collector names it, and peak_cycle_note explains it. Read both fields: a server can be OK/BODY_OVERRUN (a schedule-shape problem, fix by moving or splitting that collector) or SATURATED/BODY_OVERRUN (a capacity problem). Every collector row carries avg_duration_ms, p95_duration_ms and max_duration_ms, because a collector's runs are not always one population: query_store on one dogfood server averaged 13,834 ms over 1,155 runs of which 958 yielded nothing and cost about 36 ms, which puts the other 197 at roughly 80,900 ms EACH - each one, on its own, larger than the whole sweep budget. Read the three together: avg close to p95 close to max is one population, avg far below p95 is two, and p95 far below max is one pathological run. peak_cycle_ms is built from p95 (floored at the mean, so it can never read lower than a mean-based figure) for exactly that reason, and peak_collector carries peak_run_ms beside avg_duration_ms so the gap is visible. Those three still describe RUNS, and a collector that runs once per DATABASE writes one blended row, so no run-level statistic can say which database cost what. Five fan out from an enumeration on any SQL Server target (query_store, plan_correction, query_store_health, index_object_stats, database_scoped_config); separately, eight more fan out over a per-database connection loop when the target is Azure SQL DB, and pg_autovacuum_stats always does on PostgreSQL. The per-collector `fanout` block is that answer, null for a collector that does not fan out: `items` is how wide the fan-out was, `slowest`/`slowest_ms` name the dearest database and its cost on the window's worst run, `run_ms` is that whole run, and `dominance` is slowest_ms * items / run_ms — 1.0 for a perfectly even fan-out, rising with concentration. It matters because the remedies diverge there: near 1.0 the cost is the fan-out's WIDTH and bounded parallelism is the lever, while around 2.0 or above one database dominates and a per-database schedule override or a stagger is what helps. Do not try to infer this from p95 versus avg — on a per-database collector that ratio is usually saturated by empty-versus-productive runs and says nothing about databases. Every field named so far describes what a collector SPENT; rows_stored, runs_with_rows and productive_run_pct are what it BOUGHT, counted over the same window as total_runs and the durations, so cost and output on a row always describe the same runs. Read them together for the three readings that need different actions: rows_stored above zero is expensive AND productive; rows_stored zero with denied_since_last_success false is a collector that read and found nothing, which for one that stores a row only when an event occurs (e.g. deadlocks, blocked_process_report, pg_blocking, pg_xmin_horizon) is the correct resting state and needs no action; rows_stored zero with denied_since_last_success true is a collector that could not read and needs a grant. output_finding says which of the two zero readings applies and is null whenever rows_stored is positive. This is deliberately NOT a band: pg_deadlocks was the single most expensive collector on one managed store, 49,258,335 ms over 79,333 runs in seven days, and stored zero rows - and that zero was CORRECT, because the reader was working on all 50 targets and there were no deadlocks to find. A verdict keyed on cost-plus-zero-rows would fire on the healthy quiet install rather than the blind one. These are NOT the hourly per-collector series Darling's get_collector_cost reports as total_rows - a separate series over that caller's own days_back and across every server at once, and Darling-only, so Lite has no twin of it; the top-level output_note names both windows and disclaims that one. rows_stored is also what a run STORED, never what the monitored engine counted, so a zero cannot tell a genuinely quiet source apart from a reader capturing nothing off a busy one - nothing on this surface measures that.")]
    public static async Task<string> GetCollectionHealth(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetCollectionHealthAsync(resolved.ServerId);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No collection health data available.");
            }

            var result = rows.Select(r => new
            {
                collector = r.CollectorName,
                status = r.HealthStatus,
                total_runs = r.TotalRuns,
                errors = r.ErrorCount,
                /* Deliberate 1s lock-timeout yields (#1805) — benign, distinct from errors; clustering
                   here is a lock-contention signal about the monitored server. */
                yields = r.YieldCount,
                /* #2804: runs the #2673 wall-clock budget gave up on. Unlike a yield, which retries, an
                   abandoned cycle stored nothing and advanced no watermark — it is data LOSS, and it is
                   the reason a WARNING here may have nothing to do with `errors`. Before this it reached
                   the surface only inside note_summary's prose, so there was no number to threshold,
                   alert or trend on. */
                abandoned = r.AbandonedCount,
                abandon_rate_pct = Math.Round(r.AbandonRatePercent, 2),
                failure_rate_pct = Math.Round(r.FailureRatePercent, 1),
                avg_duration_ms = Math.Round(r.AvgDurationMs, 0),
                /* #2460: the mean above is a blend whenever a collector's runs come in two sizes, and
                   on this fleet one of them plainly does — query_store averaged 13,834 ms over 1,155
                   runs where 958 yielded nothing at ~36 ms, which puts the other 197 at ~80,900 ms
                   each. p95 is what a HEAVY run of this collector costs and is what the peak-cycle
                   arithmetic below is built from; max is carried beside it so a routine tail can be
                   told from a single pathological cycle, which is the one thing a max alone cannot
                   say about itself. Read the three together: avg ~= p95 ~= max is one population,
                   avg << p95 is two, and p95 << max is one bad run. */
                p95_duration_ms = Math.Round(r.P95DurationMs, 0),
                max_duration_ms = Math.Round(r.MaxDurationMs, 0),
                /* #3017: what the spend BOUGHT, beside what it cost. Every field above this line
                   describes cost — the run count, the three durations, and the sweep-pressure roll-up
                   built from them — and none of them said whether any of it bought anything. The rows
                   figure lived on get_collector_cost, a different tool over a different (hourly,
                   fleet-wide) series, so correlating spend against output was a join a caller had to
                   know to make.

                   Measured: pg_deadlocks was the single dearest collector on a managed store —
                   49,258,335 ms over 79,333 runs in seven days, about 13.7 h/week — and stored zero
                   rows. THAT ZERO WAS CORRECT: the reader was working on all 50 targets and there were
                   no deadlocks to find. Which is exactly why this is a fact placed beside the cost and
                   NOT a band — a verdict keyed on cost-plus-zero-rows fires on the healthy quiet
                   install rather than the blind one, the cry-wolf failure #1852 exists to prevent.

                   Flat rather than a nested block like `fanout`: the denominator these are read against
                   is total_runs, which is already flat on this row, and nesting the numerator away from
                   its denominator would be the half-a-ratio shape the block would have existed to
                   prevent. runs_with_rows is get_pg_blocking's captures_with_blocking move — 12 rows
                   over 3 of 79,333 runs is a different collector from 12 rows over all of them. */
                rows_stored = r.RowsStored,
                runs_with_rows = r.RunsWithRows,
                productive_run_pct = Math.Round(r.ProductiveRunPercent, 1),
                last_success = r.LastSuccessTime?.ToString("o"),
                last_error = r.LastError,
                /* #3010: WHEN that error was, which the field never carried. `last_error` is a single slot
                   holding the newest ERROR/PERMISSIONS message in the window, and it was served with no
                   timestamp beside it — so a condition from six days ago, on a code path the collector no
                   longer takes, reads exactly like one from the last cycle.

                   That is not hypothetical. `pg_deadlocks` moved from the in-database pg_read_file route
                   to the RDS log API; its PERMISSIONS rows stop dead at the cutover and every cycle since
                   has been a SUCCESS on all 50 targets. Six days later this tool still reported HEALTHY,
                   errors 0, a reassuring note, AND `permission denied for function pg_read_file`. Every
                   element was individually true and together they described a server being refused right
                   now, which was false. A bug report was filed on exactly that reading.

                   So all three ride together: the instant of the newest failure of either class, the
                   instant of the newest DENIAL specifically, and the derived answer to the only question a
                   reader actually has — is this current, or a fossil. */
                last_error_at = r.LastErrorTime?.ToString("o"),
                last_denied_at = r.LastDeniedTime?.ToString("o"),
                denied_since_last_success = r.DeniedSinceLastSuccess,
                /* #3017's third term, and the whole reason this waited for #3010. rows_stored = 0 spans
                   two collectors that want opposite actions: one that read and found nothing, and one
                   that could not read. denied_since_last_success is what separates them, so the finding
                   sits directly beneath it and names which reading applies. Null when the collector
                   stored something — a note that fires on the healthy case is how a signal teaches
                   people to ignore it (FormatPeakCycleNote's own reasoning).

                   Composed from the shared formatter, like note_summary above, so the web table and any
                   other consumer cannot re-derive the sentence differently. Reading the predicate here
                   does not band on it: this is display text and HealthStatus never sees it. */
                output_finding = r.OutputFinding,
                /* #1837: what a NON-failing run reported — an enumeration that came back with 0 items,
                   items whose enumeration probe failed. note_count == total_runs means every run in the
                   window came back that way, which is the "collecting nothing for weeks" case that reads
                   as HEALTHY (correctly — an empty target is not a fault) and needs saying out loud. */
                last_note = r.LastNote,
                note_count = r.NoteCount,
                /* #1852: whether the store saw user databases on this target in the same window. The
                   fact that separates "nothing to collect" from "collecting nothing" — a caller
                   diagnosing an empty collector gets it as a boolean instead of parsing it out of the
                   sentence below. False also means "no inventory to go on", never "no databases". */
                target_has_user_databases = r.TargetHasUserDatabases,
                /* The same string both WPF grids render, composed on this side so the web dashboard and
                   any other consumer cannot re-derive it differently. */
                note_summary = CollectorHealthClassifier.FormatCollectionNote(
                    r.LastNote, r.NoteCount, r.TotalRuns, r.CollectorName, r.TargetHasUserDatabases),
                /* #2472: the per-database breakdown of a collector that fans out, null for one that does
                   not. A nested object rather than four sibling fields so a consumer cannot read a
                   slowest item without the width it has to be judged against — the parts only mean
                   something together, and `dominance` is that meaning. Field-for-field Darling's. */
                fanout = r.FanoutDominance is null ? null : new
                {
                    items = r.FanoutItems,
                    slowest = r.SlowestItem,
                    slowest_ms = r.SlowestItemMs,
                    run_ms = r.SlowestRunDurationMs,
                    dominance = Math.Round(r.FanoutDominance.Value, 2)
                }
            });

            /* #2296: the roll-up that makes half-rate collection visible. Every collector on a saturated
               server reads HEALTHY — from each one's own seat nothing is wrong — so the condition only
               existed as a service-log warning ("collection body has not completed … skipping relaunch").
               The verdict compares the collectors' combined execution demand (average duration amortized
               by cadence) against the minute the fastest cadence holds; heaviest_collectors names where
               the budget goes, which is the actionable half of the answer. */
            var pressure = SweepPressureClassifier.Compute(
                rows.Select(r => (r.CollectorName, r.AvgDurationMs, r.P95DurationMs, r.FrequencyMinutes)));
            var heaviest = rows
                .Where(r => r.FrequencyMinutes > 0 && r.AvgDurationMs > 0)
                .OrderByDescending(r => r.AvgDurationMs / r.FrequencyMinutes)
                .Take(3)
                .Select(r => new
                {
                    collector = r.CollectorName,
                    avg_duration_ms = Math.Round(r.AvgDurationMs, 0),
                    p95_duration_ms = Math.Round(r.P95DurationMs, 0),
                    max_duration_ms = Math.Round(r.MaxDurationMs, 0),
                    frequency_minutes = r.FrequencyMinutes,
                    /* #2446: the ranking key said out loud, beside the single-run cost it is derived from.
                       The list still ranks by amortized contribution, because that is what explains
                       busy_percent — but an operator reading it to find the collector that overran a body
                       was reading the wrong column with nothing on the row to say so. */
                    amortized_ms_per_minute = Math.Round(r.AvgDurationMs / r.FrequencyMinutes, 0),
                    /* #2460: "% of the budget PER RUN" now comes from the run that actually costs
                       something — PeakRunMs, the p95 floored at the mean — rather than from a mean that
                       on a bimodal collector describes no run at all. It is the same number the peak
                       cycle charges this collector, so the column and the cycle reconcile by hand;
                       taken from the mean, this row said query_store cost 23% of a body when its heavy
                       run costs 135% of one. Through the shared helper rather than re-derived here, so
                       the floor rule cannot drift between the two SKUs' tools. */
                    pct_of_sweep_budget_per_run = Math.Round(
                        SweepPressureClassifier.PeakRunMs(r.AvgDurationMs, r.P95DurationMs) / SweepPressureClassifier.SweepBudgetMs * 100.0, 1)
                });

            /* #2446: the collector that owns the most of ONE sweep, which is a different collector from
               the ones above whenever it is infrequent enough for amortization to hide it. Named on every
               server, not only on BODY_OVERRUN — knowing where a body's time concentrates is worth having
               before it is a problem, and this is exactly the row heaviest_collectors ranks out of sight. */
            var peakCollector = pressure.PeakCollectorName == null ? null : new
            {
                collector = pressure.PeakCollectorName,
                /* #2460: what one aligned body is charged for this collector — its p95, floored at its
                   mean — with the mean kept beside it, because on a bimodal collector the GAP between
                   the two is the finding. amortized_ms_per_minute stays derived from the mean: that is
                   what amortization means, and a rate built from a tail would claim work the server
                   never sustains. */
                peak_run_ms = Math.Round(pressure.PeakCollectorPeakRunMs, 0),
                avg_duration_ms = Math.Round(pressure.PeakCollectorAvgDurationMs, 0),
                frequency_minutes = pressure.PeakCollectorFrequencyMinutes,
                amortized_ms_per_minute = Math.Round(pressure.PeakCollectorAvgDurationMs / pressure.PeakCollectorFrequencyMinutes, 0),
                pct_of_sweep_budget_per_run = Math.Round(pressure.PeakCollectorPeakRunMs / SweepPressureClassifier.SweepBudgetMs * 100.0, 1)
            };
            var peakCycleNote = SweepPressureClassifier.FormatPeakCycleNote(pressure);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                sweep_pressure = new
                {
                    busy_ms_per_minute = Math.Round(pressure.BusyMsPerMinute, 0),
                    busy_percent = Math.Round(pressure.BusyPercent, 1),
                    verdict = pressure.Verdict,
                    /* #2446: the second dimension, and deliberately NOT folded into verdict. verdict
                       answers "does sustained demand fit the cadence on average"; this answers "does one
                       scheduled body fit at all". They disagree exactly when an infrequent heavy collector
                       owns most of a single sweep — which an amortized number cannot see by construction,
                       since dividing by that collector's own long cadence is what makes it small. Its own
                       vocabulary (FITS / BODY_OVERRUN) so it can never be read as a fourth verdict band,
                       and its own field so a fleet scan can filter on it. */
                    peak_cycle_ms = Math.Round(pressure.PeakCycleMs, 0),
                    peak_cycle_percent = Math.Round(pressure.PeakCyclePercent, 1),
                    peak_cycle_risk = pressure.PeakCycleRisk,
                    peak_collector = peakCollector,
                    peak_cycle_note = string.IsNullOrEmpty(peakCycleNote) ? null : peakCycleNote,
                    heaviest_collectors = heaviest,
                    note = pressure.Verdict switch
                    {
                        SweepPressureClassifier.Saturated =>
                            "The collection body cannot finish inside its cadence: relaunches are skipped every cycle and this server collects at a multiple of its configured interval, while each collector above correctly reads healthy from its own seat. The lever is capacity or placement (lighter or fewer scheduled collectors, a longer cadence, or a collector closer to the target), not collector repair.",
                        SweepPressureClassifier.AtRisk =>
                            "The collection body's average demand is close to its cadence; variance will intermittently push it over, skipping relaunches and stretching the delivered interval.",
                        _ => null
                    }
                },
                /* #3017: the windows, said once for the whole array rather than repeated on all ~41
                   rows. It names the window rows_stored/runs_with_rows were counted over — the same
                   fixed trailing seven days as total_runs and the durations, out of one aggregate, so
                   cost and output on a row can never describe different runs — and DISCLAIMS the one it
                   did not read: get_collector_cost's hourly series over the caller's own days_back and
                   across every server at once. That disclaiming is #3027's discipline one level down; a
                   sentence claiming both windows were read here would be the same defect it was written
                   to avoid. It also says outright that rows are what a run STORED and never what the
                   monitored engine counted, because nothing on this surface measures the second. */
                output_note = CollectorHealthClassifier.OutputWindowNote,
                collectors = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_collection_health", ex);
        }
    }

    [McpServerTool(Name = "get_collection_log"), Description("Gets the RAW per-run collection log for a server, newest first: one row per collector run with its total duration, the part spent querying the monitored server, the part spent writing to the local store, rows collected, status and any error. get_collection_health rolls these into a per-collector verdict; this is the underlying runs, which is what you need when the rollup says healthy and collection still looks wrong, or when you want to see what a collector was doing during a specific incident window.")]
    public static async Task<string> GetCollectionLog(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return, newest first. Default 200.")] int limit = 200,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        /* Same shared row-cap contract as Darling's twin: reject out of range, do not silently clamp. */
        var invalidLimit = McpHelpers.ValidateTop(limit);
        if (invalidLimit != null) return invalidLimit;

        /* ResolveAsOf here, deliberately NOT ValidateWindow. These three reads have never capped
           hours_back -- they Math.Abs() it and window on the result -- so routing them through the
           shared validator would impose the 168-hour ceiling every other read carries, and take reach
           away from exactly the read whose premise is looking FURTHER back than the default. The anchor
           is validated because it is new; the span keeps the behaviour callers already have. */
        var anchorError = McpHelpers.ResolveAsOf(as_of, out var windowEnd);
        if (anchorError != null) return anchorError;

        try
        {
            var hours = Math.Abs(hours_back);

            /* Over-fetch by one so truncation is observed, not inferred -- see Darling's twin. */
            var rows = await dataService.GetRecentCollectionLogAsync(resolved.ServerId, hours, maxRows: limit + 1, asOfUtc: windowEnd);
            var truncated = rows.Count > limit;
            if (truncated) rows = rows.Take(limit).ToList();

            if (rows.Count == 0)
            {
                /*
                    Zero rows is two different facts wanting opposite responses. A server that collected
                    and was simply quiet in THIS window is a true negative and the move is to widen it; a
                    server with no log rows at all has never collected, which is a fault, and telling that
                    caller "nothing in the last 24 hours" sends them off widening a window that will never
                    fill. Darling's twin makes the same distinction with the same words -- a user moving
                    between the SKUs must not be told a different story about the same state.
                */
                var everCollected = await dataService.HasAnyCollectionLogAsync(resolved.ServerId);
                return everCollected
                    ? McpHelpers.Status(
                        "empty",
                        $"No collector runs recorded for {resolved.ServerName} in the last {hours} hour(s). This server HAS collected before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent runs.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No collector runs have EVER been recorded for {resolved.ServerName}. This is not an empty window — collection has not run at all for this server. Check that collection is running and that the server is enabled; get_collection_health will be equally empty until it does.");
            }

            var result = rows.Select(r => new
            {
                collector = r.CollectorName,
                collection_time = r.CollectionTime.ToString("o"),
                duration_ms = r.DurationMs,
                /*
                    The split matters more than the total: a collector slow because the monitored server
                    is slow needs a different fix from one slow because the store is, and the total alone
                    cannot tell them apart. Named store_duration_ms rather than duckdb_duration_ms so the
                    two SKUs advertise ONE field name for one meaning -- the storage engine differs, the
                    question the caller is asking does not.
                */
                sql_duration_ms = r.SqlDurationMs,
                store_duration_ms = r.DuckDbDurationMs,
                rows_collected = r.RowsCollected,
                status = r.Status,
                error_message = r.ErrorMessage,
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back = hours,
                run_count = rows.Count,
                truncated,
                runs = result,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_collection_log", ex);
        }
    }

    [McpServerTool(Name = "get_current_waits_trend"), Description("Gets the two Current Waits series over time for a server: waiting-task total wait duration per wait type per collection, and blocked-session counts per database per collection. get_waiting_tasks answers 'what is waiting right now' and can never say whether it is worse than an hour ago — this is that question. Read the two series together: a wait-type spike with no blocked sessions is a resource wait, the same spike with them is contention.")]
    public static async Task<string> GetCurrentWaitsTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 4.")] int hours_back = 4,
        [Description("Limit the blocked-session series to one database. Omit for all databases.")] string? database_name = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        /* ResolveAsOf here, deliberately NOT ValidateWindow. These three reads have never capped
           hours_back -- they Math.Abs() it and window on the result -- so routing them through the
           shared validator would impose the 168-hour ceiling every other read carries, and take reach
           away from exactly the read whose premise is looking FURTHER back than the default. The anchor
           is validated because it is new; the span keeps the behaviour callers already have. */
        var anchorError = McpHelpers.ResolveAsOf(as_of, out var windowEnd);
        if (anchorError != null) return anchorError;

        try
        {
            var hours = Math.Abs(hours_back);
            var filter = string.IsNullOrWhiteSpace(database_name) ? null : new[] { database_name };

            var waits = await dataService.GetWaitingTaskTrendAsync(resolved.ServerId, hours, asOfUtc: windowEnd);
            var blocked = await dataService.GetBlockedSessionTrendAsync(resolved.ServerId, hours, databaseNames: filter, asOfUtc: windowEnd);

            if (waits.Count == 0 && blocked.Count == 0)
            {
                /*
                    Both empty is two facts, and the wrong one is the REASSURING one -- "nothing was
                    waiting" reads as all-clear and stops a caller looking. Same words as Darling's twin.
                */
                var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "waiting_tasks");
                if (gated != null)
                {
                    return gated;
                }

                var everCollected = await dataService.HasAnyWaitingTaskSampleAsync(resolved.ServerId);
                return everCollected
                    ? McpHelpers.Status(
                        "empty",
                        $"Nothing was waiting on {resolved.ServerName} in the last {hours} hour(s). The collector HAS sampled this server, so this is a genuine all-clear for the window rather than missing data.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No waiting-task samples have EVER been recorded for {resolved.ServerName}, so this is NOT an all-clear — there is nothing to read. Check that collection is running for this server before concluding it was quiet.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back = hours,
                database_name,
                waiting_tasks = waits.Select(w => new
                {
                    collection_time = w.CollectionTime.ToString("o"),
                    wait_type = w.WaitType,
                    total_wait_ms = w.TotalWaitMs,
                }),
                blocked_sessions = blocked.Select(b => new
                {
                    collection_time = b.CollectionTime.ToString("o"),
                    database_name = b.DatabaseName,
                    blocked_count = b.BlockedCount,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_current_waits_trend", ex);
        }
    }

    [McpServerTool(Name = "get_blocking_stats"), Description("Gets blocking SEVERITY over time for a server: per-minute blocking duration (event count, total, max and average wait) and per-minute deadlock severity (victim count plus total, max and average wait across every process in the graphs). Incident counts say how OFTEN; this says how BAD. Ten one-second blocks and one ten-minute block are the same count and are not the same problem.")]
    public static async Task<string> GetBlockingStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        /* ResolveAsOf here, deliberately NOT ValidateWindow. These three reads have never capped
           hours_back -- they Math.Abs() it and window on the result -- so routing them through the
           shared validator would impose the 168-hour ceiling every other read carries, and take reach
           away from exactly the read whose premise is looking FURTHER back than the default. The anchor
           is validated because it is new; the span keeps the behaviour callers already have. */
        var anchorError = McpHelpers.ResolveAsOf(as_of, out var windowEnd);
        if (anchorError != null) return anchorError;

        try
        {
            var hours = Math.Abs(hours_back);
            var blocking = await dataService.GetBlockingDurationStatsAsync(resolved.ServerId, hours, asOfUtc: windowEnd);
            var deadlocks = await dataService.GetDeadlockSeverityStatsAsync(resolved.ServerId, hours, asOfUtc: windowEnd);

            if (blocking.Count == 0 && deadlocks.Count == 0)
            {
                /* The denominator is whether we LOOKED, not whether we ever FOUND anything: these are
                   edge tables, and a healthy server that never blocked has no rows to find. Same words
                   as Darling's twin. */
                var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "blocked_process_report");
                if (gated != null)
                {
                    return gated;
                }

                var everRan =
                    await dataService.HasAnyBlockingCollectorRunAsync(resolved.ServerId)
                    || await dataService.HasAnyDeadlockCollectorRunAsync(resolved.ServerId);
                return everRan
                    ? McpHelpers.Status(
                        "empty",
                        $"No blocking or deadlocks recorded for {resolved.ServerName} in the last {hours} hour(s). The blocking collectors HAVE run successfully for this server, so the window is genuinely clear rather than blind.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"The blocking collectors have NEVER run successfully for {resolved.ServerName}, so this is NOT a clean bill of health — nothing looked. Blocked-process reports need the XE session running, or the DMV blocking snapshot collector enabled; check those before concluding this server does not block.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back = hours,
                blocking_duration = blocking.Select(b => new
                {
                    time = b.Time.ToString("o"),
                    event_count = b.EventCount,
                    total_duration_ms = b.TotalDurationMs,
                    max_duration_ms = b.MaxDurationMs,
                    avg_duration_ms = Math.Round(b.AvgDurationMs, 0),
                }),
                deadlock_severity = deadlocks.Select(d => new
                {
                    time = d.Time.ToString("o"),
                    victim_count = d.VictimCount,
                    /* Every process's wait, not just the victims' -- the Dashboard analyzer's semantics. */
                    total_wait_ms = d.TotalWaitMs,
                    max_wait_ms = d.MaxWaitMs,
                    avg_wait_ms = Math.Round(d.AvgWaitMs, 0),
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_blocking_stats", ex);
        }
    }
}
