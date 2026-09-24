using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The latch / spinlock contention MCP tools — get_latch_stats, get_spinlock_stats — served over Lite's
/// DuckDB store. Each wraps the existing latest-snapshot reader (GetLatchStatsSnapshotAsync /
/// GetSpinlockStatsSnapshotAsync): the per-class / per-spinlock cumulative counters plus the last
/// collection interval's delta at the most recent collection in the window. STORED reads, no live hit.
///
/// <para>
/// <b>The unknowable row is spelled one way on both SKUs (#3653 A16).</b> Darling's twins are window
/// AGGREGATES and these are the newest snapshot, so the rows as a whole differ and always have; the keys that
/// spell "this row's latest interval was unknowable" do not. A restart / first-sample row stores
/// <c>sample_interval_seconds = 0</c> beside deltas of 0 that were never measured (#3540's marker), and both
/// SKUs publish it as: the <c>delta_*</c> null (#3642's rule, here since #3702), the per-second rates null,
/// and <c>interval_seconds</c> null beside them — the why. Before this a caller here read a bare null delta
/// with nothing to say whether the interval was unknowable or the row was quiet, and Darling's caller read a
/// null rate beside a delta of 0. <see cref="KnownInterval"/> / <see cref="PerSecond"/> are the rule;
/// <c>Darling.Tests/McpPageContractTests.TheSameToolName_SpellsTheUnknowableRowTheSameWay_OnBothSkus</c>
/// holds the key set equal per tool. A pre-v60 row that never stored an interval publishes its deltas (they
/// are real) with <c>interval_seconds</c> and the rates null — the snapshot read has no previous row to LAG
/// against, so the rate is not knowable from one row and is not invented.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class McpLatchSpinlockTools
{
    /// <summary>The stored interval as the wire publishes it: the seconds when measured, null when the store
    /// holds the calculator's 0 marker or (pre-v60) nothing. Darling's reader collapses the same two to null in
    /// SQL (<c>NULLIF(…, 0)</c> then a LAG that has no predecessor), so <c>interval_seconds</c> reads the same
    /// on both SKUs; the deltas beside it carry the 0-versus-NULL distinction (null on the marker, real on a
    /// pre-v60 row).</summary>
    private static int? KnownInterval(int? sampleIntervalSeconds) =>
        sampleIntervalSeconds is int seconds && seconds > 0 ? seconds : null;

    /// <summary>A snapshot delta over the stored interval it accrued over, rounded as Darling rounds its rates;
    /// null when either side is unknowable, so a restart cannot read as 0.00 of anything per second.</summary>
    private static double? PerSecond(long? delta, int? sampleIntervalSeconds) =>
        delta is long measured && KnownInterval(sampleIntervalSeconds) is int seconds
            ? Math.Round((double)measured / seconds, 2)
            : null;

    [McpServerTool(Name = "get_latch_stats"), Description("Latch contention by class. Darling sums waits over the whole hours_back window and returns the top classes by that total. Lite: LATEST IS A TIME, only the newest snapshot within hours_back, paged to limit, heaviest last-interval wait first. interval_seconds, both per-second rates and (on Darling) severity come from the LATEST interval only; on a restart or first sample (interval unknowable, stored 0) they are null, never 0 or LOW. Zero rows: not_collected on a non-SQL Server target, else unavailable, never empty. <<GUIDE>> Gets the latest latch-contention snapshot by latch class: cumulative waiting requests and wait time (with the max single wait) plus the last collection interval's delta waits. ACCESS_METHODS_DATASET_PARENT synchronizes child dataset access to the parent dataset during parallel operations, and every page latch reports under the one BUFFER class, where contention can mean hot pages or slow I/O. LATEST IS A TIME: this is the newest snapshot found within hours_back of as_of, not an aggregate over those hours - captured_at is the instant it was collected and age_seconds its distance from the window's end. THE PAGE IS BOUNDED BY limit: latches_returned is how many latch classes you got, heaviest last-interval wait first, and truncated says the snapshot held more than limit - a sum over the page is a sum over the page, not over the server. Raise limit when truncated is true. interval_seconds is the seconds the deltas accrued over, and waits_per_second / wait_ms_per_second are the deltas over it - the same keys Darling's twin derives from its latest interval. On a restart / first-sample row (the interval they would have accrued over was unknowable, stored as 0) delta_waiting_requests_count, delta_wait_time_ms, avg_wait_ms_per_request, interval_seconds and both per-second rates are all null - null, never 0, so a restart cannot read as a quiet latch; interval_seconds is also null, with the deltas standing, on a row collected before the interval was stored.")]
    public static async Task<string> GetLatchStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to search for the latest snapshot. Default 24.")] int hours_back = 24,
        [Description("Maximum latch classes to return, heaviest last-interval wait first. Default 20 (the Latch Stats grid's cap). This is what bounds the page - read truncated to know whether the snapshot held more.")] int limit = LocalDataService.LatchSpinlockGridRowCap,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            /* #3653 (the #3541 A3 class on Lite): the caller's limit + 1 as the fetch, the extra row as the
               observed truncation signal. The reader's LIMIT 20 sat under a count this tool published as the
               snapshot's population. */
            var rows = await dataService.GetLatchStatsSnapshotAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd, limit: limit + 1);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "latch_stats")
                    ?? McpHelpers.Status("unavailable", "No latch statistics available in the requested time range.");

            var truncated = rows.Count > limit;
            var page = truncated ? rows.Take(limit).ToList() : rows;

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #3541 A10: hours_back here is the span SEARCHED for the newest snapshot (its description says
                   so); the snapshot's own clock and its distance from the anchor are what make that honest. */
                captured_at = rows[0].CollectionTime.ToString("o"),
                age_seconds = McpLatestSnapshotStamp.AgeSeconds(rows[0].CollectionTime, windowEnd),
                /* #3653: the page described as a page, on the #3594 names — latch_count read as the snapshot's
                   population and was the cap. */
                latches_returned = page.Count,
                truncated,
                order = "delta_wait_time_ms_desc",
                latches = page.Select(r => new
                {
                    latch_class = r.LatchClass,
                    waiting_requests_count = r.WaitingRequestsCount,
                    wait_time_ms = r.WaitTimeMs,
                    max_wait_time_ms = r.MaxWaitTimeMs,
                    /* #3653 A7 (#3642's rule reaching this twin through the shared row): both deltas are null on
                       the restart / first-sample row — the stored interval is the calculator's 0 marker and the
                       zeros beside it were never measured — so a caller cannot read a restart as a quiet latch. */
                    delta_waiting_requests_count = r.DeltaWaitingRequestsCount,
                    delta_wait_time_ms = r.DeltaWaitTimeMs,
                    avg_wait_ms_per_request = r.DeltaWaitingRequestsCount is long requests && requests > 0 && r.DeltaWaitTimeMs is long waitMs
                        ? Math.Round((double)waitMs / requests, 2)
                        : (double?)null,
                    /* #3653 A16: Darling's per-second pair and the interval they divide by, so a null delta above
                       has its why beside it and the two SKUs spell the unknowable row with one key set. */
                    waits_per_second = PerSecond(r.DeltaWaitingRequestsCount, r.SampleIntervalSeconds),
                    wait_ms_per_second = PerSecond(r.DeltaWaitTimeMs, r.SampleIntervalSeconds),
                    interval_seconds = KnownInterval(r.SampleIntervalSeconds)
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_latch_stats", ex);
        }
    }

    [McpServerTool(Name = "get_spinlock_stats"), Description("Gets spinlock contention: collisions, spins, backoffs, per-second rates. High contention is CPU-bound, not in wait stats. Darling sums every collection in hours_back, top N by total collisions. Lite: LATEST IS A TIME, only the newest snapshot within hours_back, cumulative counters plus last-interval deltas, bounded by limit (truncated flags more). interval_seconds and the rates reflect the latest interval, never a window total; null - never 0 - when unknowable (restart/first sample); totals/counters still stand. No rows: unavailable (or not_collected first). <<GUIDE>> Gets the latest spinlock-contention snapshot: cumulative collisions, spins, backoffs and spins-per-collision plus the last collection interval's delta collisions/spins. High spinlock contention is CPU-bound internal contention that does not appear in wait stats. LATEST IS A TIME: this is the newest snapshot found within hours_back of as_of, not an aggregate over those hours - captured_at is the instant it was collected and age_seconds its distance from the window's end. THE PAGE IS BOUNDED BY limit: spinlocks_returned is how many spinlocks you got, most last-interval collisions first, and truncated says the snapshot held more than limit - sys.dm_os_spinlock_stats carries well over a hundred, so at the default the page is the hot tail, not the population. Raise limit when truncated is true. interval_seconds is the seconds the deltas accrued over, and collisions_per_second / spins_per_second are the deltas over it - the same keys Darling's twin derives from its latest interval. On a restart / first-sample row (the interval they would have accrued over was unknowable, stored as 0) delta_collisions, delta_spins, interval_seconds and both per-second rates are all null - null, never 0; interval_seconds is also null, with the deltas standing, on a row collected before the interval was stored.")]
    public static async Task<string> GetSpinlockStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to search for the latest snapshot. Default 24.")] int hours_back = 24,
        [Description("Maximum spinlocks to return, most last-interval collisions first. Default 20 (the Spinlock Stats grid's cap). This is what bounds the page - read truncated to know whether the snapshot held more.")] int limit = LocalDataService.LatchSpinlockGridRowCap,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            /* #3653: limit + 1 fetched, the extra row read as truncation — see get_latch_stats. */
            var rows = await dataService.GetSpinlockStatsSnapshotAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd, limit: limit + 1);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "spinlock_stats")
                    ?? McpHelpers.Status("unavailable", "No spinlock statistics available in the requested time range.");

            var truncated = rows.Count > limit;
            var page = truncated ? rows.Take(limit).ToList() : rows;

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                captured_at = rows[0].CollectionTime.ToString("o"),
                age_seconds = McpLatestSnapshotStamp.AgeSeconds(rows[0].CollectionTime, windowEnd),
                spinlocks_returned = page.Count,
                truncated,
                order = "delta_collisions_desc",
                spinlocks = page.Select(r => new
                {
                    spinlock_name = r.SpinlockName,
                    collisions = r.Collisions,
                    spins = r.Spins,
                    spins_per_collision = Math.Round(r.SpinsPerCollision, 1),
                    sleep_time = r.SleepTime,
                    backoffs = r.Backoffs,
                    delta_collisions = r.DeltaCollisions,
                    delta_spins = r.DeltaSpins,
                    /* #3653 A16: see get_latch_stats — Darling's per-second pair and their denominator. */
                    collisions_per_second = PerSecond(r.DeltaCollisions, r.SampleIntervalSeconds),
                    spins_per_second = PerSecond(r.DeltaSpins, r.SampleIntervalSeconds),
                    interval_seconds = KnownInterval(r.SampleIntervalSeconds)
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_spinlock_stats", ex);
        }
    }
}
