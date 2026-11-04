namespace PerformanceMonitor.Exporter;

/// <summary>
/// `--query-store-source raw|wide|compare`: where the Query Store window is read from.
/// </summary>
public enum QueryStoreSourceMode
{
    /// <summary>Today's behaviour exactly: the raw `collect.query_store_stats` read, deduped in DuckDB. No extra store query.</summary>
    Raw,

    /// <summary>Read Darling's deduplicated `collect.query_store_interval_wide` when the gate allows it; otherwise the raw read, with the reason recorded.</summary>
    Wide,

    /// <summary>Do both reads, compare them in DuckDB, and write the output from the RAW read. Records the comparison counts.</summary>
    Compare,
}

public static class QueryStoreSourceModes
{
    public static readonly string[] Names = ["raw", "wide", "compare"];

    public static bool TryParse(string? text, out QueryStoreSourceMode mode)
    {
        switch (text)
        {
            case "raw": mode = QueryStoreSourceMode.Raw; return true;
            case "wide": mode = QueryStoreSourceMode.Wide; return true;
            case "compare": mode = QueryStoreSourceMode.Compare; return true;
            default: mode = QueryStoreSourceMode.Raw; return false;
        }
    }

    public static string Name(QueryStoreSourceMode mode) => mode switch
    {
        QueryStoreSourceMode.Raw => "raw",
        QueryStoreSourceMode.Wide => "wide",
        _ => "compare",
    };
}

/// <summary>Why the wide read was not used. The names are the manifest's `query_store_fallback_reason` values.</summary>
public static class QueryStoreFallbackReasons
{
    public const string TableMissing = "table_missing";
    public const string WindowTooOld = "window_too_old";

    /// <summary>`--from`/`--to` is not on a whole hour. Darling's table is selected by `first_execution_time`, which equals the window only for whole-hour bounds.</summary>
    public const string WindowNotHourAligned = "window_not_hour_aligned";
    public const string NoCoverage = "no_coverage";
    public const string FilledLate = "filled_late";
    public const string AppliedBehind = "applied_behind";
    public const string Pending = "pending";
    public const string IntervalMisaligned = "interval_misaligned";
    public const string ServerUnmapped = "server_unmapped";

    /// <summary>A gate query failed. Conservative: read raw.</summary>
    public const string GateError = "gate_error";

    /// <summary>The wide read itself failed. Conservative: read raw.</summary>
    public const string WideReadError = "wide_read_error";
}

/// <summary>One row of `collect.query_store_interval_wide_coverage`.</summary>
public sealed record WideCoverageRow(int ServerId, DateTime FilledSince, DateTime AppliedThrough);

/// <summary>The gate's answer. When <see cref="UseWide"/> is false, <see cref="Reason"/> is the first failed clause.</summary>
public sealed record WideGateDecision(bool UseWide, string? Reason, int? ServerId, string? Detail = null)
{
    public static readonly WideGateDecision Allow = new(true, null, null);
    public static WideGateDecision Refuse(string reason, int? serverId = null, string? detail = null) => new(false, reason, serverId, detail);
}

/// <summary>
/// The decision whether Darling's `query_store_interval_wide` may stand in for the raw read of one window. Pure: every
/// input is a parameter (the clock too), so each clause is unit-tested on its own. The gate only reads the three small
/// tables; no GROUP BY, DISTINCT, sort or anti-join runs on the store.
/// </summary>
public static class QueryStoreWideGate
{
    /// <summary>A window older than this is never read from the wide table: it keeps us off the purge edge, the legacy partition and old unbuilt hours.</summary>
    public static readonly TimeSpan MaxWindowAge = TimeSpan.FromDays(8);

    /// <summary>Query Store intervals are 60 minutes long (the gate already refuses windows that are not whole hours).</summary>
    public static readonly TimeSpan QueryStoreIntervalLength = TimeSpan.FromMinutes(60);

    /// <summary>
    /// The latest snapshot of an interval lands within 74 minutes of its start (measured worst: 1 h 11 min); 90
    /// minutes is the margin `applied_through` must clear past the START of the window's LAST interval, i.e. `to - 60
    /// min + 90 min` = `to + 30 min`. Measuring from `to` instead (to + 90 min) can never pass for a lag-2 read at to
    /// + 1 h 20 min. The scheduler's due times (`Planner`) use the same lag settings; QueryStoreWideGateTests
    /// cross-checks them.
    /// </summary>
    public static readonly TimeSpan AppliedMargin = TimeSpan.FromMinutes(90);

    /// <summary>The pending read is capped at this many rows; a result this long may hide a server, so it counts as pending.</summary>
    public const int PendingRowCap = 1000;

    /// <summary>True when the window starts more than 8 days before <paramref name="nowUtc"/>. Checked before any store read.</summary>
    public static bool IsWindowTooOld(DateTime fromUtc, DateTime nowUtc) => fromUtc < nowUtc - MaxWindowAge;

    /// <summary>
    /// The clauses that need no store read: the window is on whole hours, and not older than 8 days. Null when both pass.
    /// The loader runs this before it sends the gate's first query.
    /// </summary>
    public static WideGateDecision? PreCheck(DateTime fromUtc, DateTime toUtc, DateTime nowUtc)
    {
        if (!IsHourAligned(fromUtc, toUtc))
        {
            return WideGateDecision.Refuse(QueryStoreFallbackReasons.WindowNotHourAligned);
        }

        return IsWindowTooOld(fromUtc, nowUtc) ? WideGateDecision.Refuse(QueryStoreFallbackReasons.WindowTooOld) : null;
    }

    private static string FormatMinute(DateTime utc) => utc.ToString("yyyy-MM-dd'T'HH:mm'Z'", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>True when both bounds are whole UTC hours.</summary>
    public static bool IsHourAligned(DateTime fromUtc, DateTime toUtc) =>
        fromUtc.Ticks % TimeSpan.TicksPerHour == 0 && toUtc.Ticks % TimeSpan.TicksPerHour == 0;

    /// <summary>The servers the gate checks: every coverage row whose `applied_through` reaches the window start.</summary>
    public static SortedSet<int> ServersToCheck(IReadOnlyList<WideCoverageRow> coverage, DateTime fromUtc) =>
        [.. coverage.Where(c => c.AppliedThrough >= fromUtc).Select(c => c.ServerId)];

    /// <summary>
    /// The `applied_behind` clause needs `applied_through >= toUtc - QueryStoreIntervalLength + AppliedMargin`: 90 minutes past
    /// the start of the window's last interval (to + 30 min), for a one-hour window and a multi-hour catch-up window alike.
    /// Lag 3 (read at to + 2 h 20) and lag 2 (to + 1 h 20) pass when Darling is under 1 h 50 / 50 min behind; lag 1
    /// (read at to + 50 min) passes only when Darling is less than 20 minutes behind, else it falls back to raw.
    /// Clause order: window_not_hour_aligned, window_too_old, table_missing, then per server (ascending server_id) no_coverage, filled_late,
    /// applied_behind, pending. The first failure refuses the whole window. Servers checked: every coverage row whose
    /// `applied_through` reaches the window start (Darling creates a server's coverage row before its first raw COPY, so
    /// every server that writes raw has one; a removed server with data in the window still counts, and one whose data
    /// ended before the window does not). No server to check at all refuses as no_coverage: nothing vouches for the
    /// window. The monitored-server registry is deliberately not read: the export login is denied that table.
    /// </summary>
    public static WideGateDecision Evaluate(
        DateTime fromUtc, DateTime toUtc, DateTime nowUtc, bool tablesExist,
        IReadOnlyList<WideCoverageRow> coverage, IReadOnlyCollection<int> pendingServerIds)
    {
        var early = PreCheck(fromUtc, toUtc, nowUtc);
        if (early is not null)
        {
            return early;
        }

        if (!tablesExist)
        {
            return WideGateDecision.Refuse(QueryStoreFallbackReasons.TableMissing);
        }

        var coverageByServer = new Dictionary<int, WideCoverageRow>();
        foreach (var row in coverage)
        {
            coverageByServer[row.ServerId] = row;
        }

        var toCheck = ServersToCheck(coverage, fromUtc);

        if (toCheck.Count == 0)
        {
            return WideGateDecision.Refuse(QueryStoreFallbackReasons.NoCoverage);
        }

        var pendingTruncated = pendingServerIds.Count >= PendingRowCap;
        var pending = new HashSet<int>(pendingServerIds);
        var needAppliedThrough = toUtc - QueryStoreIntervalLength + AppliedMargin;

        foreach (var serverId in toCheck)
        {
            var row = coverageByServer[serverId];

            if (row.FilledSince > fromUtc)
            {
                return WideGateDecision.Refuse(QueryStoreFallbackReasons.FilledLate, serverId);
            }

            if (row.AppliedThrough < needAppliedThrough)
            {
                return WideGateDecision.Refuse(
                    QueryStoreFallbackReasons.AppliedBehind, serverId,
                    $"server {serverId} applied_through {FormatMinute(row.AppliedThrough)} < needed {FormatMinute(needAppliedThrough)}");
            }

            if (pending.Contains(serverId))
            {
                return WideGateDecision.Refuse(QueryStoreFallbackReasons.Pending, serverId);
            }
        }

        return pendingTruncated ? WideGateDecision.Refuse(QueryStoreFallbackReasons.Pending) : WideGateDecision.Allow;
    }
}

/// <summary>
/// What one Query Store window build did, filled in by <see cref="QueryStoreWindow"/> and read by <see
/// cref="QueryStoreStatsExporter"/> for the manifest. Null on the cache in raw mode.
/// </summary>
public sealed class QueryStoreSourceReport
{
    public required QueryStoreSourceMode Mode { get; init; }

    /// <summary>`wide` or `raw`: what the output rows came from. Compare mode is always `raw`.</summary>
    public string Source { get; set; } = "raw";

    public string? FallbackReason { get; set; }
    public int? FallbackServerId { get; set; }

    /// <summary>Why the gate refused, in numbers (applied_behind: the server id, its applied_through and the needed time). Null otherwise.</summary>
    public string? FallbackDetail { get; set; }
    public double? WideReadMs { get; set; }
    public double? RawReadMs { get; set; }
    public long? CompareOnlyRaw { get; set; }
    public long? CompareOnlyWide { get; set; }
    public long? CompareDiffRows { get; set; }

    /// <summary>The first differing keys (at most 5), as printed to stdout. Never written to the manifest.</summary>
    public List<string> CompareSampleKeys { get; } = [];

    /// <summary>The manifest block for this report.</summary>
    public QueryStoreSourceStats ToStats() => new(
        QueryStoreSourceModes.Name(Mode), Source, FallbackReason, FallbackServerId, WideReadMs, RawReadMs,
        CompareOnlyRaw, CompareOnlyWide, CompareDiffRows, FallbackDetail);
}

/// <summary>The `query_store_stats` manifest entry's source fields.</summary>
public sealed record QueryStoreSourceStats(
    string Mode, string Source, string? FallbackReason, int? FallbackServerId, double? WideReadMs, double? RawReadMs,
    long? CompareOnlyRaw, long? CompareOnlyWide, long? CompareDiffRows, string? FallbackDetail = null);
