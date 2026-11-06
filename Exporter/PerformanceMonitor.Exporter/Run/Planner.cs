using System.Globalization;

namespace PerformanceMonitor.Exporter.Run;

/// <summary>One export window. A gap is recorded, never exported.</summary>
public sealed record Window(DateTime From, DateTime To, bool IsGap);

/// <summary>The family modes resolved from the settings and the optional-table list (R10-R13).</summary>
public sealed record FamilyMode(
    bool UseFast, string FastTables, string FreshTables, bool SkipHourlyFresh, string AlsoCsv,
    bool HasQueryStore, string QueryStoreTables,
    int LateMargin, string LateSource, int QueryStoreLag, string QueryStoreLagSource);

public sealed record FamilyPlan(string Name, string Tables, string? DimensionOrigin, string? Also, string Status, IReadOnlyList<Window> Windows);

/// <summary>The arguments that differ per child call; `Window` is the exported range.</summary>
public sealed record PlannedCall(string Family, DateTime From, DateTime To, string Tables, string? DimensionOrigin, string? Also, int? QueryStoreLagHours, int? FreshGuardMinutes)
{
    public string FromArg => From.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
    public string ToArg => To.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
}

/// <summary>
/// The scheduler's pure functions (R1-R13). Nothing here reads the clock, a file, the store or the network; the
/// caller passes every instant in.
/// </summary>
public static class Planner
{
    public const string Fresh = "fresh", FreshFast = "fresh_fast", QueryStore = "query_store";
    public const int DefaultLateMargin = 15, DefaultQueryStoreLag = 3;

    /// <summary>The fresh family's default tables: every table in <see cref="ExportOptions.AllTables"/> except query_store_stats, in that order.</summary>
    public static IReadOnlyList<string> DefaultFreshTables { get; } =
        ExportOptions.AllTables.Where(t => t != QueryStoreTables.QueryStoreStats).ToArray();

    public static IReadOnlyList<string> QueryStoreTableList { get; } = [QueryStoreTables.QueryStoreStats, DimensionTables.PlanShape, DimensionTables.QueryText];

    // ---- R1, R2, R3: hourly windows ----

    /// <summary>
    /// The whole-UTC-hour windows due for one family, oldest first. Fresh: [h, h+1) is due at h+1h15m. Query Store: due at
    /// h+lag+15m, and lag 1 uses a 45-minute margin (h+1h45m). Fresh ignores the lag. The hours whose end is older than
    /// now - maxAgeHours are ONE gap entry, returned first, which does not count toward <paramref name="maxWindows"/>;
    /// at most <paramref name="maxWindows"/> exportable windows follow it. The cap covers only hours a run can still
    /// export, so a run after an outage never spends its slots recording gaps while recoverable hours age out.
    /// </summary>
    public static IReadOnlyList<Window> DueWindows(string family, DateTime? lastTo, DateTime now, int maxWindows = 6, int maxAgeHours = 20, int lagHours = 3)
    {
        var lag = family == Fresh ? 1 : lagHours;
        var margin = family != Fresh && lag == 1 ? 45 : 15;
        var edge = now.AddHours(-lag).AddMinutes(-margin);
        var newestDueTo = edge.Date.AddHours(edge.Hour + 1);
        var startTo = lastTo is { } lt ? lt.AddHours(1) : newestDueTo.AddHours(-(maxAgeHours - 1));
        var oldestAllowedTo = now.AddHours(-maxAgeHours);
        var windows = new List<Window>();

        var to = startTo;
        DateTime? gapTo = null;
        for (; to <= newestDueTo && to < oldestAllowedTo; to = to.AddHours(1))
        {
            gapTo = to;
        }
        if (gapTo is { } g)
        {
            windows.Add(new Window(startTo.AddHours(-1), g, true));
        }

        for (var exported = 0; to <= newestDueTo && exported < maxWindows; to = to.AddHours(1), exported++)
        {
            windows.Add(new Window(to.AddHours(-1), to, false));
        }
        return windows;
    }

    // ---- R3, R4: fresh_fast windows ----

    /// <summary>
    /// The fresh_fast family's due calls, oldest first. Boundaries are N-aligned from UTC midnight; a boundary b is due at b+lateMinutes.
    /// One call covers at most 60 minutes and at most <paramref name="maxCalls"/> calls come back. The range older than
    /// now - maxAgeHours is ONE gap entry, returned first, which does not count toward the cap.
    /// </summary>
    public static IReadOnlyList<Window> FastDueWindows(DateTime? lastTo, DateTime now, int windowMinutes, int maxCalls = 6, int maxAgeHours = 20, int lateMinutes = DefaultLateMargin)
    {
        var edge = now.AddMinutes(-lateMinutes);
        var newest = edge.Date.AddMinutes(Math.Floor(edge.TimeOfDay.TotalMinutes / windowMinutes) * windowMinutes);
        var oldest = now.AddHours(-maxAgeHours);
        var start = lastTo ?? oldest.Date.AddMinutes(Math.Ceiling(oldest.TimeOfDay.TotalMinutes / windowMinutes) * windowMinutes);
        var output = new List<Window>();
        if (start >= newest) return output;

        if (start < oldest)
        {
            var gapTo = oldest.Date.AddMinutes(Math.Floor(oldest.TimeOfDay.TotalMinutes / windowMinutes) * windowMinutes);
            if (gapTo > newest) gapTo = newest;
            if (gapTo > start)
            {
                output.Add(new Window(start, gapTo, true));
                start = gapTo;
            }
        }

        var calls = 0;
        for (var from = start; from < newest && calls < maxCalls;)
        {
            var to = from.AddMinutes(60);
            if (to > newest) to = newest;
            output.Add(new Window(from, to, false));
            calls++;
            from = to;
        }
        return output;
    }

    // ---- R5, R6, R13: config text parsers ----

    /// <summary>The first non-blank line, trimmed, as an integer 2..15; anything else is the default 15.</summary>
    public static int ParseLateRowMargin(string? text)
    {
        var line = FirstNonBlankLine(text);
        if (line is null || line.Length is < 1 or > 3 || !line.All(char.IsAsciiDigit)) return DefaultLateMargin;
        var n = int.Parse(line, CultureInfo.InvariantCulture);
        return n is < 2 or > 15 ? DefaultLateMargin : n;
    }

    /// <summary>The first non-blank line, trimmed, exactly 1, 2 or 3; anything else is the default 3.</summary>
    public static int ParseQueryStoreLag(string? text) =>
        FirstNonBlankLine(text) is { } v && (v is "1" or "2" or "3") ? int.Parse(v, CultureInfo.InvariantCulture) : DefaultQueryStoreLag;

    /// <summary>One table per line; `#` starts a comment; blank lines and surrounding whitespace are ignored.</summary>
    public static List<string> ParseOptionalTables(IEnumerable<string> lines)
    {
        var names = new List<string>();
        foreach (var raw in lines)
        {
            var l = raw;
            var hash = l.IndexOf('#');
            if (hash >= 0) l = l[..hash];
            l = l.Trim();
            if (l.Length > 0) names.Add(l);
        }
        return names;
    }

    private static string? FirstNonBlankLine(string? text)
    {
        if (text is null) return null;
        foreach (var l in text.Split('\n'))
        {
            var t = l.Trim();
            if (t.Length > 0) return t;
        }
        return null;
    }

    // ---- R10, R11, R12: table sets and modes ----

    public static IReadOnlyList<string> FreshTableList(RunSettings s) => s.Families.FreshTables ?? DefaultFreshTables;

    public static FamilyMode ResolveMode(RunSettings s, IReadOnlyList<string> optionalTables)
    {
        var fresh = FreshTableList(s);
        var also = string.Join(',', optionalTables);
        var freshCsv = string.Join(',', fresh);
        var useFast = s.Families.FastWindowMinutes < 60;
        var fastCsv = "";
        var skipHourly = false;
        if (useFast)
        {
            var fast = fresh.Where(t => SubHourTables.All.Contains(t)).ToArray();
            var hourly = fresh.Where(t => !SubHourTables.All.Contains(t)).ToArray();
            fastCsv = string.Join(',', fast);
            if (fast.Length == 0) useFast = false;
            else if (hourly.Length > 0) freshCsv = string.Join(',', hourly);
            else if (also.Length == 0) skipHourly = true;
            // else: nothing hourly left but --also is set. --also needs a core table list to ride on, so the fresh call keeps the full list.
        }

        var lag = s.Families.QueryStoreLagHours ?? DefaultQueryStoreLag;
        var margin = s.Families.LateRowMarginMinutes ?? DefaultLateMargin;
        return new FamilyMode(useFast, fastCsv, freshCsv, skipHourly, also,
            s.Families.QueryStoreEnabled, s.Families.QueryStoreEnabled ? string.Join(',', QueryStoreTableList) : "",
            margin, s.Families.LateRowMarginMinutes is null ? "default" : "config",
            lag, s.Families.QueryStoreLagHours is null ? "default" : "config");
    }

    /// <summary>The families to consider, in run order: fresh_fast, then fresh, then query_store (R9).</summary>
    public static IReadOnlyList<string> FamilyOrder(FamilyMode m)
    {
        var order = new List<string>();
        if (m.UseFast) order.Add(FreshFast);
        if (!m.SkipHourlyFresh) order.Add(Fresh);
        if (m.HasQueryStore) order.Add(QueryStore);
        return order;
    }

    /// <summary>Query Store at lag 1 may start in the minutes the hourly rules reserve (:00-:19, and after the cutoff).</summary>
    public static bool IsQueryStoreEarly(string family, FamilyMode m) => family == QueryStore && m.QueryStoreLag == 1;

    /// <summary>One family's windows and status for this instant. Never applies the clock-minute cutoff (that is read as the run goes on).</summary>
    public static FamilyPlan PlanFamily(RunSettings s, FamilyMode m, string family, DateTime? lastTo, DateTime now)
    {
        var tables = family switch { FreshFast => m.FastTables, Fresh => m.FreshTables, _ => m.QueryStoreTables };
        var origin = family switch { Fresh => "query_stats", QueryStore => "query_store", _ => null };
        var also = family == Fresh && m.AlsoCsv.Length > 0 ? m.AlsoCsv : null;
        IReadOnlyList<Window> windows;
        string status;
        if (family == FreshFast)
        {
            windows = FastDueWindows(lastTo, now, s.Families.FastWindowMinutes, s.Schedule.MaxWindowsPerFamily, s.Schedule.MaxWindowAgeHours, m.LateMargin);
            status = windows.Count == 0 ? "nothing_due" : "due";
        }
        else if (m.UseFast && now.Minute < 20 && !IsQueryStoreEarly(family, m))
        {
            windows = [];
            status = "waits_for_minute_20";
        }
        else
        {
            windows = DueWindows(family, lastTo, now, s.Schedule.MaxWindowsPerFamily, s.Schedule.MaxWindowAgeHours, m.QueryStoreLag);
            status = windows.Count == 0 ? "nothing_due" : "due";
        }
        return new FamilyPlan(family, tables, origin, also, status, windows);
    }

    /// <summary>The per-call arguments for one window of a family (R5, R6, R10).</summary>
    public static PlannedCall CallFor(FamilyMode m, FamilyPlan plan, Window w) => new(
        plan.Name, w.From, w.To, plan.Tables, plan.DimensionOrigin, plan.Also,
        plan.Name == QueryStore && m.QueryStoreLag != 3 ? m.QueryStoreLag : null,
        plan.Name == FreshFast && m.LateMargin != DefaultLateMargin ? m.LateMargin : null);

    /// <summary>
    /// The child's full argument list (R20): the base arguments, then the per-call flags in a fixed order. No empty elements.
    /// </summary>
    public static List<string> BuildExportArgs(RunSettings s, PlannedCall c)
    {
        var a = new List<string>(s.ChildBaseArgs);
        a.AddRange(["--store", s.Store, "--from", c.FromArg, "--to", c.ToArg, "--out", s.OutDir,
            "--statement-timeout-seconds", s.StatementTimeoutSeconds.ToString(CultureInfo.InvariantCulture),
            "--duckdb-memory-limit", s.DuckdbMemoryLimit]);
        if (s.DuckdbThreads is { } threads) a.AddRange(["--duckdb-threads", threads.ToString(CultureInfo.InvariantCulture)]);
        if (!string.IsNullOrEmpty(s.Upload)) a.AddRange(["--upload", s.Upload]);
        // The order is fixed: --aws-region, --state-dir, then --application-name, right after --upload.
        if (!string.IsNullOrEmpty(s.AwsRegion)) a.AddRange(["--aws-region", s.AwsRegion]);
        if (!string.IsNullOrEmpty(s.ExportStateDir)) a.AddRange(["--state-dir", s.ExportStateDir]);
        if (!string.IsNullOrEmpty(s.ApplicationName)) a.AddRange(["--application-name", s.ApplicationName]);
        if (!string.IsNullOrEmpty(c.Tables)) a.AddRange(["--tables", c.Tables]);
        if (!string.IsNullOrEmpty(c.DimensionOrigin)) a.AddRange(["--dimension-origin", c.DimensionOrigin]);
        if (!string.IsNullOrEmpty(c.Also)) a.AddRange(["--also", c.Also]);
        if (c.QueryStoreLagHours is >= 1 and <= 3) a.AddRange(["--query-store-lag-hours", c.QueryStoreLagHours.Value.ToString(CultureInfo.InvariantCulture)]);
        if (c.FreshGuardMinutes is >= 2 and <= 15) a.AddRange(["--fresh-guard-minutes", c.FreshGuardMinutes.Value.ToString(CultureInfo.InvariantCulture)]);
        if (!string.IsNullOrEmpty(s.PgUser)) a.AddRange(["--pg-user", s.PgUser, "--pg-credential-file", s.PgCredentialFile!]);
        if (!string.IsNullOrEmpty(s.ProcedurePlanReuse)) a.AddRange(["--procedure-plan-reuse", s.ProcedurePlanReuse]);
        if (!string.IsNullOrEmpty(s.QueryStoreSource)) a.AddRange(["--query-store-source", s.QueryStoreSource]);
        a.RemoveAll(string.IsNullOrEmpty);
        return a;
    }
}
