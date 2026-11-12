namespace PerformanceMonitor.Exporter;

/// <summary>
/// The set of raw tables that the exporter can pull from the `collect` schema.
/// Every one of them is filtered on `collection_time` and has `server_id` dropped
/// on the way out, because `server_id` is local to one store.
/// </summary>
public static class RawTables
{
    public static readonly string[] All =
    [
        "perfmon_stats",
        "wait_stats",
        "file_io_stats",
        "memory_stats",
        "cpu_utilization_stats",
        "spinlock_stats",
        "latch_stats",
        "tempdb_stats",
        "deadlocks",
        "blocked_process_reports",
        "collection_log",
    ];
}

/// <summary>
/// The two hourly rollups the exporter builds itself from raw query_stats / procedure_stats, because Darling's own
/// hourly continuous aggregates drop plan hash, I/O, rows and spills.
/// </summary>
public static class HourlyTables
{
    public const string QueryStatsHourly = "query_stats_hourly";
    public const string ProcedureStatsHourly = "procedure_stats_hourly";

    public static readonly string[] All = [QueryStatsHourly, ProcedureStatsHourly];
}

/// <summary>
/// The window table for Query Store: one row per (server, database, query, plan, interval,
/// execution type, replica role), the last (highest collection_time) snapshot of each
/// Query Store runtime-stats interval. Filtered on its interval start time, not its
/// collection time, so the window selects a set of intervals rather than a set of
/// snapshots.
/// </summary>
public static class QueryStoreTables
{
    public const string QueryStoreStats = "query_store_stats";

    public static readonly string[] All = [QueryStoreStats];
}

/// <summary>
/// The two dimension tables the exporter builds from query_stats and Query Store keys plus
/// the plan/text dimension tables (see DimensionTableExporter): plan_shape is keyed by
/// (server_name, query_plan_hash) because showplan XML differs on every compile (one day on
/// one store had 302,106 distinct plan digests but only 4,247 distinct plan hashes), and
/// query_text is keyed by (server_name, database_name, query_hash). Each row carries an
/// `origin` of `query_stats` or `query_store`; when both sources have the same key, the
/// query_stats row wins.
/// </summary>
public static class DimensionTables
{
    public const string PlanShape = "plan_shape";
    public const string QueryText = "query_text";
    public const string ProcedurePlan = "procedure_plan";

    public static readonly string[] All = [PlanShape, QueryText, ProcedurePlan];
}

/// <summary>
/// The three companion tables `plan_shape` writes alongside itself: one row per (source_store, plan_digest,
/// statement_ordinal, feature_version) parsed from the plan XML, plus the two child tables keyed the same way plus a
/// sequence number. Not selectable with `--tables` and not in `AllTables` — they are produced by the plan_shape
/// export, never on their own.
/// </summary>
public static class PlanFeatureTables
{
    public const string PlanFeatures = "plan_features";
    public const string PlanWarnings = "plan_warnings";
    public const string PlanMissingIndexes = "plan_missing_indexes";

    public static readonly string[] All = [PlanFeatures, PlanWarnings, PlanMissingIndexes];
}

/// <summary>
/// The nine system_health window tables, all filled by one pass over
/// collect.system_health_events (see SystemHealthExporter): each output row keys on the
/// same (source_store, server_name, collection_time, event_time) prefix, then carries one
/// event category's shredded fields. Selecting any of these via --tables runs the single
/// pass for the requested tables only; the tables not requested are skipped entirely, not
/// written empty.
/// </summary>
public static class SystemHealthTables
{
    public const string Cpu = "system_health_cpu";
    public const string System = "system_health_system";
    public const string Memory = "system_health_memory";
    public const string QueryProcessing = "system_health_query_processing";
    public const string Io = "system_health_io";
    public const string IoPending = "system_health_io_pending";
    public const string Errors = "system_health_errors";
    public const string Waits = "system_health_waits";
    public const string MemoryBroker = "system_health_memory_broker";
    public const string MemoryNodeOom = "system_health_memory_node_oom";

    public static readonly string[] All =
    [
        Cpu, System, Memory, QueryProcessing, Io, IoPending, Errors, Waits, MemoryBroker, MemoryNodeOom,
    ];
}

/// <summary>
/// The tables that may be exported over a window that is not a whole UTC hour (--from or --to off the hour,
/// on a whole UTC minute). The one place this list lives; <see cref="ExportOptions.Validate"/> refuses a
/// sub-hour window for any table not in it.
///
/// IN, because the store read is a plain `collection_time >= @from AND collection_time &lt; @to` range with no
/// hour bucketing, no baseline and no snapshot window, so any [from, to) slice is exact and two adjacent
/// slices never overlap or leave a gap:
///   * <see cref="RawTables.All"/> -- all share <c>SqlBuilder.BuildRawSelect</c> (one generic range SELECT).
///   * <see cref="SystemHealthTables.All"/> -- one pass over collect.system_health_events with the same
///     range predicate (<c>SystemHealthExporter.WindowSql</c>).
///   The compressed-chunk guard (<c>StoreCapabilities.CompressedChunksInWindowAsync</c>) tests chunk-range
///   overlap with [from, to), so it is correct for any range; the straggler guard compares open transactions
///   with the window end, which is also any instant.
///
/// OUT (whole-hour windows only), everything else:
///   * <see cref="HourlyTables.All"/> -- rolled up with date_trunc('hour', collection_time), so a partial hour
///     would yield a partial bucket that a later slice cannot complete.
///   * <see cref="QueryStoreTables.All"/> -- selects whole Query Store intervals by interval start, with a
///     to + 2h completeness margin.
///   * <see cref="DimensionTables.All"/> (and the plan_features companions) -- derived from the hourly
///     query_stats / Query Store windows and keyed on the dimension state store.
///   * every optional table (--tables or --also) -- change-only tables diff against a UTC-midnight baseline
///     (ChangeOnlyExporter.IsUtcMidnightWindow), daily snapshots use the midnight window
///     (OptionalTableExporter.IsDailySnapshotWindow), and hourly-rollup grains bucket by hour.
/// </summary>
public static class SubHourTables
{
    public static readonly string[] All = [.. RawTables.All, .. SystemHealthTables.All];

    /// <summary>
    /// True when <paramref name="fromUtc"/> or <paramref name="toUtc"/> is not on a whole UTC hour (the window
    /// is "sub-hour": it may start or end inside an hour, and may span several hours).
    /// </summary>
    public static bool IsSubHourWindow(DateTime fromUtc, DateTime toUtc) =>
        !IsWholeHour(fromUtc) || !IsWholeHour(toUtc);

    /// <summary>
    /// The first of <paramref name="tables"/> that cannot be exported over a sub-hour window, or null when all
    /// of them can.
    /// </summary>
    public static string? FirstHourlyOnlyTable(IEnumerable<string> tables) =>
        tables.FirstOrDefault(t => !All.Contains(t));

    private static bool IsWholeHour(DateTime utc) => utc.Minute == 0 && utc.Second == 0 && utc.Millisecond == 0;
}

/// <summary>Parsed and validated command-line options for the `export` command.</summary>
public sealed class ExportOptions
{
    public required string Store { get; init; }
    public required DateTime FromUtc { get; init; }
    public required DateTime ToUtc { get; init; }
    public required string OutDir { get; init; }
    public required string[] Tables { get; init; }

    /// <summary>
    /// Optional-table names requested via `--also`. Distinct from <see cref="Tables"/>: this list accepts optional
    /// names only, so `--also` cannot be used to change the core `--tables` set. An unknown or absent name is not
    /// validated here -- that happens at run time against the registry (unknown) and the store (absent), because
    /// "absent" needs a live connection.
    /// </summary>
    public string[] Also { get; init; } = [];

    public string PgHost { get; init; } = "127.0.0.1";
    public int PgPort { get; init; } = 5641;
    public string PgDatabase { get; init; } = "darling";
    public string PgUser { get; init; } = "darling";
    public string? PgCredentialFile { get; init; }

    public int StatementTimeoutSeconds { get; init; } = 600;
    public bool DryRun { get; init; }
    public bool AllowOpenWindow { get; init; }

    /// <summary>
    /// Manual catch-up escape hatch. Turns off the compressed-chunk guard's refusal and doubles <see
    /// cref="ContentFetchPacer"/>'s sleeps. The scheduler never passes this -- it is for a human running a catch-up
    /// export by hand.
    /// </summary>
    public bool AllowCompressed { get; init; }
    public string? Upload { get; init; }
    /// <summary>The S3 region. Null means the AWS SDK's default resolution (environment, profile).</summary>
    public string? AwsRegion { get; init; }

    /// <summary>The folder for the exporter's state files. Required: `export` rejects a call that has none.</summary>
    public string StateDir { get; init; } = "";
    public bool IgnoreState { get; init; }

    /// <summary>
    /// Fetch `pg_statement_text` for every key, bypassing the threshold gate (calls &gt;= 2 or total exec time
    /// &gt;= 100 ms). Default false: the gate is on.
    /// </summary>
    public bool PgTextFetchAll { get; init; }

    /// <summary>
    /// `--plan-fetch-all`: turn the plan-fetch gate off, so every new plan key has its XML fetched whatever its
    /// execution count or CPU. Default false: the gate is on.
    /// </summary>
    public bool PlanFetchAll { get; init; }

    /// <summary>
    /// `--procedure-plan-reuse off|shadow|on`: whether `procedure_plan` skips plan XML for a digest whose cached plan
    /// was fetched less than 6 hours ago. Default shadow: decide and count, fetch everything. `--ignore-state` behaves
    /// like off.
    /// </summary>
    public ProcedurePlanReuseMode ProcedurePlanReuse { get; init; } = ProcedurePlanReuseMode.Shadow;

    /// <summary>
    /// `--query-store-source raw|wide|compare`: where the Query Store window is read from. Default raw: today's read,
    /// no extra store query.
    /// </summary>
    public QueryStoreSourceMode QueryStoreSource { get; init; } = QueryStoreSourceMode.Raw;

    /// <summary>
    /// The Query Store family's lag setting: 3 (today's default), 2 or 1 (--query-store-lag-hours, or `families.query_store.lag_hours` in a config). Lag 3 and 2 make a window due 3 h / 2 h after its
    /// start plus a 15-minute margin; lag 1 makes it due 1 h after its start plus a 45-minute margin. It sets the
    /// open-window guard margin (<see cref="QueryStoreGuardMargin"/>) and the upper `collection_time` bound of the Query
    /// Store read (<see cref="QueryStoreCollectionToUtc"/>).
    /// </summary>
    public int QueryStoreLagHours { get; init; } = 3;

    /// <summary>The open-window guard margin for the Query Store family: 2h15m (lag 3), 1h15m (lag 2), 45m (lag 1).</summary>
    public static TimeSpan QueryStoreGuardMargin(int lagHours) =>
        lagHours == 1 ? TimeSpan.FromMinutes(45) : TimeSpan.FromMinutes((lagHours - 1) * 60 + 15);

    /// <summary>
    /// The exclusive upper `collection_time` bound for the Query Store read: `--to` + 2 h for every lag. A run at lag 1, 2
    /// or 3 happens at `--to` + 45 min, + 1 h 15 min or + 2 h 15 min at the earliest, so the read covers
    /// `[window start, run time)` (the worst measured snapshot lag is To + 12 min); a bound beyond the run time reads
    /// nothing extra, because those rows do not exist yet.
    /// </summary>
    public DateTime QueryStoreCollectionToUtc => ToUtc.AddHours(2);

    /// <summary>
    /// Which key source(s) `plan_shape` / `query_text` (and their plan_features companions) read: `query_stats`,
    /// `query_store`, or `both` (the default, today's behavior). See DimensionOrigins.
    /// </summary>
    public string DimensionOrigin { get; init; } = DimensionOrigins.Both;

    /// <summary>
    /// The rule for a store name: lower-case, starts with a letter, at most 31 characters. The name becomes a folder, a
    /// manifest key and, in a downstream loader, a SQL identifier, so it stays a plain identifier.
    /// </summary>
    public const string StoreNamePattern = "^[a-z][a-z0-9_]{0,30}$";

    private static readonly System.Text.RegularExpressions.Regex StoreNameRegex = new(StoreNamePattern);

    /// <summary>True when <paramref name="name"/> follows <see cref="StoreNamePattern"/>.</summary>
    public static bool IsValidStoreName([System.Diagnostics.CodeAnalysis.NotNullWhen(true)] string? name) => name is not null && StoreNameRegex.IsMatch(name);

    /// <summary>
    /// The `application_name` of the store session: `--application-name`, or a config's `store.application_name`.
    /// Defaults to <see cref="ConfigLoader.DefaultApplicationName"/>.
    /// </summary>
    public string ApplicationName { get; init; } = ConfigLoader.DefaultApplicationName;

    /// <summary>
    /// Where the store credential comes from when a config file is in use. Null (no config) keeps today's
    /// resolution: the `DARLING_EXPORT_PG_PASSWORD` env var, else the DPAPI file in <see cref="PgCredentialFile"/>.
    /// </summary>
    public CredentialSpec? Credential { get; init; }

    /// <summary>
    /// Default DPAPI-encrypted credential file location on a Darling monitoring box. Darling
    /// itself writes this file; the exporter only reads it.
    /// </summary>
    public const string DefaultCredentialFile = @"C:\ProgramData\PerformanceMonitorDarling\pg-credential.dpapi";

    /// <summary>
    public static readonly string[] AllTables = [.. RawTables.All, .. HourlyTables.All, .. QueryStoreTables.All, .. DimensionTables.All, .. SystemHealthTables.All];

    /// <summary>
    /// Validates a parsed set of raw arguments and returns immutable options, or throws
    /// <see cref="ArgumentException"/> with a message naming the bad argument.
    /// </summary>
    public static ExportOptions Validate(
        string? store,
        string? from,
        string? to,
        string? outDir,
        string? tablesCsv,
        string pgHost,
        int pgPort,
        string pgDatabase,
        string pgUser,
        string? pgCredentialFile,
        int statementTimeoutSeconds,
        bool dryRun,
        bool allowOpenWindow,
        DateTime nowUtc,
        string? upload = null,
        string? awsRegion = null,
        string? stateDir = null,
        bool ignoreState = false,
        string? dimensionOrigin = null,
        string? alsoCsv = null,
        bool allowCompressed = false,
        bool pgTextFetchAll = false,
        bool planFetchAll = false,
        string? procedurePlanReuse = null,
        string? queryStoreSource = null,
        int? queryStoreLagHours = null,
        int? freshGuardMinutes = null,
        string? applicationName = null,
        CredentialSpec? credential = null)
    {
        if (freshGuardMinutes is not null && freshGuardMinutes is < 2 or > 15)
        {
            throw new ArgumentException("--fresh-guard-minutes must be an integer from 2 to 15");
        }

        if (queryStoreLagHours is not null && queryStoreLagHours is < 1 or > 3)
        {
            throw new ArgumentException("--query-store-lag-hours must be 1, 2 or 3");
        }

        var resolvedQueryStoreSource = QueryStoreSourceMode.Raw;
        if (queryStoreSource is not null && !QueryStoreSourceModes.TryParse(queryStoreSource, out resolvedQueryStoreSource))
        {
            throw new ArgumentException($"--query-store-source must be one of: {string.Join(", ", QueryStoreSourceModes.Names)}");
        }

        var resolvedProcedurePlanReuse = ProcedurePlanReuseMode.Shadow;
        if (procedurePlanReuse is not null && !ProcedurePlanReuseModes.TryParse(procedurePlanReuse, out resolvedProcedurePlanReuse))
        {
            throw new ArgumentException($"--procedure-plan-reuse must be one of: {string.Join(", ", ProcedurePlanReuseModes.Names)}");
        }

        if (applicationName is not null && string.IsNullOrWhiteSpace(applicationName))
        {
            throw new ArgumentException("--application-name must not be empty");
        }

        if (!IsValidStoreName(store))
        {
            throw new ArgumentException($"--store must match {StoreNamePattern}");
        }

        if (string.IsNullOrWhiteSpace(from) || !TryParseUtcMinute(from, out var fromUtc))
        {
            throw new ArgumentException("--from must be an ISO-8601 UTC timestamp on a whole minute, e.g. 2026-09-01T00:00:00Z");
        }

        if (string.IsNullOrWhiteSpace(to) || !TryParseUtcMinute(to, out var toUtc))
        {
            throw new ArgumentException("--to must be an ISO-8601 UTC timestamp on a whole minute, e.g. 2026-09-01T00:00:00Z");
        }

        if (toUtc <= fromUtc)
        {
            throw new ArgumentException("--to must be later than --from");
        }

        if (!string.IsNullOrWhiteSpace(dimensionOrigin) && !DimensionOrigins.All.Contains(dimensionOrigin))
        {
            throw new ArgumentException($"--dimension-origin must be one of: {string.Join(", ", DimensionOrigins.All)}");
        }

        var resolvedDimensionOrigin = string.IsNullOrWhiteSpace(dimensionOrigin) ? DimensionOrigins.Both : dimensionOrigin;

        // --also accepts optional names only; it is never wired into the Query Store call, so combining it with
        // --dimension-origin query_store is a configuration error, not a warning.
        string[] alsoTables = string.IsNullOrWhiteSpace(alsoCsv)
            ? []
            : alsoCsv.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (alsoTables.Length > 0 && resolvedDimensionOrigin == DimensionOrigins.QueryStore)
        {
            throw new ArgumentException("--also cannot be combined with --dimension-origin query_store; optional tables never run in the Query Store call");
        }

        // Per-family window guard, replacing the old flat 2h/3h rule. The Query Store family (its own 2h15m margin) is
        // the table set that either reads query_store_stats directly, or reads plan_shape/query_text from Query Store
        // (origin query_store or both). Every other table set only needs the 15-minute completeness margin the longest
        // collector cycle plus slack).
        var minTablesCsv = tablesCsv ?? string.Empty;
        var requestedTables = minTablesCsv.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        var isAllTablesRun = string.IsNullOrWhiteSpace(tablesCsv);
        var includesQueryStoreStats = isAllTablesRun || requestedTables.Contains(QueryStoreTables.QueryStoreStats);
        var includesPlanOrTextDimension = isAllTablesRun
            || requestedTables.Contains(DimensionTables.PlanShape)
            || requestedTables.Contains(DimensionTables.QueryText);
        var dimensionReadsQueryStore = resolvedDimensionOrigin is DimensionOrigins.QueryStore or DimensionOrigins.Both;
        var isQueryStoreFamily = includesQueryStoreStats || (includesPlanOrTextDimension && dimensionReadsQueryStore);
        var lagHours = queryStoreLagHours ?? 3;
        // The non-Query-Store margin is 15 minutes unless --fresh-guard-minutes lowers it. `run` passes the fresh_fast late-row
        // margin (`families.fast.late_row_margin_minutes`) here, and the planner makes a window due at To + margin, so a
        // due window must satisfy To <= now - margin. The Query Store family ignores this option.
        var freshMarginMinutes = freshGuardMinutes ?? 15;
        var guardMargin = isQueryStoreFamily ? QueryStoreGuardMargin(lagHours) : TimeSpan.FromMinutes(freshMarginMinutes);

        if (!allowOpenWindow && toUtc > nowUtc.Subtract(guardMargin))
        {
            var marginText = isQueryStoreFamily ? (lagHours == 1 ? "45 minutes" : lagHours == 2 ? "1 hour 15 minutes" : "2 hours 15 minutes") : $"{freshMarginMinutes} minutes";
            throw new ArgumentException(
                $"--to is later than now minus {marginText}; pass --allow-open-window to export a window that recent " +
                "(the store may still be receiving late-arriving rows for it)");
        }

        if (string.IsNullOrWhiteSpace(outDir))
        {
            throw new ArgumentException("--out is required");
        }

        string[] tables;
        if (string.IsNullOrWhiteSpace(tablesCsv))
        {
            tables = AllTables;
        }
        else
        {
            tables = tablesCsv.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            foreach (var t in tables)
            {
                // A denied table fails validation with the same usage-error exit code as an unknown table -- checked
                // before the unknown-table check, so the message is specific.
                if (ColumnPolicies.IsDenied(t))
                {
                    throw new ArgumentException($"Table '{t}' is denied and can never be exported.");
                }

                // A manual run may name a core table or a registry optional-table name directly via --tables.
                if (!AllTables.Contains(t) && OptionalTables.TryGet(t) is null)
                {
                    throw new ArgumentException($"Unknown table '{t}'. Known tables: {string.Join(", ", AllTables)}");
                }
            }
        }

        foreach (var t in alsoTables)
        {
            if (tables.Contains(t))
            {
                throw new ArgumentException($"--also table '{t}' is already in --tables; name it in one place only");
            }
        }

        // A window off the hour is allowed only for tables whose read is a plain collection_time range
        // (SubHourTables). Everything else buckets by hour or reads a midnight baseline/snapshot, so a partial
        // hour would silently produce a partial, non-reproducible export.
        if (SubHourTables.IsSubHourWindow(fromUtc, toUtc))
        {
            var hourlyOnly = SubHourTables.FirstHourlyOnlyTable(tables);
            if (hourlyOnly is not null)
            {
                throw new ArgumentException(
                    $"{hourlyOnly} needs a whole-hour window; --from/--to must be on a whole UTC hour unless every table is one of: {string.Join(", ", SubHourTables.All)}");
            }

            if (alsoTables.Length > 0)
            {
                throw new ArgumentException($"--also ({alsoTables[0]}) needs a whole-hour window; optional tables are not exported over a sub-hour window");
            }

            if (!string.IsNullOrWhiteSpace(dimensionOrigin))
            {
                throw new ArgumentException("--dimension-origin needs a whole-hour window; it only applies to the hourly-only dimension tables");
            }
        }

        if (statementTimeoutSeconds <= 0)
        {
            throw new ArgumentException("--statement-timeout-seconds must be positive");
        }

        return new ExportOptions
        {
            Store = store,
            FromUtc = fromUtc,
            ToUtc = toUtc,
            OutDir = outDir,
            Tables = tables,
            PgHost = pgHost,
            PgPort = pgPort,
            PgDatabase = pgDatabase,
            PgUser = pgUser,
            PgCredentialFile = pgCredentialFile,
            StatementTimeoutSeconds = statementTimeoutSeconds,
            DryRun = dryRun,
            AllowOpenWindow = allowOpenWindow,
            Upload = upload,
            AwsRegion = awsRegion,
            StateDir = stateDir ?? "",
            IgnoreState = ignoreState,
            DimensionOrigin = resolvedDimensionOrigin,
            Also = alsoTables,
            AllowCompressed = allowCompressed,
            PgTextFetchAll = pgTextFetchAll,
            PlanFetchAll = planFetchAll,
            ProcedurePlanReuse = resolvedProcedurePlanReuse,
            QueryStoreSource = resolvedQueryStoreSource,
            QueryStoreLagHours = lagHours,
            ApplicationName = applicationName ?? ConfigLoader.DefaultApplicationName,
            Credential = credential,
        };
    }

    /// <summary>
    /// Parses an ISO-8601 timestamp and requires it to land on a whole UTC hour (minutes,
    /// seconds and sub-seconds all zero).
    /// </summary>
    public static bool TryParseUtcHour(string text, out DateTime resultUtc) =>
        TryParseUtcMinute(text, out resultUtc) && resultUtc.Minute == 0;

    /// <summary>
    /// Parses an ISO-8601 timestamp and requires it to land on a whole UTC minute (seconds and
    /// sub-seconds zero). Whether a window off the hour is acceptable depends on the table set
    /// (<see cref="SubHourTables"/>), because the hourly rollups' date_trunc('hour', ...) and the
    /// midnight baselines would silently produce a partial, non-reproducible export.
    /// </summary>
    public static bool TryParseUtcMinute(string text, out DateTime resultUtc)
    {
        resultUtc = default;

        if (!DateTimeOffset.TryParse(
                text,
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal,
                out var parsed))
        {
            return false;
        }

        var utc = parsed.ToUniversalTime();

        if (utc.Second != 0 || utc.Millisecond != 0 || utc.Ticks % TimeSpan.TicksPerMinute != 0)
        {
            return false;
        }

        resultUtc = utc.UtcDateTime;
        return true;
    }
}
