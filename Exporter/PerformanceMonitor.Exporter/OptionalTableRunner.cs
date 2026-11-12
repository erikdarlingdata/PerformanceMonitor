using System.Diagnostics;
using System.Security.Cryptography;
using DuckDB.NET.Data;
using Npgsql;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Resolves and runs the tables named by `--also`/manual `--tables` optional names, and folds the result into the
/// separate optional manifest. This file is the seam that ties together the registry/<see
/// cref="SqlBuilder.BuildOptionalSelect"/> and the optional-table <see cref="OptionalTableExporter"/>.
/// </summary>
public static class OptionalTableRunner
{
    /// <summary>
    /// Splits <paramref name="requestedNames"/> into ones the registry knows and ones it does not. Absence on the
    /// store is checked separately in <see cref="RunAsync"/>, because it needs a live connection.
    /// </summary>
    public static (List<OptionalTable> Known, List<SkippedOptionalTable> Unknown) Resolve(
        IEnumerable<string> requestedNames, Func<string, OptionalTable?> lookup)
    {
        var known = new List<OptionalTable>();
        var unknown = new List<SkippedOptionalTable>();
        foreach (var name in requestedNames)
        {
            // The deny-list check runs BEFORE the registry lookup: a denied name never touches the store, whatever the
            // registry does or does not know about it.
            if (ColumnPolicies.IsDenied(name))
            {
                unknown.Add(new SkippedOptionalTable { Table = name, Reason = "denied" });
                Console.Error.WriteLine($"ERROR denied table {name}");
                continue;
            }

            var table = lookup(name);
            if (table is null)
            {
                unknown.Add(new SkippedOptionalTable { Table = name, Reason = "unknown" });
            }
            else
            {
                known.Add(table);
            }
        }

        return (known, unknown);
    }

    /// <summary>
    /// Runs every requested optional table, in registry order, against the store named by
    /// <paramref name="connectionStringBuilder"/>. A table absent on the store (`to_regclass` is null) is
    /// skipped with reason "absent"; every other failure lands in <c>failed_optional</c> and stops that table
    /// only -- the rest of the optional run, and the core export it followed, are unaffected.
    /// </summary>
    public static async Task<OptionalRunResult> RunAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder,
        ExportOptions options,
        List<OptionalTable> requested,
        List<SkippedOptionalTable> unknown,
        TextWriter output,
        CancellationToken ct,
        int? darlingSchemaVersion = null)
    {
        var skipped = new List<SkippedOptionalTable>(unknown);
        var failed = new List<FailedOptionalTable>();
        var entries = new List<ManifestTableEntry>();
        var pgTextKeys = new List<(string? ServerName, long QueryId)>();

        // Registry order, not request order. This also satisfies ordering rule for pg_statement_text: it is registered
        // after pg_statement_stats in OptionalTables.All, whatever order the caller named them in --also.
        var ordered = OptionalTables.All.Where(t => requested.Contains(t)).ToList();
        var requestedNames = requested.Select(t => t.Name).ToHashSet(StringComparer.Ordinal);

        // Absolute paths of every Parquet part pg_statement_stats wrote THIS run, so pg_statement_text (a Dimension
        // entry) can read its key set back without a second store read of pg_statement_stats.
        List<string>? pgStatementStatsPartPaths = null;

        foreach (var table in ordered)
        {
            // a Blocked entry is skipped with its note, never read, never a failure -- checked
            // before the deny/discovery/read path, and before the grain check below (a table can be both Blocked and
            // not-yet-implemented; the note is the more useful reason).
            if (table.Status == OptionalTableStatus.Blocked)
            {
                skipped.Add(new SkippedOptionalTable { Table = table.Name, Reason = $"blocked: {table.Note}" });
                output.WriteLine($"  skipped: blocked: {table.Note}");
                continue;
            }

            // The schema gate: a table pinned to a darling_schema_version the store hasn't reached yet is skipped,
            // never a failure. A null store version (an old Darling that has no darling_schema_version row) is treated
            // as "below every gate".
            if (table.MinDarlingSchemaVersion is { } minVersion
                && (darlingSchemaVersion is null || darlingSchemaVersion < minVersion))
            {
                skipped.Add(new SkippedOptionalTable { Table = table.Name, Reason = $"needs darling schema >= {minVersion}" });
                output.WriteLine($"  skipped: needs darling schema >= {minVersion}");
                continue;
            }

            // Hourly: the window's single raw read rolls up into <RollupName> and no raw part is
            // written. "Change + Rollup" tables (Grain == Change AND RollupName set, e.g. ag_database_replica_states)
            // are NOT Hourly-grain and never reach this branch -- they are dispatched through RunChangeOnlyAsync
            // below, which builds both outputs from one store read.
            if (table.Grain == ExportGrain.Hourly)
            {
                var hourlyPartPath = await RunHourlyRollupAsync(connectionStringBuilder, options, table, entries, skipped, failed, output, ct).ConfigureAwait(false);
                if (hourlyPartPath is not null && table.Name == "pg_statement_stats")
                {
                    pgStatementStatsPartPaths = [hourlyPartPath];
                }

                continue;
            }

            // Daily grain on a Window-kind table: the last complete collection cycle per server before the window's
            // own 00:00Z, read over its own lookback range -- never the caller's window. Placed immediately before the
            // grain-skip below, per the wave-3 shared- edit rule.
            if (table.Grain == ExportGrain.Daily && table.Kind == OptionalTableKind.Window)
            {
                output.WriteLine($"Exporting optional table {table.Name}...");
                try
                {
                    await using var absentCheckConnection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
                    var isAbsent = await IsAbsentAsync(absentCheckConnection, table, ct).ConfigureAwait(false);
                    if (isAbsent)
                    {
                        skipped.Add(new SkippedOptionalTable { Table = table.Name, Reason = "absent" });
                        output.WriteLine($"  skipped: absent on this store");
                        continue;
                    }

                    await using var discoveryConnection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
                    var columns = await DiscoverColumnsAsync(discoveryConnection, table, ct).ConfigureAwait(false);
                    var policy = ColumnPolicies.For(table.Name);
                    var sql = SqlBuilder.BuildOptionalSelect(table, columns, policy);

                    // The day's snapshot is produced once: by the 00:00 window, or by the first later window of the
                    // day when no run has recorded it. Under --ignore-state nothing is recorded, so only 00:00 runs it.
                    using var dailyState = options.IgnoreState ? null : StateStore.Open(options.StateDir);
                    var day = options.FromUtc.Date;
                    var dayAlreadyExported = dailyState is null
                        ? !OptionalTableExporter.IsDailySnapshotWindow(options.FromUtc)
                        : dailyState.HasDailySnapshot(options.Store, table.Name, day);

                    var result = await DailySnapshotExporter.ExportAsync(
                        table, options, options.Store, options.OutDir,
                        (readFrom, readTo, innerCt) => OpenRangeReaderAsync(connectionStringBuilder, sql, readFrom, readTo, options, table, innerCt),
                        ct, dayAlreadyExported).ConfigureAwait(false);

                    if (result.SkippedReason is not null)
                    {
                        skipped.Add(new SkippedOptionalTable { Table = table.Name, Reason = result.SkippedReason });
                        output.WriteLine($"  {result.SkippedReason}");
                        continue;
                    }

                    dailyState?.RecordDailySnapshot(options.Store, table.Name, day);

                    foreach (var part in result.Parts)
                    {
                        entries.Add(new ManifestTableEntry
                        {
                            Table = table.Name,
                            OutputFile = part.RelativePath,
                            RowCount = part.RowCount,
                            ByteSize = part.ByteSize,
                            Sha256 = part.Sha256,
                            Columns = ManifestColumn.OfParquetFile(AbsolutePath(options, part.RelativePath)),
                            StoreQueryElapsedMs = result.StoreQueryElapsedMs,
                            WriteElapsedMs = result.WriteElapsedMs,
                            MissingContentCount = 0,
                            TimeColumn = "snapshot_time",
                            Kind = "snapshot",
                            KeyColumns = null,
                            PolicyVersion = policy?.Version,
                            Grain = "daily",
                            ReadFrom = result.ReadFrom,
                            ReadTo = result.ReadTo,
                        });
                    }

                    output.WriteLine($"  {result.RowCount} rows across {result.Parts.Count} part(s)");
                }
                catch (Exception ex)
                {
                    failed.Add(new FailedOptionalTable { Table = table.Name, Error = ex.Message });
                    output.WriteLine($"  FAILED: {ex.Message}");
                }

                continue;
            }

            // Change grain: its own path, dispatched here so a Change table never falls
            // through to the generic Event Window/Snapshot read below.
            if (table.Grain == ExportGrain.Change)
            {
                await RunChangeOnlyAsync(connectionStringBuilder, options, table, entries, skipped, failed, output, ct).ConfigureAwait(false);
                continue;
            }

            // Safety net: every grain now has a path above, so nothing reaches this skip unless a new grain is added
            // without one. The exceptions are Daily on the Snapshot kind (servers, today's existing daily-snapshot
            // Event path) and Daily on the Window kind (handled above).
            if (table.Grain != ExportGrain.Event
                && !(table.Grain == ExportGrain.Daily && table.Kind == OptionalTableKind.Snapshot)
                && table.Grain != ExportGrain.Daily)
            {
                skipped.Add(new SkippedOptionalTable { Table = table.Name, Reason = $"grain {table.Grain} not implemented yet" });
                output.WriteLine($"  skipped: grain {table.Grain} not implemented yet");
                continue;
            }

            if (table.Kind == OptionalTableKind.Dimension)
            {
                if (table.Name == "pg_statement_text")
                {
                    await RunPgStatementTextAsync(
                        connectionStringBuilder, options, table, requestedNames, pgStatementStatsPartPaths,
                        entries, skipped, failed, pgTextKeys, output, ct).ConfigureAwait(false);
                    continue;
                }

                skipped.Add(new SkippedOptionalTable { Table = table.Name, Reason = "unknown dimension" });
                continue;
            }

            output.WriteLine($"Exporting optional table {table.Name}...");
            try
            {
                await using var absentCheckConnection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
                var isAbsent = await IsAbsentAsync(absentCheckConnection, table, ct).ConfigureAwait(false);
                if (isAbsent)
                {
                    skipped.Add(new SkippedOptionalTable { Table = table.Name, Reason = "absent" });
                    output.WriteLine($"  skipped: absent on this store");
                    continue;
                }

                await using var discoveryConnection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
                var columns = await DiscoverColumnsAsync(discoveryConnection, table, ct).ConfigureAwait(false);

                // The config-schema column tripwire: a table in the config schema with a secret-like discovered column
                // fails, and is never read. collect tables (e.g. ag_replica_states.endpoint_url) are not checked.
                if (string.Equals(table.Schema, "config", StringComparison.OrdinalIgnoreCase))
                {
                    var secretLikeColumn = columns.Select(c => c.Name).FirstOrDefault(ColumnPolicies.IsSecretLikeColumn);
                    if (secretLikeColumn is not null)
                    {
                        failed.Add(new FailedOptionalTable { Table = table.Name, Error = $"secret-like column {secretLikeColumn}" });
                        output.WriteLine($"  FAILED: secret-like column {secretLikeColumn}");
                        continue;
                    }
                }

                // query_snapshots (7b) is not a plain projection: its plans dedupe into plan_shape/plan_features and
                // its manifest entry carries unparsed_plan_rows. One call site, one new type -- QuerySnapshotsExporter
                // -- so ExportRunner/DimensionTableExporter stay untouched.
                if (table.Name == "query_snapshots")
                {
                    var qsResult = await QuerySnapshotsExporter.ExportAsync(connectionStringBuilder, options, columns.Select(c => c.Name).ToList(), ct).ConfigureAwait(false);

                    entries.Add(BuildQuerySnapshotsEntry(
                        table, "query_snapshots", qsResult.FactOutputPath, qsResult.FactRowCount, part: null,
                        timeColumn: table.TimeColumn, kind: "window", unparsedPlanRows: qsResult.UnparsedPlanRows));
                    entries.Add(BuildQuerySnapshotsEntry(
                        table, DimensionTables.PlanShape, qsResult.PlanOutputPath, qsResult.PlanRowCount, QuerySnapshotsExporter.Part,
                        timeColumn: "last_seen", kind: "dimension", unparsedPlanRows: null));
                    entries.Add(BuildQuerySnapshotsEntry(
                        table, PlanFeatureTables.PlanFeatures, qsResult.FeaturesOutputPath, qsResult.FeatureRowCount, QuerySnapshotsExporter.Part,
                        timeColumn: null, kind: "dimension", unparsedPlanRows: null));
                    entries.Add(BuildQuerySnapshotsEntry(
                        table, PlanFeatureTables.PlanWarnings, qsResult.WarningsOutputPath, qsResult.WarningRowCount, QuerySnapshotsExporter.Part,
                        timeColumn: null, kind: "dimension", unparsedPlanRows: null));
                    entries.Add(BuildQuerySnapshotsEntry(
                        table, PlanFeatureTables.PlanMissingIndexes, qsResult.MissingIndexesOutputPath, qsResult.MissingIndexRowCount, QuerySnapshotsExporter.Part,
                        timeColumn: null, kind: "dimension", unparsedPlanRows: null));

                    output.WriteLine($"  {qsResult.FactRowCount} rows; {qsResult.PlanRowCount} new plan_shape row(s); unparsed_plan_rows={qsResult.UnparsedPlanRows}");
                    continue;
                }

                var policy = ColumnPolicies.For(table.Name);
                var sql = SqlBuilder.BuildOptionalSelect(table, columns, policy);

                // A snapshot table's day is produced once, like a Daily-grain table's: by the 00:00 window, or by the
                // first later window of the day when no run has recorded it.
                var isSnapshot = table.Kind == OptionalTableKind.Snapshot;
                using var snapshotState = isSnapshot && !options.IgnoreState ? StateStore.Open(options.StateDir) : null;
                var day = options.FromUtc.Date;
                var request = new OptionalExportRequest(table.Name, table.TimeColumn, isSnapshot, options.FromUtc, options.ToUtc)
                {
                    DayAlreadyExported = isSnapshot && (snapshotState is null
                        ? !OptionalTableExporter.IsDailySnapshotWindow(options.FromUtc)
                        : snapshotState.HasDailySnapshot(options.Store, table.Name, day)),
                };
                var result = await OptionalTableExporter.ExportAsync(
                    request,
                    options.Store,
                    options.OutDir,
                    innerCt => OpenReaderAsync(connectionStringBuilder, sql, options, table, innerCt),
                    ct).ConfigureAwait(false);

                if (result.SkippedReason is not null)
                {
                    skipped.Add(new SkippedOptionalTable { Table = table.Name, Reason = result.SkippedReason });
                    output.WriteLine($"  {result.SkippedReason}");
                    continue;
                }

                snapshotState?.RecordDailySnapshot(options.Store, table.Name, day);

                if (table.Name == "pg_statement_stats")
                {
                    pgStatementStatsPartPaths = [.. result.Parts.Select(p => Path.Combine(options.OutDir, options.Store, p.RelativePath.Replace('/', Path.DirectorySeparatorChar)))];
                }

                foreach (var part in result.Parts)
                {
                    entries.Add(new ManifestTableEntry
                    {
                        Table = table.Name,
                        OutputFile = part.RelativePath,
                        RowCount = part.RowCount,
                        ByteSize = part.ByteSize,
                        Sha256 = part.Sha256,
                        Columns = ManifestColumn.OfParquetFile(AbsolutePath(options, part.RelativePath)),
                        StoreQueryElapsedMs = result.StoreQueryElapsedMs,
                        WriteElapsedMs = result.WriteElapsedMs,
                        MissingContentCount = 0,
                        // A snapshot table has no time column of its own; its rows carry the stamped snapshot_time.
                        TimeColumn = table.Kind == OptionalTableKind.Snapshot ? "snapshot_time" : table.TimeColumn,
                        Kind = table.Kind == OptionalTableKind.Snapshot ? "snapshot" : "window",
                        KeyColumns = null,
                        PolicyVersion = policy?.Version,
                    });
                }

                output.WriteLine($"  {result.RowCount} rows across {result.Parts.Count} part(s)");
            }
            catch (Exception ex)
            {
                failed.Add(new FailedOptionalTable { Table = table.Name, Error = ex.Message });
                output.WriteLine($"  FAILED: {ex.Message}");
            }
        }

        var deniedCount = skipped.Count(s => s.Reason == "denied");
        return new OptionalRunResult(entries, skipped, failed, deniedCount, pgTextKeys);
    }

    /// <summary>
    /// Runs a Grain.Change table through <see cref="ChangeOnlyExporter"/>: opens (or skips, under
    /// <c>--ignore-state</c>) the <see cref="ChangeStateStore"/> at <c>options.StateDir</c>, discovers columns, builds
    /// the store SELECT through <see cref="SqlBuilder.BuildOptionalSelect"/> exactly like the Event path, and folds
    /// the result into one manifest entry with <c>grain:"change"</c>.
    /// </summary>
    private static async Task RunChangeOnlyAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder,
        ExportOptions options,
        OptionalTable table,
        List<ManifestTableEntry> entries,
        List<SkippedOptionalTable> skipped,
        List<FailedOptionalTable> failed,
        TextWriter output,
        CancellationToken ct)
    {
        output.WriteLine($"Exporting optional table {table.Name}...");
        try
        {
            await using var absentCheckConnection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
            if (await IsAbsentAsync(absentCheckConnection, table, ct).ConfigureAwait(false))
            {
                skipped.Add(new SkippedOptionalTable { Table = table.Name, Reason = "absent" });
                output.WriteLine("  skipped: absent on this store");
                return;
            }

            await using var discoveryConnection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
            var columns = await DiscoverColumnsAsync(discoveryConnection, table, ct).ConfigureAwait(false);
            var policy = ColumnPolicies.For(table.Name);
            var sql = SqlBuilder.BuildOptionalSelect(table, columns, policy);

            ChangeStateStore? changeStateStore = options.IgnoreState ? null : ChangeStateStore.Open(options.StateDir);
            try
            {
                var forceBaseline = !options.IgnoreState && changeStateStore is not null &&
                    (changeStateStore.MinWindowTo(options.Store, table.Name) is { } minWindowTo && options.FromUtc < minWindowTo);

                var result = await ChangeOnlyExporter.ExportAsync(
                    table,
                    columns,
                    options.Store,
                    options.OutDir,
                    innerCt => OpenReaderAsync(connectionStringBuilder, sql, options, table, innerCt),
                    options.FromUtc,
                    options.ToUtc,
                    DateTime.UtcNow,
                    changeStateStore,
                    forceBaseline,
                    ct).ConfigureAwait(false);

                foreach (var part in result.Parts)
                {
                    entries.Add(new ManifestTableEntry
                    {
                        Table = table.Name,
                        OutputFile = part.RelativePath,
                        RowCount = part.RowCount,
                        ByteSize = part.ByteSize,
                        Sha256 = part.Sha256,
                        Columns = ManifestColumn.OfParquetFile(AbsolutePath(options, part.RelativePath)),
                        StoreQueryElapsedMs = result.StoreQueryElapsedMs,
                        WriteElapsedMs = result.WriteElapsedMs,
                        MissingContentCount = 0,
                        TimeColumn = table.TimeColumn,
                        Kind = "window",
                        KeyColumns = null,
                        PolicyVersion = policy?.Version,
                        Grain = "change",
                        ChangeKeyColumns = result.ResolvedKeyColumns,
                        ChangeBaseline = result.Baseline,
                        ChangeRowsSuppressed = result.RowsSuppressed,
                    });
                }

                if (result.RollupPart is not null)
                {
                    entries.Add(new ManifestTableEntry
                    {
                        Table = table.RollupName!,
                        SourceTable = $"{table.Schema}.{table.Name}",
                        OutputFile = result.RollupPart.RelativePath,
                        RowCount = result.RollupPart.RowCount,
                        ByteSize = result.RollupPart.ByteSize,
                        Sha256 = result.RollupPart.Sha256,
                        Columns = ManifestColumn.OfParquetFile(AbsolutePath(options, result.RollupPart.RelativePath)),
                        StoreQueryElapsedMs = 0,
                        WriteElapsedMs = result.WriteElapsedMs,
                        MissingContentCount = 0,
                        TimeColumn = "hour",
                        Kind = "window",
                        KeyColumns = null,
                        PolicyVersion = policy?.Version,
                        Grain = "hourly",
                        RollupVersion = RollupSql.Specs[table.RollupName!].Version,
                    });

                    output.WriteLine($"  plus {result.RollupRowCount} rolled-up row(s) into {table.RollupName}");
                }

                output.WriteLine($"  {result.RowCount} rows across {result.Parts.Count} part(s); baseline={result.Baseline}");
            }
            finally
            {
                changeStateStore?.Dispose();
            }
        }
        catch (Exception ex)
        {
            failed.Add(new FailedOptionalTable { Table = table.Name, Error = ex.Message });
            output.WriteLine($"  FAILED: {ex.Message}");
        }
    }

    /// <summary>
    /// Routes `pg_statement_text`: computes the (server_name, queryid) key set in DuckDB from the SAME run's
    /// `pg_statement_stats` Parquet parts (never a second store read of that table), drops keys already recorded in
    /// the StateStore's `pg_text_keys`, fetches the unknown ones from `collect.pg_statement_text` in paced batches,
    /// writes a Parquet part, and adds the new keys to <paramref name="pgTextKeys"/>. It never records them itself:
    /// the caller records them once the optional manifest is written and, with `--upload`, uploaded, so a failed
    /// upload leaves the keys unknown and the next run fetches that text again.
    /// </summary>
    private static async Task RunPgStatementTextAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder,
        ExportOptions options,
        OptionalTable table,
        HashSet<string> requestedNames,
        List<string>? pgStatementStatsPartPaths,
        List<ManifestTableEntry> entries,
        List<SkippedOptionalTable> skipped,
        List<FailedOptionalTable> failed,
        List<(string? ServerName, long QueryId)> pgTextKeys,
        TextWriter output,
        CancellationToken ct)
    {
        output.WriteLine($"Exporting optional table {table.Name}...");

        if (!requestedNames.Contains("pg_statement_stats") || pgStatementStatsPartPaths is null || pgStatementStatsPartPaths.Count == 0)
        {
            skipped.Add(new SkippedOptionalTable { Table = table.Name, Reason = PgStatementTextExporter.MissingStatsReason });
            output.WriteLine($"  {PgStatementTextExporter.MissingStatsReason}");
            return;
        }

        try
        {
            var storeStopwatch = Stopwatch.StartNew();
            var textPacer = new ContentFetchPacer(allowCompressed: options.AllowCompressed);
            var timings = new ExportTimings(textPacer);

            // Key set: read the already-written pg_statement_stats Parquet parts into DuckDB and compute the distinct
            // (server_name, queryid) pairs there -- never a second store read.
            var globs = string.Join(", ", pgStatementStatsPartPaths.Select(p => $"'{p.Replace("'", "''")}'"));
            using var duckDb = DuckDbSession.OpenInMemory(Path.Combine(Path.GetTempPath(), $"darling-export-pgtext-{Guid.NewGuid():N}"));
            using (var createCommand = duckDb.CreateCommand())
            {
                createCommand.CommandText = $"CREATE TABLE pg_statement_stats_run AS SELECT * FROM read_parquet([{globs}])";
                createCommand.ExecuteNonQuery();
            }

            // Threshold gate: fetch text only for statements with calls >= 2 or total exec time >= 100 ms.
            // IgnoreState and --pg-text-fetch-all bypass it.
            var keySet = PgStatementTextExporter.ReadKeySetGated(
                duckDb, "pg_statement_stats_run", options.IgnoreState || options.PgTextFetchAll, output);
            var allKeys = keySet.Keys;
            var belowThresholdSkipped = keySet.BelowThresholdSkipped;
            if (keySet.GateApplied)
            {
                output.WriteLine($"  threshold gate: {belowThresholdSkipped} key(s) below threshold skipped");
            }

            using var stateStore = StateStore.Open(options.StateDir);
            var unknownKeys = options.IgnoreState
                ? allKeys.ToHashSet()
                : stateStore.FilterUnknownPgTextKeys(options.Store, allKeys);

            if (unknownKeys.Count == 0)
            {
                output.WriteLine("  0 rows (every key already known)");
                return;
            }

            // Resolve server_name -> server_id for the unknown keys via collect.servers, so the store-side probe can
            // join on (server_id, queryid) as the primary key.
            await using var serversConnection = await timings.OpenConnectionAsync(connectionStringBuilder, ct).ConfigureAwait(false);
            var serverIdByName = new Dictionary<string, int>(StringComparer.Ordinal);
            await timings.StoreReadAsync(async () =>
            {
                await using var serversCommand = new NpgsqlCommand("/* darling-export:opt.servers */ SELECT server_id, server_name FROM collect.servers", serversConnection);
                await using var reader = await serversCommand.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    serverIdByName[reader.GetString(1)] = reader.GetInt32(0);
                }

                return 0;
            }).ConfigureAwait(false);

            var idKeys = new List<(int ServerId, long QueryId)>();
            var nameByServerId = new Dictionary<int, string?>();
            var unresolvedKeyCount = 0;
            foreach (var (serverName, queryId) in unknownKeys)
            {
                if (serverName is null || !serverIdByName.TryGetValue(serverName, out var serverId))
                {
                    unresolvedKeyCount++;
                    continue;
                }

                idKeys.Add((serverId, queryId));
                nameByServerId[serverId] = serverName;
            }

            if (unresolvedKeyCount > 0)
            {
                output.WriteLine($"  {unresolvedKeyCount} key(s) dropped: server_name not in collect.servers");
            }

            await using var fetchConnection = await timings.OpenConnectionAsync(connectionStringBuilder, ct).ConfigureAwait(false);
            var textRows = await PgStatementTextExporter.FetchContentAsync(
                fetchConnection, idKeys, ms => PgStatementTextExporter.DefaultSleep(ms, ct), ct, options.AllowCompressed,
                timings, textPacer).ConfigureAwait(false);
            storeStopwatch.Stop();

            var writeStopwatch = Stopwatch.StartNew();
            var columns = new List<ColumnShape>
            {
                new("source_store", "VARCHAR"),
                new("server_name", "VARCHAR"),
                new("queryid", "BIGINT"),
                new("query_text", "VARCHAR"),
                new("first_seen", "TIMESTAMP"),
                new("last_seen", "TIMESTAMP"),
            };

            var tableDir = Path.Combine(options.OutDir, options.Store, table.Name);
            Directory.CreateDirectory(tableDir);
            var relativePath = OutputPaths.RelativeParquetPath(options.Store, table.Name, options.FromUtc, options.ToUtc);
            var absolutePath = Path.Combine(options.OutDir, options.Store, relativePath.Replace('/', Path.DirectorySeparatorChar));

            using var writeDb = DuckDbSession.OpenInMemory(Path.Combine(Path.GetTempPath(), $"darling-export-pgtext-write-{Guid.NewGuid():N}"));
            using (var createCommand = writeDb.CreateCommand())
            {
                var columnDefs = string.Join(", ", columns.Select(c => $"\"{c.Name}\" {c.DuckDbType}"));
                createCommand.CommandText = $"CREATE TABLE {table.Name} ({columnDefs})";
                createCommand.ExecuteNonQuery();
            }

            using (var appender = writeDb.CreateAppender(table.Name))
            {
                foreach (var row in textRows)
                {
                    if (!nameByServerId.TryGetValue(row.ServerId, out var serverName))
                    {
                        continue;
                    }

                    var appenderRow = appender.CreateRow();
                    appenderRow.AppendValue(options.Store);
                    appenderRow.AppendValue(serverName);
                    appenderRow.AppendValue(row.QueryId);
                    appenderRow.AppendValue(row.QueryText);
                    appenderRow.AppendValue(row.FirstSeen);
                    appenderRow.AppendValue(row.LastSeen);
                    appenderRow.EndRow();
                }
            }

            using (var copyCommand = writeDb.CreateCommand())
            {
                copyCommand.CommandText =
                    $"COPY (SELECT * FROM {table.Name} ORDER BY server_name, queryid) TO '{absolutePath.Replace("'", "''")}' " +
                    "(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000, ROW_GROUP_SIZE_BYTES '64MB')";
                copyCommand.ExecuteNonQuery();
            }

            writeStopwatch.Stop();

            var byteSize = new FileInfo(absolutePath).Length;
            string sha256;
            using (var stream = File.OpenRead(absolutePath))
            {
                sha256 = Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(stream));
            }

            entries.Add(new ManifestTableEntry
            {
                Table = table.Name,
                OutputFile = relativePath,
                RowCount = textRows.Count,
                ByteSize = byteSize,
                Sha256 = sha256,
                Columns = ManifestColumn.OfParquetFile(absolutePath),
                StoreQueryElapsedMs = storeStopwatch.Elapsed.TotalMilliseconds,
                WriteElapsedMs = writeStopwatch.Elapsed.TotalMilliseconds,
                MissingContentCount = 0,
                TimeColumn = null,
                Kind = "dimension",
                KeyColumns = table.KeyColumns,
                UnresolvedKeys = unresolvedKeyCount,
                BelowThresholdSkipped = keySet.GateApplied ? belowThresholdSkipped : null,
                Timings = timings.Snapshot(includeParse: false, includeWindowCache: false),
            });

            if (!options.IgnoreState)
            {
                pgTextKeys.AddRange(idKeys.Select(k => (nameByServerId.GetValueOrDefault(k.ServerId), k.QueryId)));
            }

            output.WriteLine($"  {textRows.Count} rows across 1 part(s)");
        }
        catch (Exception ex)
        {
            failed.Add(new FailedOptionalTable { Table = table.Name, Error = ex.Message });
            output.WriteLine($"  FAILED: {ex.Message}");
        }
    }

    /// <summary>
    /// One `query_snapshots` run output's manifest entry. <paramref name="outputPath"/> is the file's on-disk path
    /// under `&lt;out&gt;/&lt;store&gt;/&lt;outputTable&gt;/`; the entry names it the way every other entry does, as
    /// `&lt;table&gt;/&lt;file&gt;` relative to the store folder, always with `/`.
    /// </summary>
    internal static ManifestTableEntry BuildQuerySnapshotsEntry(
        OptionalTable table, string outputTable, string outputPath, long rowCount, string? part,
        string? timeColumn, string kind, long? unparsedPlanRows)
    {
        var byteSize = File.Exists(outputPath) ? new FileInfo(outputPath).Length : 0;
        var sha256 = File.Exists(outputPath) ? ComputeSha256(outputPath) : string.Empty;
        return new ManifestTableEntry
        {
            Table = outputTable,
            OutputFile = $"{outputTable}/{Path.GetFileName(outputPath)}",
            RowCount = rowCount,
            ByteSize = byteSize,
            Sha256 = sha256,
            Columns = ManifestColumn.OfParquetFile(outputPath),
            StoreQueryElapsedMs = 0,
            WriteElapsedMs = 0,
            MissingContentCount = 0,
            TimeColumn = timeColumn,
            Kind = kind,
            KeyColumns = null,
            UnparsedPlanRows = unparsedPlanRows,
        };
    }

    private static string ComputeSha256(string path)
    {
        using var stream = File.OpenRead(path);
        var hash = SHA256.HashData(stream);
        return Convert.ToHexStringLower(hash);
    }

    /// <summary>The on-disk path of a part the manifest names by its store-relative `/` path.</summary>
    private static string AbsolutePath(ExportOptions options, string relativePath) =>
        Path.Combine(options.OutDir, options.Store, relativePath.Replace('/', Path.DirectorySeparatorChar));

    /// <summary>
    /// Runs one Hourly-grain table: the window's single raw read into <see cref="HourlyRollupExporter"/>, which rolls
    /// it up and writes exactly one part named after <see cref="OptionalTable.RollupName"/>. Returns the absolute path
    /// of that part when the export ran, so the caller can feed it to <c>pg_statement_text</c> in place of the (now
    /// nonexistent) raw part. Absence and column discovery use the same checks the Event path uses.
    /// </summary>
    private static async Task<string?> RunHourlyRollupAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder,
        ExportOptions options,
        OptionalTable table,
        List<ManifestTableEntry> entries,
        List<SkippedOptionalTable> skipped,
        List<FailedOptionalTable> failed,
        TextWriter output,
        CancellationToken ct)
    {
        output.WriteLine($"Exporting optional table {table.Name}...");
        try
        {
            await using var absentCheckConnection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
            if (await IsAbsentAsync(absentCheckConnection, table, ct).ConfigureAwait(false))
            {
                skipped.Add(new SkippedOptionalTable { Table = table.Name, Reason = "absent" });
                output.WriteLine($"  skipped: absent on this store");
                return null;
            }

            await using var discoveryConnection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
            var columns = await DiscoverColumnsAsync(discoveryConnection, table, ct).ConfigureAwait(false);
            var policy = ColumnPolicies.For(table.Name);
            var sql = SqlBuilder.BuildOptionalSelect(table, columns, policy);
            var spec = RollupSql.Specs[table.RollupName!];

            // A spec with counters reads one bridge lookback before the window, so each key's last sample before the
            // window is in the read: it feeds the counters' bridged delta and is never counted itself.
            var readFrom = spec.HasCounters ? options.FromUtc - HourlyRollupExporter.CounterBridgeLookback : options.FromUtc;
            var result = await HourlyRollupExporter.ExportAsync(
                spec, options.Store, options.OutDir, options.FromUtc, options.ToUtc,
                innerCt => OpenRangeReaderAsync(connectionStringBuilder, sql, readFrom, options.ToUtc, options, table, innerCt),
                ct).ConfigureAwait(false);

            entries.Add(new ManifestTableEntry
            {
                Table = table.RollupName!,
                SourceTable = $"{table.Schema}.{table.Name}",
                OutputFile = result.Part.RelativePath,
                RowCount = result.Part.RowCount,
                ByteSize = result.Part.ByteSize,
                Sha256 = result.Part.Sha256,
                Columns = ManifestColumn.OfParquetFile(AbsolutePath(options, result.Part.RelativePath)),
                StoreQueryElapsedMs = result.StoreQueryElapsedMs,
                WriteElapsedMs = result.WriteElapsedMs,
                MissingContentCount = 0,
                TimeColumn = "hour",
                Kind = "window",
                KeyColumns = null,
                PolicyVersion = policy?.Version,
                Grain = "hourly",
                RollupVersion = spec.Version,
                ReadFrom = readFrom,
                ReadTo = options.ToUtc,
            });

            output.WriteLine($"  {result.RawRowCount} raw row(s) rolled up into {result.RolledRowCount} row(s)");
            return Path.Combine(options.OutDir, options.Store, result.Part.RelativePath.Replace('/', Path.DirectorySeparatorChar));
        }
        catch (Exception ex)
        {
            failed.Add(new FailedOptionalTable { Table = table.Name, Error = ex.Message });
            output.WriteLine($"  FAILED: {ex.Message}");
            return null;
        }
    }

    private static async Task<bool> IsAbsentAsync(NpgsqlConnection connection, OptionalTable table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("/* darling-export:opt.absent_check */ SELECT to_regclass(@qualified) IS NULL", connection);
        command.Parameters.AddWithValue("qualified", $"{table.Schema}.{table.Name}");
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is bool b && b;
    }

    private static async Task<List<DiscoveredColumn>> DiscoverColumnsAsync(NpgsqlConnection connection, OptionalTable table, CancellationToken ct)
    {
        var columns = new List<DiscoveredColumn>();
        await using var command = new NpgsqlCommand(SqlBuilder.OptionalColumnDiscoverySql, connection);
        command.Parameters.AddWithValue("schema", table.Schema);
        command.Parameters.AddWithValue("table", table.Name);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            columns.Add(new DiscoveredColumn(reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        }

        if (columns.Count == 0)
        {
            throw new InvalidOperationException($"No exportable columns found for {table.Schema}.{table.Name}.");
        }

        return columns;
    }

    /// <summary>
    /// Opens a reader for a table's own read range rather than the caller's window: a Daily-grain table's lookback
    /// range, or an Hourly-grain table's window plus its counter bridge lookback. The chunk guard and the @from/@to
    /// parameters are both bound to <paramref name="readFrom"/>/ <paramref name="readTo"/>.
    /// </summary>
    private static async Task<System.Data.Common.DbDataReader> OpenRangeReaderAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, string sql, DateTime readFrom, DateTime readTo,
        ExportOptions options, OptionalTable table, CancellationToken ct)
    {
        var connection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
        var command = new NpgsqlCommand(sql, connection);

        await StoreCapabilities.GuardAgainstCompressedChunksAsync(
            connection, table.Schema, table.Name, readFrom, readTo, options.AllowCompressed, ct)
            .ConfigureAwait(false);

        command.Parameters.Add(new NpgsqlParameter("from", NpgsqlTypes.NpgsqlDbType.Timestamp)
        {
            Value = DateTime.SpecifyKind(readFrom, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter("to", NpgsqlTypes.NpgsqlDbType.Timestamp)
        {
            Value = DateTime.SpecifyKind(readTo, DateTimeKind.Unspecified),
        });

        return await command.ExecuteReaderAsync(System.Data.CommandBehavior.CloseConnection, ct).ConfigureAwait(false);
    }

    private static async Task<System.Data.Common.DbDataReader> OpenReaderAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, string sql, ExportOptions options, OptionalTable table, CancellationToken ct)
    {
        var connection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
        var command = new NpgsqlCommand(sql, connection);
        if (sql.Contains("@from", StringComparison.Ordinal))
        {
            // The chunk guard. Only windowed optional reads (those with an @from/@to) can hit a hypertable; a snapshot
            // read has no window to guard.
            await StoreCapabilities.GuardAgainstCompressedChunksAsync(
                connection, table.Schema, table.Name, options.FromUtc, options.ToUtc, options.AllowCompressed, ct)
                .ConfigureAwait(false);

            command.Parameters.Add(new NpgsqlParameter("from", NpgsqlTypes.NpgsqlDbType.Timestamp)
            {
                Value = DateTime.SpecifyKind(options.FromUtc, DateTimeKind.Unspecified),
            });
            command.Parameters.Add(new NpgsqlParameter("to", NpgsqlTypes.NpgsqlDbType.Timestamp)
            {
                Value = DateTime.SpecifyKind(options.ToUtc, DateTimeKind.Unspecified),
            });
        }

        return await command.ExecuteReaderAsync(System.Data.CommandBehavior.CloseConnection, ct).ConfigureAwait(false);
    }
}

/// <summary>The result of one optional-table run: the manifest entries for tables that succeeded, plus skipped/failed.
/// <see cref="DeniedCount"/> is the number of requested names rejected by the deny-list; the caller exits 3 when it is
/// greater than zero, even if everything else in the run succeeded. <see cref="PgTextKeys"/> are the
/// `pg_statement_text` keys fetched this run and not yet recorded in the state store; the caller records them after the
/// optional manifest is published.</summary>
public sealed record OptionalRunResult(
    List<ManifestTableEntry> Entries,
    List<SkippedOptionalTable> Skipped,
    List<FailedOptionalTable> Failed,
    int DeniedCount = 0,
    List<(string? ServerName, long QueryId)>? PgTextKeys = null);
