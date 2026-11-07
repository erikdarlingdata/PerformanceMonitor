using System.Reflection;
using Npgsql;
using PerformanceMonitor.Exporter.Upload;

namespace PerformanceMonitor.Exporter;

/// <summary>Runs a full export for one (store, window): every requested table, then the manifest.</summary>
public sealed class ExportRunner
{
    /// <summary>
    /// Runs the export. In dry-run mode, prints each table's SQL and returns without
    /// connecting for data (raw tables still connect once, to discover columns). Otherwise
    /// exports every table and writes the manifest last, only if every table succeeded.
    /// </summary>
    public static async Task<int> RunAsync(ExportOptions options, TextWriter output, CancellationToken ct)
    {
        // Defense in depth for options built without ExportOptions.Validate: a sub-hour window never reaches the
        // store for an hourly-only table (see SubHourTables).
        if (SubHourTables.IsSubHourWindow(options.FromUtc, options.ToUtc))
        {
            var hourlyOnly = SubHourTables.FirstHourlyOnlyTable(options.Tables.Concat(options.Also));
            if (hourlyOnly is not null)
            {
                throw new ArgumentException($"{hourlyOnly} needs a whole-hour window");
            }
        }

        if (options.DryRun)
        {
            return await RunDryRunAsync(options, output, ct).ConfigureAwait(false);
        }

        var connectionStringBuilder = StoreConnectionFactory.Build(options);

        var stragglerResult = await RunStragglerGuardAsync(connectionStringBuilder, options, output, ct).ConfigureAwait(false);

        int? darlingSchemaVersion = await ReadDarlingSchemaVersionAsync(connectionStringBuilder, ct).ConfigureAwait(false);

        // the chunk guard now runs as a pre-flight, before ANY store read of a core table's data. This replaces
        // the per-read guard calls inside the window caches / TableExporter / OptionalTableRunner's core-table path;
        // there is exactly one guard call per requested core hypertable now, run here, before the schema-version
        // read's connection is even reused for data.
        await RunChunkGuardPreflightAsync(connectionStringBuilder, options, ct).ConfigureAwait(false);

        // A registry optional name given to --tables is an optional table: it runs through OptionalTableRunner below,
        // with its column policy, schema gate and chunk guard, and lands in the optional manifest. Only core names run
        // through the core exporters here and land in the core manifest.
        var (coreTables, optionalNames) = SplitTables(options);

        var tableEntries = new List<ManifestTableEntry>();

        // one run-scoped Query Store raw-window cache, built at most once, lazily, by
        // whichever of query_store_stats/plan_shape/query_text touches it first. Deleted in
        // `finally` below regardless of whether it was ever built.
        using var rawWindowCache = QueryStoreRawWindowCache.Create(connectionStringBuilder, options);

        // same pattern for query_stats -- one run-scoped raw-window cache, built at most
        // once, lazily, by whichever of query_stats_hourly/plan_shape/query_text (origin
        // query_stats or both) touches it first.
        using var queryStatsWindowCache = QueryStatsWindowCache.Create(connectionStringBuilder, options);

        // same pattern for procedure_stats -- one run-scoped raw-window cache, built at
        // most once, lazily, by whichever of procedure_stats_hourly/procedure_plan touches it
        // first.
        using var procedureStatsWindowCache = ProcedureStatsWindowCache.Create(connectionStringBuilder, options);

        // Every requested system_health table is produced by one pass over the store, so they
        // are pulled out of the per-table loop below and handled once, up front, in whatever
        // order they were requested.
        var requestedSystemHealthTables = coreTables.Where(SystemHealthTables.All.Contains).ToArray();
        if (requestedSystemHealthTables.Length > 0)
        {
            output.WriteLine($"Exporting {string.Join(", ", requestedSystemHealthTables)}...");
            var systemHealthEntries = await SystemHealthExporter.ExportAsync(
                connectionStringBuilder, options, requestedSystemHealthTables, ct).ConfigureAwait(false);
            foreach (var entry in systemHealthEntries)
            {
                tableEntries.Add(entry);
                output.WriteLine($"  {entry.RowCount} rows, {entry.ByteSize} bytes -> {entry.OutputFile}");
            }
        }

        foreach (var table in coreTables)
        {
            if (SystemHealthTables.All.Contains(table))
            {
                continue;
            }

            output.WriteLine($"Exporting {table}...");

            if (table == DimensionTables.PlanShape)
            {
                var planShapeEntries = await DimensionTableExporter.ExportPlanShapeAsync(connectionStringBuilder, rawWindowCache, queryStatsWindowCache, options, ct).ConfigureAwait(false);
                tableEntries.AddRange(planShapeEntries);
                foreach (var planShapeEntry in planShapeEntries)
                {
                    output.WriteLine($"  {planShapeEntry.RowCount} rows, {planShapeEntry.ByteSize} bytes -> {planShapeEntry.OutputFile}");
                }
                continue;
            }

            if (table == DimensionTables.ProcedurePlan)
            {
                var procedurePlanEntries = await ProcedurePlanExporter.ExportAsync(connectionStringBuilder, procedureStatsWindowCache, options, ct).ConfigureAwait(false);
                tableEntries.AddRange(procedurePlanEntries);
                foreach (var procedurePlanEntry in procedurePlanEntries)
                {
                    output.WriteLine($"  {procedurePlanEntry.RowCount} rows, {procedurePlanEntry.ByteSize} bytes -> {procedurePlanEntry.OutputFile}");
                }
                continue;
            }

            ManifestTableEntry entry;
            if (table == DimensionTables.QueryText)
            {
                entry = await DimensionTableExporter.ExportQueryTextAsync(connectionStringBuilder, rawWindowCache, queryStatsWindowCache, options, ct).ConfigureAwait(false);
            }
            else if (table == QueryStoreTables.QueryStoreStats)
            {
                entry = await QueryStoreStatsExporter.ExportAsync(rawWindowCache, options, ct).ConfigureAwait(false);
            }
            else if (table == HourlyTables.QueryStatsHourly)
            {
                entry = await QueryStatsHourlyExporter.ExportAsync(queryStatsWindowCache, options, ct).ConfigureAwait(false);
            }
            else if (table == HourlyTables.ProcedureStatsHourly)
            {
                entry = await ProcedureStatsHourlyExporter.ExportAsync(procedureStatsWindowCache, options, ct).ConfigureAwait(false);
            }
            else
            {
                List<string>? rawColumns = null;
                if (!HourlyTables.All.Contains(table) && !QueryStoreTables.All.Contains(table))
                {
                    await using var discoveryConnection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
                    rawColumns = await RawColumnDiscovery.DiscoverAsync(discoveryConnection, table, ct).ConfigureAwait(false);
                }

                entry = await TableExporter.ExportAsync(connectionStringBuilder, options, table, rawColumns, ct).ConfigureAwait(false);
            }

            tableEntries.Add(entry);

            output.WriteLine($"  {entry.RowCount} rows, {entry.ByteSize} bytes -> {entry.OutputFile}");
        }

        // A --tables list of optional names only has no core export, so no core manifest: the optional manifest below
        // is the run's record.
        if (coreTables.Length > 0)
        {
            // Core tables get their time_column/kind/key_columns from the loader-mirrored map, not from the optional
            // registry -- every entry above came from a core exporter.
            var coreTableEntries = tableEntries.Select(CoreTableMetadata.WithMetadata).ToList();

            var manifest = new Manifest
            {
                Store = options.Store,
                FromUtc = options.FromUtc,
                ToUtc = options.ToUtc,
                ExporterVersion = ExporterVersion.Current,
                DarlingSchemaVersion = darlingSchemaVersion,
                Tables = coreTableEntries,
                StragglerWarning = stragglerResult.LastCount,
                StragglerWaitSeconds = stragglerResult.WaitSeconds,
            };

            // The core manifest is written -- and uploaded, if requested -- before any optional table starts. A broken
            // optional table can never block or corrupt the core export's own record of success.
            var manifestPath = OutputPaths.ManifestFilePath(options.OutDir, options.Store, options.FromUtc, options.ToUtc, coreTables, options.DimensionOrigin);
            await ManifestWriter.WriteAsync(manifest, manifestPath, ct).ConfigureAwait(false);
            output.WriteLine($"Manifest written to {manifestPath}");

            if (options.Upload is not null)
            {
                var settings = UploadSettings.FromUri(options.Upload, options.AwsRegion);
                await using var uploader = UploaderFactory.Create(settings);
                await RunUpload.UploadRunAsync(uploader, settings.Layout, options, manifest, manifestPath, output, ct).ConfigureAwait(false);
                output.WriteLine($"Uploaded to {settings.Display}");
            }

            // Every table the run touched has now succeeded (and, if --upload was given, the
            // upload has too): only now is it safe to record this run's dimension keys as
            // exported, so a run that fails partway through records nothing and a later retry
            // still fetches the content it did not get to write.
            RecordExportedDimensionKeys(options);
        }

        if (optionalNames.Length == 0)
        {
            return 0;
        }

        return await RunOptionalTablesAsync(connectionStringBuilder, options, optionalNames, output, darlingSchemaVersion, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Splits a run's table set into the core names (<see cref="ExportOptions.AllTables"/>) and the optional names:
    /// every `--also` name plus every registry name given to `--tables`. A registry name in `--tables` is routed
    /// exactly like an `--also` name, so its column policy, schema gate and chunk guard apply.
    /// </summary>
    public static (string[] CoreTables, string[] OptionalNames) SplitTables(ExportOptions options)
    {
        var optionalNames = OptionalNames(options.Tables, options.Also);
        var coreTables = options.Tables.Where(t => !optionalNames.Contains(t)).ToArray();
        return (coreTables, optionalNames);
    }

    /// <summary>
    /// The optional names of a call: every `--also` name, then every optional-registry name in `--tables`, each once.
    /// The optional manifest is named by this set (<see cref="OutputPaths.ManifestFilePath(string, string, DateTime,
    /// DateTime, IReadOnlyCollection{string}, string?)"/>), so the scheduler builds the same set from the call's
    /// arguments to find that manifest after an exit 3.
    /// </summary>
    public static string[] OptionalNames(IEnumerable<string> tables, IEnumerable<string> also)
    {
        var optionalInTables = tables.Where(t => !ExportOptions.AllTables.Contains(t) && OptionalTables.TryGet(t) is not null);
        return also.Concat(optionalInTables).Distinct(StringComparer.Ordinal).ToArray();
    }

    /// <summary>
    /// Runs every optional table (<paramref name="optionalNames"/>: the `--also` names plus the registry names given
    /// to `--tables`), one at a time in registry order, after the core manifest already exists. Writes the separate
    /// optional manifest (never colliding with the core manifest's name -- see <see cref="OutputPaths.TableSetTag"/>)
    /// and returns exit code 3 when at least one requested optional table failed, or 0 when every optional table
    /// either succeeded or was cleanly skipped.
    /// </summary>
    private static async Task<int> RunOptionalTablesAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, ExportOptions options, string[] optionalNames, TextWriter output, int? darlingSchemaVersion, CancellationToken ct)
    {
        var (known, unknown) = OptionalTableRunner.Resolve(optionalNames, OptionalTables.TryGet);
        var runResult = await OptionalTableRunner.RunAsync(connectionStringBuilder, options, known, unknown, output, ct, darlingSchemaVersion).ConfigureAwait(false);
        await PublishOptionalRunAsync(options, runResult, output, ct, optionalNames: optionalNames).ConfigureAwait(false);
        return runResult.Failed.Count > 0 || runResult.DeniedCount > 0 ? 3 : 0;
    }

    /// <summary>
    /// Writes the optional manifest, uploads the run when `--upload` is set and at least one table produced a file, and
    /// only then records the run's new `pg_statement_text` keys. The keys follow the upload, as the core path's dimension
    /// keys do: a failed upload records nothing, so the next run fetches that text again instead of skipping it as known.
    /// <paramref name="createUploader"/> exists for tests; the default is <see cref="UploaderFactory.Create"/>.
    /// <paramref name="optionalNames"/> names the manifest: the run's optional names, which default to `--also` alone.
    /// </summary>
    public static async Task PublishOptionalRunAsync(
        ExportOptions options, OptionalRunResult runResult, TextWriter output, CancellationToken ct,
        Func<UploadSettings, IUploader>? createUploader = null, IReadOnlyCollection<string>? optionalNames = null)
    {
        var optionalManifest = new Manifest
        {
            Store = options.Store,
            FromUtc = options.FromUtc,
            ToUtc = options.ToUtc,
            ExporterVersion = ExporterVersion.Current,
            DarlingSchemaVersion = null,
            Tables = runResult.Entries,
            SkippedOptional = runResult.Skipped.Count > 0 ? runResult.Skipped : null,
            FailedOptional = runResult.Failed.Count > 0 ? runResult.Failed : null,
        };

        var optionalManifestPath = OutputPaths.ManifestFilePath(
            options.OutDir, options.Store, options.FromUtc, options.ToUtc, optionalNames ?? options.Also, dimensionOrigin: null);
        await ManifestWriter.WriteAsync(optionalManifest, optionalManifestPath, ct).ConfigureAwait(false);
        output.WriteLine($"Optional manifest written to {optionalManifestPath}");

        if (options.Upload is not null && runResult.Entries.Count > 0)
        {
            var settings = UploadSettings.FromUri(options.Upload, options.AwsRegion);
            await using var uploader = createUploader is null ? UploaderFactory.Create(settings) : createUploader(settings);
            await RunUpload.UploadRunAsync(uploader, settings.Layout, options, optionalManifest, optionalManifestPath, output, ct).ConfigureAwait(false);
        }

        if (runResult.PgTextKeys is { Count: > 0 } pgTextKeys)
        {
            using var stateStore = StateStore.Open(options.StateDir);
            stateStore.RecordPgTextKeys(options.Store, pgTextKeys);
        }
    }

    private static void RecordExportedDimensionKeys(ExportOptions options)
    {
        var hasPlanKeys = PendingKeyRecorder.PendingPlanKeys.TryGetValue(options.Store, out var planKeys);
        var hasTextKeys = PendingKeyRecorder.PendingTextKeys.TryGetValue(options.Store, out var textKeys);
        var hasFeatureDigests = PendingKeyRecorder.PendingFeatureDigests.TryGetValue(options.Store, out var featureDigests);
        var hasProcedurePlanKeys = PendingKeyRecorder.PendingProcedurePlanKeys.TryGetValue(options.Store, out var procedurePlanKeys);
        var hasProcedureDigests = PendingKeyRecorder.PendingProcedureDigests.TryGetValue(options.Store, out var procedureDigests);
        var hasProcedureIdentities = PendingProcedurePlanIdentities.Has(options.Store);
        if (!hasPlanKeys && !hasTextKeys && !hasFeatureDigests && !hasProcedurePlanKeys && !hasProcedureDigests && !hasProcedureIdentities)
        {
            return;
        }

        using var stateStore = StateStore.Open(options.StateDir);
        stateStore.RecordExportedKeys(
            options.Store,
            planKeys ?? [],
            textKeys ?? [],
            featureDigests ?? [],
            procedurePlanKeys ?? [],
            procedureDigests ?? [],
            newProcedurePlanIdentities: PendingProcedurePlanIdentities.Take(options.Store));

        PendingKeyRecorder.PendingPlanKeys.Remove(options.Store);
        PendingKeyRecorder.PendingTextKeys.Remove(options.Store);
        PendingKeyRecorder.PendingFeatureDigests.Remove(options.Store);
        PendingKeyRecorder.PendingProcedurePlanKeys.Remove(options.Store);
        PendingKeyRecorder.PendingProcedureDigests.Remove(options.Store);
    }

    private static async Task<int> RunDryRunAsync(ExportOptions options, TextWriter output, CancellationToken ct)
    {
        NpgsqlConnectionStringBuilder? connectionStringBuilder = null;
        var (coreTables, optionalNames) = SplitTables(options);

        foreach (var table in coreTables)
        {
            output.WriteLine($"-- {table}");

            if (HourlyTables.All.Contains(table) || DimensionTables.All.Contains(table) || QueryStoreTables.All.Contains(table))
            {
                output.WriteLine(SqlBuilder.BuildSelect(table, rawColumns: null));
                continue;
            }

            if (SystemHealthTables.All.Contains(table))
            {
                output.WriteLine(SystemHealthExporter.WindowSql);
                continue;
            }

            // Raw tables need a column list, which requires connecting once to discover it
            // (dry-run may connect to discover columns; it just never reads data rows).
            connectionStringBuilder ??= StoreConnectionFactory.Build(options);

            await using var connection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
            var rawColumns = await RawColumnDiscovery.DiscoverAsync(connection, table, ct).ConfigureAwait(false);
            output.WriteLine(SqlBuilder.BuildSelect(table, rawColumns));
        }

        // --dry-run prints every optional table's SQL too (--also names and registry names given to --tables), with
        // the table's column policy applied to its discovered columns, exactly as the optional runner sends it.
        foreach (var name in optionalNames)
        {
            output.WriteLine($"-- {name} (optional)");
            var table = OptionalTables.TryGet(name);
            if (table is null)
            {
                output.WriteLine($"-- unknown optional table '{name}'");
                continue;
            }

            connectionStringBuilder ??= StoreConnectionFactory.Build(options);
            await using var optionalConnection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
            await using var command = new NpgsqlCommand(SqlBuilder.OptionalColumnDiscoverySql, optionalConnection);
            command.Parameters.AddWithValue("schema", table.Schema);
            command.Parameters.AddWithValue("table", table.Name);
            var columns = new List<DiscoveredColumn>();
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                columns.Add(new DiscoveredColumn(reader.GetString(0), reader.GetString(1), reader.GetString(2)));
            }

            if (columns.Count == 0)
            {
                output.WriteLine($"-- '{name}' has no exportable columns (absent, or every column excluded)");
                continue;
            }

            output.WriteLine(SqlBuilder.BuildOptionalSelect(table, columns, ColumnPolicies.For(table.Name)));
        }

        return 0;
    }

    private static async Task<int?> ReadDarlingSchemaVersionAsync(NpgsqlConnectionStringBuilder connectionStringBuilder, CancellationToken ct)
    {
        await using var connection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand("/* darling-export:schema_version */ SELECT max(version) FROM collect.darling_schema_version", connection);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is null or DBNull ? null : Convert.ToInt32(result);
    }

    /// <summary>
    /// Every requested core hypertable's compressed-chunk check, run BEFORE any store read of a table's data. One
    /// tagged catalog query per table -- <see cref="StoreCapabilities.CompressedChunksInWindowAsync"/> -- against
    /// `timescaledb_information.chunks`, on one shared connection. If any core table has a compressed chunk in its
    /// scan window and `--allow-compressed` was not set, this throws a single exception naming every offending (table,
    /// chunk) pair, and the caller never reads a row, writes a Parquet file, or writes a manifest. `query_store_stats`
    /// (and plan_shape/query_text when their dimension origin reads Query Store) checks `to + 2h`, the same
    /// late-snapshot margin <see cref="QueryStoreWindow.BuildRawWindowFileAsync"/> applies to its own read.
    /// System_health tables and dimension-only reads that don't touch a raw hypertable (query_text/plan_shape under a
    /// query_stats-only origin) have no entry in <see cref="CoreGuardTargets"/> and are skipped, same as today.
    /// </summary>
    private static async Task RunChunkGuardPreflightAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, ExportOptions options, CancellationToken ct)
    {
        var targets = CoreGuardTargets(options);
        if (targets.Count == 0)
        {
            return;
        }

        await using var connection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);

        var offenders = new List<(string Table, string ChunkName)>();
        foreach (var (table, scanTo) in targets)
        {
            var chunks = await StoreCapabilities.CompressedChunksInWindowAsync(
                connection, "collect", table, options.FromUtc, scanTo, ct).ConfigureAwait(false);
            foreach (var compressed in chunks.Where(c => c.IsCompressed))
            {
                offenders.Add((table, compressed.ChunkName));
            }
        }

        if (offenders.Count == 0 || options.AllowCompressed)
        {
            return;
        }

        throw new StoreCapabilities.CompressedChunkException(offenders);
    }

    /// <summary>
    /// The (table, scanTo) pairs the pre-flight guard checks for this run: every requested raw table (<see
    /// cref="RawTables"/>), `query_stats`/`procedure_stats` when their hourly rollup or the corresponding
    /// dimension origin is requested, `query_store_stats` when it or a Query Store-reading dimension origin
    /// is requested, and `system_health_events` when any <see cref="SystemHealthTables"/> table is requested
    /// -- each with `to`, except `query_store_stats` which uses `to + 2h` (the same margin
    /// <see cref="QueryStoreWindow.BuildRawWindowFileAsync"/> reads).
    /// </summary>
    private static List<(string Table, DateTime ScanTo)> CoreGuardTargets(ExportOptions options)
    {
        var targets = new List<(string Table, DateTime ScanTo)>();

        foreach (var table in options.Tables)
        {
            if (RawTables.All.Contains(table))
            {
                targets.Add((table, options.ToUtc));
            }
        }

        var dimensionReadsQueryStats = options.DimensionOrigin is DimensionOrigins.QueryStats or DimensionOrigins.Both;
        var dimensionReadsQueryStore = options.DimensionOrigin is DimensionOrigins.QueryStore or DimensionOrigins.Both;
        var touchesDimension = options.Tables.Contains(DimensionTables.PlanShape) || options.Tables.Contains(DimensionTables.QueryText);

        if (options.Tables.Contains(HourlyTables.QueryStatsHourly) || (touchesDimension && dimensionReadsQueryStats))
        {
            targets.Add(("query_stats", options.ToUtc));
        }

        if (options.Tables.Contains(HourlyTables.ProcedureStatsHourly) || options.Tables.Contains(DimensionTables.ProcedurePlan))
        {
            targets.Add(("procedure_stats", options.ToUtc));
        }

        if (options.Tables.Contains(QueryStoreTables.QueryStoreStats) || (touchesDimension && dimensionReadsQueryStore))
        {
            targets.Add((QueryStoreTables.QueryStoreStats, options.QueryStoreCollectionToUtc));
        }

        if (SystemHealthTables.All.Any(options.Tables.Contains))
        {
            targets.Add(("system_health_events", options.ToUtc));
        }

        return targets;
    }

    /// <summary>
    /// Runs the straggler guard once for this run's window, before any table is read. `@collection_to` is `--to` for a
    /// fresh call, or `--to` plus 2 hours when the table set touches Query Store (query_store_stats, or
    /// plan_shape/query_text under a Query Store-reading origin) -- the same cutoff the Query Store read itself trusts
    /// for a complete snapshot. This never throws past the guard's own retries; a straggler is recorded and the run
    /// proceeds either way.
    /// </summary>
    private static async Task<StragglerGuardResult> RunStragglerGuardAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, ExportOptions options, TextWriter output, CancellationToken ct)
    {
        var dimensionReadsQueryStore = options.DimensionOrigin is DimensionOrigins.QueryStore or DimensionOrigins.Both;
        var touchesQueryStore = options.Tables.Contains(QueryStoreTables.QueryStoreStats)
            || ((options.Tables.Contains(DimensionTables.PlanShape) || options.Tables.Contains(DimensionTables.QueryText)) && dimensionReadsQueryStore);
        // The guard asks about transactions older than this instant; never ask about the future (a lag-1 or lag-2 run
        // happens before the read's generous upper bound, and every open transaction would count).
        var collectionTo = touchesQueryStore ? DateTime.SpecifyKind(
            options.QueryStoreCollectionToUtc < DateTime.UtcNow ? options.QueryStoreCollectionToUtc : DateTime.UtcNow, DateTimeKind.Utc) : options.ToUtc;

        var testWaitMs = Environment.GetEnvironmentVariable("DARLING_EXPORT_TEST_STRAGGLER_WAIT_MS");
        Func<TimeSpan, CancellationToken, Task>? waitAsync = null;
        if (int.TryParse(testWaitMs, out var waitMs))
        {
            // Test-only shortcut (never set in production): swaps StragglerGuard's real 60s
            // retry delay for a short one, so the harness's 5-try retry loop doesn't cost
            // 5 real minutes. The retry COUNT and the manifest's recorded values are unaffected.
            waitAsync = (_, token) => Task.Delay(TimeSpan.FromMilliseconds(waitMs), token);
        }

        var result = waitAsync is null
            ? await StragglerGuard.CheckAsync(connectionStringBuilder, collectionTo, ct).ConfigureAwait(false)
            : await StragglerGuard.CheckAsync(
                innerCt => StragglerGuard.CountStragglersAsync(connectionStringBuilder, collectionTo, innerCt),
                ct,
                waitAsync).ConfigureAwait(false);

        if (result.LastCount > 0)
        {
            output.WriteLine($"Straggler guard: {result.LastCount} open writing transaction(s) started before {collectionTo:O}; waited {result.WaitSeconds}s, proceeding.");
        }

        return result;
    }
}

/// <summary>The exporter's own version string, from the assembly's informational version.</summary>
public static class ExporterVersion
{
    public static readonly string Current =
        Assembly.GetExecutingAssembly()
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
        ?? "unknown";
}
