using System.Diagnostics;
using System.Security.Cryptography;
using DuckDB.NET.Data;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Exports the two content dimension tables (`plan_shape`, `query_text`). Each is a union of
/// two key sources: query_stats (part B's original behavior, digest already known) and Query
/// Store (added alongside query_store_stats support: a plan/text key here resolves through
/// query_store_plan_map / query_store_text instead of query_stats' own digest columns). When
/// both sources produce the same key, the query_stats row wins; the surviving row's `origin`
/// column records which source it came from. Content for the two sources is fetched in bounded
/// batches (500 keys/digests at a time), so at most one batch's worth of plan XML or query text
/// is ever held in memory: a raw window can have tens of millions of query_stats rows and
/// hundreds of thousands of Query Store rows, but only a small number of distinct keys.
/// Keys already recorded as exported in the local state store (see StateStore) are dropped
/// before any content lookup runs, so a re-run of an overlapping window does not re-fetch and
/// re-write content it already wrote once.
/// </summary>
public static class DimensionTableExporter
{
    private const int DigestBatchSize = 250;

    private sealed record PlanKey(string? ServerName, string QueryPlanHash, DateTime FirstSeen, DateTime LastSeen, string Origin)
    {
        // Populated differently per origin: query_stats keys already know their digest;
        // Query Store keys resolve it through plan_map before content lookup runs.
        public byte[]? ExampleDigest { get; set; }
        public bool ContentWritten { get; set; }
        public int? ServerId { get; init; }
        public string? ExampleDatabaseName { get; init; }
        public long? ExamplePlanId { get; init; }

        // This window's totals for the key, read by the plan-fetch gate (see PlanFetchGate). Unused once the gate has run.
        public long Executions { get; set; }
        public long CpuUs { get; set; }
    }

    private sealed record TextKey(string? ServerName, string? DatabaseName, string? QueryHash, DateTime FirstSeen, DateTime LastSeen, string Origin)
    {
        public byte[]? ExampleDigest { get; set; }
        public bool ContentWritten { get; set; }
        public int? ServerId { get; init; }
        public long? ExampleQueryId { get; init; }
        // Query Store text has no digest table; its content is resolved directly by
        // (server_id, database_name, query_id) and cached here once resolved.
        public string? QueryStoreText { get; set; }
        public bool QueryStoreTextResolved { get; set; }
    }

    // Key columns other than the hash (which the key queries filter to NOT NULL) can be NULL in the store: a
    // query with no database context has a NULL database_name. NULL is a key value of its own, not an error.
    private static string? NullableString(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    public static Task<IReadOnlyList<ManifestTableEntry>> ExportPlanShapeAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, QueryStoreRawWindowCache rawWindowCache,
        QueryStatsWindowCache queryStatsWindowCache, ExportOptions options, CancellationToken ct) =>
        ExportPlanShapeCoreAsync(connectionStringBuilder, rawWindowCache, queryStatsWindowCache, options, ct);

    public static Task<ManifestTableEntry> ExportQueryTextAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, QueryStoreRawWindowCache rawWindowCache,
        QueryStatsWindowCache queryStatsWindowCache, ExportOptions options, CancellationToken ct) =>
        ExportQueryTextCoreAsync(connectionStringBuilder, rawWindowCache, queryStatsWindowCache, options, ct);

    private static async Task<IReadOnlyList<ManifestTableEntry>> ExportPlanShapeCoreAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, QueryStoreRawWindowCache rawWindowCache,
        QueryStatsWindowCache queryStatsWindowCache, ExportOptions options, CancellationToken ct)
    {
        const string table = DimensionTables.PlanShape;
        var part = DimensionOrigins.PartFor(options.DimensionOrigin);
        var outputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, table, options.FromUtc, options.ToUtc, part);
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);
        var tempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-{Guid.NewGuid():N}.duckdb");

        var featuresTempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-{Guid.NewGuid():N}.duckdb");
        var warningsTempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-{Guid.NewGuid():N}.duckdb");
        var missingIndexesTempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-{Guid.NewGuid():N}.duckdb");

        try
        {
            var storeStopwatch = Stopwatch.StartNew();

            var pacer = new ContentFetchPacer(allowCompressed: options.AllowCompressed);
            var timings = new ExportTimings(pacer);

            await using var pgConnection = await timings.OpenConnectionAsync(connectionStringBuilder, ct).ConfigureAwait(false);

            var byKey = new Dictionary<(string?, string), PlanKey>();
            var readQueryStats = options.DimensionOrigin is DimensionOrigins.QueryStats or DimensionOrigins.Both;
            var readQueryStore = options.DimensionOrigin is DimensionOrigins.QueryStore or DimensionOrigins.Both;

            if (readQueryStats)
            {
                await ReadPlanShapeKeysFromQueryStatsAsync(queryStatsWindowCache, timings, ct, reader =>
                {
                    var key = new PlanKey(NullableDuckString(reader, 0), reader.GetString(1), reader.GetDateTime(2), reader.GetDateTime(3), DimensionOrigin.QueryStats)
                    {
                        ExampleDigest = ReadDuckBlob(reader, 4),
                        Executions = reader.GetInt64(5),
                        CpuUs = reader.GetInt64(6),
                    };
                    byKey[(key.ServerName, key.QueryPlanHash)] = key; // query_stats always wins on a collision, so it's safe to just overwrite here first
                }).ConfigureAwait(false);
            }

            var hasQueryStore = readQueryStore && await timings.StoreReadAsync(() => StoreCapabilities.HasQueryStoreStatsAsync(pgConnection, ct)).ConfigureAwait(false);
            if (hasQueryStore)
            {
                await ReadQueryStorePlanKeysAsync(rawWindowCache, timings, ct, reader =>
                {
                    var serverName = NullableDuckString(reader, 1);
                    var planHash = reader.GetString(2);
                    var k = (serverName, planHash);
                    if (byKey.TryGetValue(k, out var existing))
                    {
                        // query_stats already has this key; it wins. Two Query Store rows for the same key (one per
                        // server_id) are the same plan on one server, so the later row adds its totals to the first.
                        if (existing.Origin == DimensionOrigin.QueryStore)
                        {
                            existing.Executions += reader.GetInt64(7);
                            existing.CpuUs += reader.GetInt64(8);
                        }

                        return;
                    }

                    byKey[k] = new PlanKey(serverName, planHash, reader.GetDateTime(3), reader.GetDateTime(4), DimensionOrigin.QueryStore)
                    {
                        ServerId = reader.GetInt32(0),
                        ExampleDatabaseName = NullableDuckString(reader, 5),
                        ExamplePlanId = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                        Executions = reader.GetInt64(7),
                        CpuUs = reader.GetInt64(8),
                    };
                }).ConfigureAwait(false);
            }

            var mergedCount = byKey.Count;
            using var stateStore = StateStore.Open(options.StateDir);
            var survivingKeys = options.IgnoreState
                ? byKey.Keys.ToHashSet()
                : stateStore.FilterUnknownPlanKeys(options.Store, byKey.Keys)!;
            var knownKeysSkipped = mergedCount - survivingKeys.Count;

            var keys = survivingKeys.Select(k => byKey[k]).ToList();

            // Plan-fetch gate: only keys that have run enough go on to the store reads below. --ignore-state
            // (operator re-export) and --plan-fetch-all fetch everything, as before the gate existed.
            var gateDecisions = new List<PlanFetchDecision>();
            long belowThresholdDeferred = 0;
            long deferredNowFetched = 0;
            var gateEnabled = PlanFetchGate.IsEnabled(options);
            if (gateEnabled)
            {
                var gated = PlanFetchGate.Apply(
                    stateStore, options.Store, DateTime.UtcNow,
                    keys.Select(k => new PlanFetchCandidate(k.ServerName, k.QueryPlanHash, k.Origin, k.Executions, k.CpuUs, k.FirstSeen, k.LastSeen)));
                gateDecisions = gated;
                var byDecisionKey = keys.ToDictionary(k => (k.ServerName, k.QueryPlanHash));
                keys = [];
                foreach (var decision in gated)
                {
                    if (!decision.Qualifies)
                    {
                        belowThresholdDeferred++;
                        continue;
                    }

                    if (decision.WasPending)
                    {
                        deferredNowFetched++;
                    }

                    // The row carries the accumulated first_seen / last_seen across every window the key was held back in.
                    keys.Add(byDecisionKey[(decision.ServerName, decision.QueryPlanHash)] with
                    {
                        FirstSeen = decision.FirstSeen,
                        LastSeen = decision.LastSeen,
                    });
                }

                // Deterministic order for the batches below, which sort by first_seen anyway.
                keys = [.. keys.OrderBy(k => k.ServerName, StringComparer.Ordinal).ThenBy(k => k.QueryPlanHash, StringComparer.Ordinal)];
            }

            // Query Store keys need their digest resolved via plan_map before the shared
            // digest-based content lookup below can run; query_stats keys already have theirs.
            var queryStoreKeysNeedingDigest = keys.Where(k => k.Origin == DimensionOrigin.QueryStore).ToList();
            await ResolvePlanMapDigestsAsync(pgConnection, queryStoreKeysNeedingDigest, pacer, ct, timings).ConfigureAwait(false);

            var keysByDigestHex = new Dictionary<string, List<PlanKey>>();
            var keysWithNoDigest = new List<PlanKey>();
            foreach (var key in keys)
            {
                if (key.ExampleDigest is null)
                {
                    keysWithNoDigest.Add(key);
                    continue;
                }

                var hex = DimensionContent.ToHexDigest(key.ExampleDigest);
                if (!keysByDigestHex.TryGetValue(hex, out var list))
                {
                    list = [];
                    keysByDigestHex[hex] = list;
                }
                list.Add(key);
            }

            // batches are ordered by the key's first_seen, so consecutive batches touch heap/TOAST
            // pages nearby the ones just captured. A digest can be shared by several keys; it sorts by the earliest
            // first_seen among them, with the hex value itself as a deterministic tie-break.
            var distinctDigests = keysByDigestHex
                .OrderBy(kv => kv.Value.Min(k => k.FirstSeen))
                .ThenBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => Convert.FromHexString(kv.Key))
                .ToList();

            var columns = new List<ColumnShape>
            {
                new("source_store", "VARCHAR"),
                new("server_name", "VARCHAR"),
                new("query_plan_hash", "VARCHAR"),
                new("first_seen", "TIMESTAMP"),
                new("last_seen", "TIMESTAMP"),
                new("example_digest", "VARCHAR"),
                new("plan_xml", "VARCHAR"),
                new("plan_xml_bytes", "BIGINT"),
                new("origin", "VARCHAR"),
            };

            long rowCount = 0;
            long missingContentCount = 0;

            var featureColumns = FeatureColumns();
            var warningColumns = WarningColumns();
            var missingIndexColumns = MissingIndexColumns();

            var featureVersion = PlanFeatureExtractor.FeatureVersion;
            var digestsInThisBatch = distinctDigests.Select(DimensionContent.ToHexDigest).ToHashSet(StringComparer.Ordinal);
            var unknownDigests = options.IgnoreState
                ? digestsInThisBatch
                : stateStore.FilterUnknownFeatureDigests(options.Store, digestsInThisBatch, featureVersion);
            long featureDigestsSkipped = digestsInThisBatch.Count - unknownDigests.Count;
            long featureDigestsParsed = 0;
            var parsedFeatureDigests = new List<(string PlanDigest, int FeatureVersion)>();

            long featureRowCount = 0;
            long warningRowCount = 0;
            long missingIndexRowCount = 0;

            var writeStopwatch = new Stopwatch();
            using (var writer = new ChunkedParquetWriter(tempDbPath, table, columns))
            using (var featuresWriter = new ChunkedParquetWriter(featuresTempDbPath, PlanFeatureTables.PlanFeatures, featureColumns))
            using (var warningsWriter = new ChunkedParquetWriter(warningsTempDbPath, PlanFeatureTables.PlanWarnings, warningColumns))
            using (var missingIndexesWriter = new ChunkedParquetWriter(missingIndexesTempDbPath, PlanFeatureTables.PlanMissingIndexes, missingIndexColumns))
            {
                foreach (var batch in DimensionContent.Batch(distinctDigests, DigestBatchSize))
                {
                    var contentByDigest = await timings.PacedContentFetchAsync(pacer, () => FetchPlanContentAsync(pgConnection, batch, ct), ct).ConfigureAwait(false);
                    foreach (var digest in batch)
                    {
                        var hex = DimensionContent.ToHexDigest(digest);
                        var (xml, gz) = contentByDigest.TryGetValue(hex, out var found) ? found : (null, null);
                        var content = timings.Parse(() => DimensionContent.ResolvePlanXml(xml, gz)); // resolved once per digest, not per key

                        foreach (var key in keysByDigestHex[hex])
                        {
                            if (AppendPlanRow(writer.Appender, options.Store, key, hex, content, ref missingContentCount))
                            {
                                writer.RowAppended(System.Text.Encoding.UTF8.GetByteCount(content!));
                                rowCount++;
                            }
                        }

                        // Features are keyed by plan content, so they get parsed once per distinct digest, whether
                        // that digest resolved to real content or an empty Query Store shell (a shell
                        // still gets its one no_statements feature row, because features are keyed by content, not
                        // by whether plan_shape wrote a row for it).
                        // A digest with no content in the store (its query_plan_dim row was purged, or never written) has
                        // nothing to parse: it gets no feature row and is not recorded, so it is not mistaken for a plan
                        // that failed to parse.
                        if (content is not null && unknownDigests.Contains(hex))
                        {
                            var result = timings.Parse(() => PlanFeatureExtractor.Extract(hex, content));
                            foreach (var feature in result.Features)
                            {
                                AppendFeatureRow(featuresWriter.Appender, options.Store, options.FromUtc, feature);
                                featuresWriter.RowAppended();
                                featureRowCount++;
                            }
                            foreach (var warning in result.Warnings)
                            {
                                AppendWarningRow(warningsWriter.Appender, options.Store, warning);
                                warningsWriter.RowAppended();
                                warningRowCount++;
                            }
                            foreach (var missingIndex in result.MissingIndexes)
                            {
                                AppendMissingIndexRow(missingIndexesWriter.Appender, options.Store, missingIndex);
                                missingIndexesWriter.RowAppended();
                                missingIndexRowCount++;
                            }

                            featureDigestsParsed++;
                            parsedFeatureDigests.Add((hex, featureVersion));
                        }
                    }
                }

                // A Query Store plan with no plan_map row has no content to write yet.
                missingContentCount += keysWithNoDigest.Count;

                storeStopwatch.Stop();
                writeStopwatch.Start();
                writer.Complete(outputPath);
                var featuresOutputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, PlanFeatureTables.PlanFeatures, options.FromUtc, options.ToUtc, part);
                var warningsOutputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, PlanFeatureTables.PlanWarnings, options.FromUtc, options.ToUtc, part);
                var missingIndexesOutputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, PlanFeatureTables.PlanMissingIndexes, options.FromUtc, options.ToUtc, part);
                Directory.CreateDirectory(Path.GetDirectoryName(featuresOutputPath)!);
                Directory.CreateDirectory(Path.GetDirectoryName(warningsOutputPath)!);
                Directory.CreateDirectory(Path.GetDirectoryName(missingIndexesOutputPath)!);
                featuresWriter.Complete(featuresOutputPath);
                warningsWriter.Complete(warningsOutputPath);
                missingIndexesWriter.Complete(missingIndexesOutputPath);
                writeStopwatch.Stop();

                var byteSize = new FileInfo(outputPath).Length;
                var sha256 = ComputeSha256(outputPath);

                var timingSnapshot = timings.Snapshot(includeParse: true, includeWindowCache: true);
                var entry = new ManifestTableEntry
                {
                    Table = table,
                    OutputFile = OutputPaths.RelativeParquetPath(options.Store, table, options.FromUtc, options.ToUtc, part),
                    RowCount = rowCount,
                    ByteSize = byteSize,
                    Sha256 = sha256,
                    Columns = [.. columns.Select(c => new ManifestColumn { Name = c.Name, DuckDbType = c.DuckDbType })],
                    StoreQueryElapsedMs = storeStopwatch.Elapsed.TotalMilliseconds,
                    WriteElapsedMs = writeStopwatch.Elapsed.TotalMilliseconds,
                    MissingContentCount = missingContentCount,
                    KnownKeysSkipped = knownKeysSkipped,
                    BelowThresholdDeferred = gateEnabled ? belowThresholdDeferred : null,
                    DeferredNowFetched = gateEnabled ? deferredNowFetched : null,
                    Timings = timingSnapshot,
                };

                var featuresEntry = BuildCompanionEntry(
                    options, PlanFeatureTables.PlanFeatures, featuresOutputPath, featureRowCount, featureColumns,
                    storeStopwatch, writeStopwatch, featureDigestsParsed, featureDigestsSkipped, part, timingSnapshot);
                var warningsEntry = BuildCompanionEntry(
                    options, PlanFeatureTables.PlanWarnings, warningsOutputPath, warningRowCount, warningColumns,
                    storeStopwatch, writeStopwatch, null, null, part, timingSnapshot);
                var missingIndexesEntry = BuildCompanionEntry(
                    options, PlanFeatureTables.PlanMissingIndexes, missingIndexesOutputPath, missingIndexRowCount, missingIndexColumns,
                    storeStopwatch, writeStopwatch, null, null, part, timingSnapshot);

                // Only keys whose content was written are remembered: a key with missing content (plan not captured yet,
                // or purged) is retried by a later run instead of being skipped forever.
                PendingKeyRecorder.PendingPlanKeys[options.Store] = [.. keys.Where(k => k.ContentWritten).Select(k => (k.ServerName, k.QueryPlanHash))];
                var writtenKeys = keys.Where(k => k.ContentWritten).Select(k => (k.ServerName, k.QueryPlanHash)).ToHashSet();
                if (gateEnabled)
                {
                    // Held-back keys, and qualified keys whose content was missing, keep their accumulated totals.
                    PendingKeyRecorder.PendingPlanFetchUpserts[options.Store] =
                    [
                        .. gateDecisions.Where(d => !d.Qualifies || !writtenKeys.Contains((d.ServerName, d.QueryPlanHash))),
                    ];
                }
                PendingKeyRecorder.AddPendingFeatureDigests(options.Store, parsedFeatureDigests);

                return new List<ManifestTableEntry> { entry, featuresEntry, warningsEntry, missingIndexesEntry };
            }
        }
        finally
        {
            DuckDbSession.DeleteTempDatabase(tempDbPath);
            DuckDbSession.DeleteTempDatabase(featuresTempDbPath);
            DuckDbSession.DeleteTempDatabase(warningsTempDbPath);
            DuckDbSession.DeleteTempDatabase(missingIndexesTempDbPath);
        }
    }

    /// <summary>
    /// Appends one plan_shape row, or counts the key as missing content and appends nothing. A row with no plan is not
    /// written, because the loader keeps the first row per key and a NULL row would block the real plan from a later
    /// run. An empty Query Store shell (content with no &lt;Stmt element at all) counts as missing content too: SQL
    /// Server's own empty capture must not block a real plan from landing under the same key later, even though
    /// features are still extracted for it below (features are keyed by content, not by whether plan_shape wrote a
    /// row).
    /// </summary>
    private static bool AppendPlanRow(DuckDBAppender appender, string store, PlanKey key, string? hex, string? content, ref long missingContentCount)
    {
        if (content is null || !content.Contains("<Stmt", StringComparison.Ordinal))
        {
            missingContentCount++;
            return false;
        }

        key.ContentWritten = true;

        var row = appender.CreateRow();
        row.AppendValue(store);
        row.AppendValue(key.ServerName);
        row.AppendValue(key.QueryPlanHash);
        row.AppendValue(key.FirstSeen);
        row.AppendValue(key.LastSeen);
        AppendNullableString(row, hex);
        AppendNullableString(row, content);
        row.AppendValue((long)System.Text.Encoding.UTF8.GetByteCount(content));
        row.AppendValue(key.Origin);
        row.EndRow();
        return true;
    }

    internal static List<ColumnShape> FeatureColumns() =>
    [
        new("source_store", "VARCHAR"),
        new("plan_digest", "VARCHAR"),
        new("statement_ordinal", "INTEGER"),
        new("feature_version", "INTEGER"),
        new("extraction_status", "VARCHAR"),
        new("query_plan_hash", "VARCHAR"),
        new("query_hash", "VARCHAR"),
        new("statement_type", "VARCHAR"),
        new("cardinality_estimation_model", "INTEGER"),
        new("compile_time_ms", "BIGINT"),
        new("compile_memory_kb", "BIGINT"),
        new("compile_cpu_ms", "BIGINT"),
        new("degree_of_parallelism", "INTEGER"),
        new("effective_dop", "INTEGER"),
        new("estimated_subtree_cost", "DOUBLE"),
        new("estimated_rows", "DOUBLE"),
        new("memory_grant_requested_kb", "BIGINT"),
        new("memory_grant_granted_kb", "BIGINT"),
        new("memory_grant_max_used_kb", "BIGINT"),
        new("memory_grant_wait_ms", "BIGINT"),
        new("retrieved_from_cache", "BOOLEAN"),
        new("non_parallel_reason", "VARCHAR"),
        new("cached_plan_size_kb", "BIGINT"),
        new("operator_count", "INTEGER"),
        new("physical_ops", "VARCHAR"),
        new("missing_index_count", "INTEGER"),
        new("warning_count", "INTEGER"),
        new("plan_xml_bytes", "BIGINT"),
        new("first_seen", "TIMESTAMP"),
    ];

    internal static List<ColumnShape> WarningColumns() =>
    [
        new("source_store", "VARCHAR"),
        new("plan_digest", "VARCHAR"),
        new("statement_ordinal", "INTEGER"),
        new("feature_version", "INTEGER"),
        new("warning_seq", "INTEGER"),
        new("warning_type", "VARCHAR"),
        new("severity", "VARCHAR"),
        new("message", "VARCHAR"),
        new("max_benefit_percent", "DOUBLE"),
        new("spill_type", "VARCHAR"),
        new("spill_granted_kb", "BIGINT"),
        new("spill_used_kb", "BIGINT"),
        new("spill_writes_to_temp_db", "BIGINT"),
        new("spill_reads_from_temp_db", "BIGINT"),
    ];

    internal static List<ColumnShape> MissingIndexColumns() =>
    [
        new("source_store", "VARCHAR"),
        new("plan_digest", "VARCHAR"),
        new("statement_ordinal", "INTEGER"),
        new("feature_version", "INTEGER"),
        new("index_seq", "INTEGER"),
        new("database_name", "VARCHAR"),
        new("schema_name", "VARCHAR"),
        new("table_name", "VARCHAR"),
        new("impact", "DOUBLE"),
        new("equality_columns", "VARCHAR"),
        new("inequality_columns", "VARCHAR"),
        new("include_columns", "VARCHAR"),
        new("create_statement", "VARCHAR"),
    ];

    internal static void AppendFeatureRow(DuckDBAppender appender, string store, DateTime firstSeen, PlanFeatureRow feature)
    {
        var row = appender.CreateRow();
        row.AppendValue(store);
        row.AppendValue(feature.PlanDigest);
        row.AppendValue(feature.StatementOrdinal);
        row.AppendValue(feature.FeatureVersion);
        row.AppendValue(feature.ExtractionStatus);
        AppendNullableString(row, feature.QueryPlanHash);
        AppendNullableString(row, feature.QueryHash);
        AppendNullableString(row, feature.StatementType);
        AppendNullableInt(row, feature.CardinalityEstimationModel);
        AppendNullableLong(row, feature.CompileTimeMs);
        AppendNullableLong(row, feature.CompileMemoryKb);
        AppendNullableLong(row, feature.CompileCpuMs);
        AppendNullableInt(row, feature.DegreeOfParallelism);
        AppendNullableInt(row, feature.EffectiveDop);
        AppendNullableDouble(row, feature.EstimatedSubtreeCost);
        AppendNullableDouble(row, feature.EstimatedRows);
        AppendNullableLong(row, feature.MemoryGrantRequestedKb);
        AppendNullableLong(row, feature.MemoryGrantGrantedKb);
        AppendNullableLong(row, feature.MemoryGrantMaxUsedKb);
        AppendNullableLong(row, feature.MemoryGrantWaitMs);
        AppendNullableBool(row, feature.RetrievedFromCache);
        AppendNullableString(row, feature.NonParallelReason);
        AppendNullableLong(row, feature.CachedPlanSizeKb);
        AppendNullableInt(row, feature.OperatorCount);
        AppendNullableString(row, feature.PhysicalOps);
        AppendNullableInt(row, feature.MissingIndexCount);
        AppendNullableInt(row, feature.WarningCount);
        row.AppendValue(feature.PlanXmlBytes);
        row.AppendValue(firstSeen);
        row.EndRow();
    }

    internal static void AppendWarningRow(DuckDBAppender appender, string store, PlanWarningRow warning)
    {
        var row = appender.CreateRow();
        row.AppendValue(store);
        row.AppendValue(warning.PlanDigest);
        row.AppendValue(warning.StatementOrdinal);
        row.AppendValue(warning.FeatureVersion);
        row.AppendValue(warning.WarningSeq);
        row.AppendValue(warning.WarningType);
        row.AppendValue(warning.Severity);
        row.AppendValue(warning.Message);
        AppendNullableDouble(row, warning.MaxBenefitPercent);
        AppendNullableString(row, warning.SpillType);
        AppendNullableLong(row, warning.SpillGrantedKb);
        AppendNullableLong(row, warning.SpillUsedKb);
        AppendNullableLong(row, warning.SpillWritesToTempDb);
        AppendNullableLong(row, warning.SpillReadsFromTempDb);
        row.EndRow();
    }

    internal static void AppendMissingIndexRow(DuckDBAppender appender, string store, PlanMissingIndexRow index)
    {
        var row = appender.CreateRow();
        row.AppendValue(store);
        row.AppendValue(index.PlanDigest);
        row.AppendValue(index.StatementOrdinal);
        row.AppendValue(index.FeatureVersion);
        row.AppendValue(index.IndexSeq);
        row.AppendValue(index.DatabaseName);
        row.AppendValue(index.SchemaName);
        row.AppendValue(index.TableName);
        row.AppendValue(index.Impact);
        AppendNullableString(row, index.EqualityColumns);
        AppendNullableString(row, index.InequalityColumns);
        AppendNullableString(row, index.IncludeColumns);
        row.AppendValue(index.CreateStatement);
        row.EndRow();
    }

    private static void AppendNullableInt(IDuckDBAppenderRow row, int? value)
    {
        if (value.HasValue) row.AppendValue(value.Value); else row.AppendNullValue();
    }

    private static void AppendNullableLong(IDuckDBAppenderRow row, long? value)
    {
        if (value.HasValue) row.AppendValue(value.Value); else row.AppendNullValue();
    }

    private static void AppendNullableDouble(IDuckDBAppenderRow row, double? value)
    {
        if (value.HasValue) row.AppendValue(value.Value); else row.AppendNullValue();
    }

    private static void AppendNullableBool(IDuckDBAppenderRow row, bool? value)
    {
        if (value.HasValue) row.AppendValue(value.Value); else row.AppendNullValue();
    }

    internal static ManifestTableEntry BuildCompanionEntry(
        ExportOptions options, string table, string outputPath, long rowCount, List<ColumnShape> columns,
        Stopwatch storeStopwatch, Stopwatch writeStopwatch, long? featureDigestsParsed, long? featureDigestsSkipped, string? part = null,
        ExportTimingSnapshot? timings = null)
    {
        var byteSize = new FileInfo(outputPath).Length;
        var sha256 = ComputeSha256(outputPath);

        return new ManifestTableEntry
        {
            Table = table,
            OutputFile = OutputPaths.RelativeParquetPath(options.Store, table, options.FromUtc, options.ToUtc, part),
            RowCount = rowCount,
            ByteSize = byteSize,
            Sha256 = sha256,
            Columns = [.. columns.Select(c => new ManifestColumn { Name = c.Name, DuckDbType = c.DuckDbType })],
            StoreQueryElapsedMs = storeStopwatch.Elapsed.TotalMilliseconds,
            WriteElapsedMs = writeStopwatch.Elapsed.TotalMilliseconds,
            MissingContentCount = 0,
            KnownKeysSkipped = 0,
            FeatureDigestsParsed = featureDigestsParsed,
            FeatureDigestsSkipped = featureDigestsSkipped,
            Timings = timings,
        };
    }

    private static async Task<ManifestTableEntry> ExportQueryTextCoreAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, QueryStoreRawWindowCache rawWindowCache,
        QueryStatsWindowCache queryStatsWindowCache, ExportOptions options, CancellationToken ct)
    {
        const string table = DimensionTables.QueryText;
        var part = DimensionOrigins.PartFor(options.DimensionOrigin);
        var outputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, table, options.FromUtc, options.ToUtc, part);
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);
        var tempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-{Guid.NewGuid():N}.duckdb");

        try
        {
            var storeStopwatch = Stopwatch.StartNew();

            var textPacer = new ContentFetchPacer(allowCompressed: options.AllowCompressed);
            var timings = new ExportTimings(textPacer);

            await using var pgConnection = await timings.OpenConnectionAsync(connectionStringBuilder, ct).ConfigureAwait(false);

            var byKey = new Dictionary<(string?, string?, string?), TextKey>();
            var readQueryStats = options.DimensionOrigin is DimensionOrigins.QueryStats or DimensionOrigins.Both;
            var readQueryStore = options.DimensionOrigin is DimensionOrigins.QueryStore or DimensionOrigins.Both;

            if (readQueryStats)
            {
                await ReadQueryTextKeysFromQueryStatsAsync(queryStatsWindowCache, timings, ct, reader =>
                {
                    var key = new TextKey(NullableDuckString(reader, 0), NullableDuckString(reader, 1), NullableDuckString(reader, 2), reader.GetDateTime(3), reader.GetDateTime(4), DimensionOrigin.QueryStats)
                    {
                        ExampleDigest = ReadDuckBlob(reader, 5),
                    };
                    byKey[(key.ServerName, key.DatabaseName, key.QueryHash)] = key;
                }).ConfigureAwait(false);
            }

            var hasQueryStore = readQueryStore && await timings.StoreReadAsync(() => StoreCapabilities.HasQueryStoreStatsAsync(pgConnection, ct)).ConfigureAwait(false);
            if (hasQueryStore)
            {
                await ReadQueryStoreTextKeysAsync(rawWindowCache, timings, ct, reader =>
                {
                    var serverName = NullableDuckString(reader, 1);
                    var databaseName = NullableDuckString(reader, 2);
                    var queryHash = NullableDuckString(reader, 3);
                    var k = (serverName, databaseName, queryHash);
                    if (byKey.ContainsKey(k))
                    {
                        return;
                    }

                    byKey[k] = new TextKey(serverName, databaseName, queryHash, reader.GetDateTime(4), reader.GetDateTime(5), DimensionOrigin.QueryStore)
                    {
                        ServerId = reader.GetInt32(0),
                        ExampleQueryId = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                    };
                }).ConfigureAwait(false);
            }

            var mergedCount = byKey.Count;
            using var stateStore = StateStore.Open(options.StateDir);
            var survivingKeys = options.IgnoreState
                ? byKey.Keys.ToHashSet()
                : stateStore.FilterUnknownTextKeys(options.Store, byKey.Keys)!;
            var knownKeysSkipped = mergedCount - survivingKeys.Count;

            var keys = survivingKeys.Select(k => byKey[k]).ToList();

            var queryStoreKeys = keys.Where(k => k.Origin == DimensionOrigin.QueryStore).ToList();
            await ResolveQueryStoreTextAsync(pgConnection, queryStoreKeys, textPacer, ct, timings).ConfigureAwait(false);

            var keysByDigestHex = new Dictionary<string, List<TextKey>>();
            foreach (var key in keys.Where(k => k.Origin == DimensionOrigin.QueryStats))
            {
                var hex = DimensionContent.ToHexDigest(key.ExampleDigest!);
                if (!keysByDigestHex.TryGetValue(hex, out var list))
                {
                    list = [];
                    keysByDigestHex[hex] = list;
                }
                list.Add(key);
            }

            // batches ordered by the key's first_seen, same rule as plan_shape.
            var distinctDigests = keysByDigestHex
                .OrderBy(kv => kv.Value.Min(k => k.FirstSeen))
                .ThenBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => Convert.FromHexString(kv.Key))
                .ToList();

            var columns = new List<ColumnShape>
            {
                new("source_store", "VARCHAR"),
                new("server_name", "VARCHAR"),
                new("database_name", "VARCHAR"),
                new("query_hash", "VARCHAR"),
                new("first_seen", "TIMESTAMP"),
                new("last_seen", "TIMESTAMP"),
                new("example_digest", "VARCHAR"),
                new("query_text", "VARCHAR"),
                new("query_text_bytes", "BIGINT"),
                new("origin", "VARCHAR"),
            };

            long rowCount = 0;
            long missingContentCount = 0;

            var writeStopwatch = new Stopwatch();
            using (var writer = new ChunkedParquetWriter(tempDbPath, table, columns))
            {
                foreach (var batch in DimensionContent.Batch(distinctDigests, DigestBatchSize))
                {
                    var contentByDigest = await timings.PacedContentFetchAsync(textPacer, () => FetchQueryTextContentAsync(pgConnection, batch, ct), ct).ConfigureAwait(false);
                    foreach (var digest in batch)
                    {
                        var hex = DimensionContent.ToHexDigest(digest);
                        foreach (var key in keysByDigestHex[hex])
                        {
                            var text = contentByDigest.TryGetValue(hex, out var found) ? found.Item1 : null;
                            if (AppendTextRow(writer.Appender, options.Store, key, hex, text, ref missingContentCount))
                            {
                                writer.RowAppended(System.Text.Encoding.UTF8.GetByteCount(text!));
                                rowCount++;
                            }
                        }
                    }
                }

                foreach (var key in queryStoreKeys)
                {
                    if (AppendTextRow(writer.Appender, options.Store, key, hex: null, text: key.QueryStoreText, ref missingContentCount))
                    {
                        writer.RowAppended(System.Text.Encoding.UTF8.GetByteCount(key.QueryStoreText!));
                        rowCount++;
                    }
                }

                storeStopwatch.Stop();
                writeStopwatch.Start();
                writer.Complete(outputPath);
                writeStopwatch.Stop();
            }

            var byteSize = new FileInfo(outputPath).Length;
            var sha256 = ComputeSha256(outputPath);

            var entry = new ManifestTableEntry
            {
                Table = table,
                OutputFile = OutputPaths.RelativeParquetPath(options.Store, table, options.FromUtc, options.ToUtc, part),
                RowCount = rowCount,
                ByteSize = byteSize,
                Sha256 = sha256,
                Columns = [.. columns.Select(c => new ManifestColumn { Name = c.Name, DuckDbType = c.DuckDbType })],
                StoreQueryElapsedMs = storeStopwatch.Elapsed.TotalMilliseconds,
                WriteElapsedMs = writeStopwatch.Elapsed.TotalMilliseconds,
                MissingContentCount = missingContentCount,
                KnownKeysSkipped = knownKeysSkipped,
                Timings = timings.Snapshot(includeParse: false, includeWindowCache: true),
            };

            PendingKeyRecorder.PendingTextKeys[options.Store] = [.. keys.Where(k => k.ContentWritten).Select(k => (k.ServerName, k.DatabaseName, k.QueryHash))];

            return entry;
        }
        finally
        {
            DuckDbSession.DeleteTempDatabase(tempDbPath);
        }
    }

    /// <summary>Appends one query_text row, or counts missing text and appends nothing (same reason as plans).</summary>
    private static bool AppendTextRow(DuckDBAppender appender, string store, TextKey key, string? hex, string? text, ref long missingContentCount)
    {
        if (text is null)
        {
            missingContentCount++;
            return false;
        }

        key.ContentWritten = true;

        var row = appender.CreateRow();
        row.AppendValue(store);
        AppendNullableString(row, key.ServerName);
        AppendNullableString(row, key.DatabaseName);
        AppendNullableString(row, key.QueryHash);
        row.AppendValue(key.FirstSeen);
        row.AppendValue(key.LastSeen);
        AppendNullableString(row, hex);
        AppendNullableString(row, text);
        if (text is null)
        {
            row.AppendNullValue();
        }
        else
        {
            row.AppendValue((long)System.Text.Encoding.UTF8.GetByteCount(text));
        }
        row.AppendValue(key.Origin);
        row.EndRow();
        return true;
    }

    private static void AppendNullableString(IDuckDBAppenderRow row, string? value)
    {
        if (value is null)
        {
            row.AppendNullValue();
        }
        else
        {
            row.AppendValue(value);
        }
    }

    /// <summary>Runs a store call under <paramref name="timings"/> (store_read_ms) when there is one, plain otherwise.</summary>
    private static Task<T> TimedStoreRead<T>(ExportTimings? timings, Func<Task<T>> read) =>
        timings is null ? read() : timings.StoreReadAsync(read);

    private static async Task ResolvePlanMapDigestsAsync(NpgsqlConnection connection, List<PlanKey> keys, ContentFetchPacer pacer, CancellationToken ct, ExportTimings? timings = null)
    {
        // batches (and the batch sequence) are ordered by (server_id, database_name, plan_id), so
        // consecutive batches walk neighbouring btree leaf pages of the plan_map lookup.
        var ordered = keys
            .OrderBy(k => k.ServerId)
            .ThenBy(k => k.ExampleDatabaseName, StringComparer.Ordinal)
            .ThenBy(k => k.ExamplePlanId)
            .ToList();

        foreach (var batch in DimensionContent.BatchGeneric(ordered, DigestBatchSize))
        {
            var sids = batch.Select(k => k.ServerId!.Value).ToList();
            var dbs = batch.Select(k => k.ExampleDatabaseName!).ToList();
            var pids = batch.Select(k => k.ExamplePlanId!.Value).ToList();

            var digestByTriple = await pacer.RunAsync(() => TimedStoreRead(timings, async () =>
            {
                await using var command = new NpgsqlCommand(SqlBuilder.QueryStorePlanMapBatchSql, connection);
                command.Parameters.Add(new NpgsqlParameter("sids", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = sids });
                command.Parameters.Add(new NpgsqlParameter("dbs", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = dbs });
                command.Parameters.Add(new NpgsqlParameter("pids", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { Value = pids });

                var result = new Dictionary<(int, string, long), byte[]>();
                await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    var sid = reader.GetInt32(0);
                    var db = reader.GetString(1);
                    var pid = reader.GetInt64(2);
                    if (!reader.IsDBNull(3))
                    {
                        result[(sid, db, pid)] = (byte[])reader.GetValue(3);
                    }
                }

                return result;
            }), ct).ConfigureAwait(false);

            foreach (var key in batch)
            {
                if (digestByTriple.TryGetValue((key.ServerId!.Value, key.ExampleDatabaseName!, key.ExamplePlanId!.Value), out var digest))
                {
                    key.ExampleDigest = digest;
                }
            }
        }
    }

    private static async Task ResolveQueryStoreTextAsync(NpgsqlConnection connection, List<TextKey> keys, ContentFetchPacer pacer, CancellationToken ct, ExportTimings? timings = null)
    {
        // same batch ordering rule as ResolvePlanMapDigestsAsync, by (server_id, database_name,
        // query_id).
        var ordered = keys
            .OrderBy(k => k.ServerId)
            .ThenBy(k => k.DatabaseName, StringComparer.Ordinal)
            .ThenBy(k => k.ExampleQueryId)
            .ToList();

        foreach (var batch in DimensionContent.BatchGeneric(ordered, DigestBatchSize))
        {
            var sids = batch.Select(k => k.ServerId!.Value).ToList();
            var dbs = batch.Select(k => k.DatabaseName ?? string.Empty).ToList();
            var qids = batch.Select(k => k.ExampleQueryId!.Value).ToList();

            var textByTriple = await pacer.RunAsync(() => TimedStoreRead(timings, async () =>
            {
                await using var command = new NpgsqlCommand(SqlBuilder.QueryStoreTextBatchSql, connection);
                command.Parameters.Add(new NpgsqlParameter("sids", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = sids });
                command.Parameters.Add(new NpgsqlParameter("dbs", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = dbs });
                command.Parameters.Add(new NpgsqlParameter("qids", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { Value = qids });

                var result = new Dictionary<(int, string, long), string?>();
                await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    var sid = reader.GetInt32(0);
                    var db = reader.GetString(1);
                    var qid = reader.GetInt64(2);
                    var text = reader.IsDBNull(3) ? null : reader.GetString(3);
                    result[(sid, db, qid)] = text;
                }

                return result;
            }), ct).ConfigureAwait(false);

            // The Query Store text lookup returns query text itself: count it as a content fetch (after the pacer
            // has timed the call, so counting cannot lengthen the pacing sleep).
            timings?.RecordContentFetch(
                textByTriple.Count,
                textByTriple.Values.Sum(t => t is null ? 0L : System.Text.Encoding.UTF8.GetByteCount(t)));

            foreach (var key in batch)
            {
                if (textByTriple.TryGetValue((key.ServerId!.Value, key.DatabaseName ?? string.Empty, key.ExampleQueryId!.Value), out var text))
                {
                    key.QueryStoreText = text;
                }
                key.QueryStoreTextResolved = true;
            }
        }
    }

    /// <summary>
    /// reads the plan-key rows from the DuckDB raw-window cache (<see
    /// cref="SqlBuilder.QueryStorePlanShapeKeyDuckDbSql"/>) instead of a second store read.
    /// The window predicate is already baked into the cached `query_store_raw` table, so this
    /// query carries no `@from`/`@to` parameters. <see cref="SqlBuilder.QueryStorePlanShapeKeySql"/>
    /// stays in <see cref="SqlBuilder"/> only as a test oracle -- no code path here passes it
    /// to an NpgsqlCommand.
    /// </summary>
    private static async Task ReadQueryStorePlanKeysAsync(QueryStoreRawWindowCache rawWindowCache, ExportTimings timings, CancellationToken ct, Action<DuckDBDataReader> onRow)
    {
        var rawTempDbPath = await rawWindowCache.GetOrBuildAsync(timings, ct).ConfigureAwait(false);
        using var duckConnection = DuckDbSession.Open(rawTempDbPath);
        using var command = duckConnection.CreateCommand();
        command.CommandText = SqlBuilder.QueryStorePlanShapeKeyDuckDbSql;
        using var reader = (DuckDBDataReader)command.ExecuteReader();
        while (reader.Read())
        {
            onRow(reader);
        }
    }

    /// <summary>Same shape as <see cref="ReadQueryStorePlanKeysAsync"/>, for the text-key query.</summary>
    private static async Task ReadQueryStoreTextKeysAsync(QueryStoreRawWindowCache rawWindowCache, ExportTimings timings, CancellationToken ct, Action<DuckDBDataReader> onRow)
    {
        var rawTempDbPath = await rawWindowCache.GetOrBuildAsync(timings, ct).ConfigureAwait(false);
        using var duckConnection = DuckDbSession.Open(rawTempDbPath);
        using var command = duckConnection.CreateCommand();
        command.CommandText = SqlBuilder.QueryStoreTextKeyDuckDbSql;
        using var reader = (DuckDBDataReader)command.ExecuteReader();
        while (reader.Read())
        {
            onRow(reader);
        }
    }

    /// <summary>Same NULL handling as <see cref="NullableString"/>, for a DuckDB reader.</summary>
    private static string? NullableDuckString(DuckDBDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    /// <summary>
    /// DuckDB.NET (1.5.5) hands back a BLOB column computed by an aggregate (e.g. <c>min()</c>)
    /// as a <see cref="Stream"/>, not a value castable to <c>byte[]</c> -- unlike a BLOB column
    /// read straight off a table, which does support <c>GetFieldValue&lt;byte[]&gt;</c>. Read it
    /// via <see cref="DuckDBDataReader.GetStream"/> and copy it out instead of casting.
    /// </summary>
    internal static byte[] ReadDuckBlob(DuckDBDataReader reader, int ordinal)
    {
        using var stream = reader.GetStream(ordinal);
        using var buffer = new MemoryStream();
        stream.CopyTo(buffer);
        return buffer.ToArray();
    }

    /// <summary>
    /// reads the plan-shape key rows from the DuckDB query_stats raw-window cache (<see
    /// cref="SqlBuilder.PlanShapeKeyDuckDbSql"/>) instead of a second store read. The window
    /// predicate is already baked into the cached `query_stats_raw` table, so this query
    /// carries no `@from`/`@to` parameters.
    /// </summary>
    private static async Task ReadPlanShapeKeysFromQueryStatsAsync(QueryStatsWindowCache windowCache, ExportTimings timings, CancellationToken ct, Action<DuckDBDataReader> onRow)
    {
        var rawTempDbPath = await windowCache.GetOrBuildAsync(timings, ct).ConfigureAwait(false);
        using var duckConnection = DuckDbSession.Open(rawTempDbPath);
        using var command = duckConnection.CreateCommand();
        command.CommandText = SqlBuilder.PlanShapeKeyDuckDbSql;
        using var reader = (DuckDBDataReader)command.ExecuteReader();
        while (reader.Read())
        {
            onRow(reader);
        }
    }

    /// <summary>Same shape as <see cref="ReadPlanShapeKeysFromQueryStatsAsync"/>, for the text-key query.</summary>
    private static async Task ReadQueryTextKeysFromQueryStatsAsync(QueryStatsWindowCache windowCache, ExportTimings timings, CancellationToken ct, Action<DuckDBDataReader> onRow)
    {
        var rawTempDbPath = await windowCache.GetOrBuildAsync(timings, ct).ConfigureAwait(false);
        using var duckConnection = DuckDbSession.Open(rawTempDbPath);
        using var command = duckConnection.CreateCommand();
        command.CommandText = SqlBuilder.QueryTextKeyDuckDbSql;
        using var reader = (DuckDBDataReader)command.ExecuteReader();
        while (reader.Read())
        {
            onRow(reader);
        }
    }

    internal static async Task<Dictionary<string, (string?, byte[]?)>> FetchPlanContentAsync(
        NpgsqlConnection connection, List<byte[]> digests, CancellationToken ct)
    {
        var result = new Dictionary<string, (string?, byte[]?)>();

        await using var command = new NpgsqlCommand(SqlBuilder.PlanDimBatchSql, connection);
        command.Parameters.Add(new NpgsqlParameter("digests", NpgsqlDbType.Array | NpgsqlDbType.Bytea) { Value = digests });

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var digest = (byte[])reader.GetValue(0);
            var xml = reader.IsDBNull(1) ? null : reader.GetString(1);
            var gz = reader.IsDBNull(2) ? null : (byte[])reader.GetValue(2);
            result[DimensionContent.ToHexDigest(digest)] = (xml, gz);
        }

        return result;
    }

    private static async Task<Dictionary<string, (string?, byte[]?)>> FetchQueryTextContentAsync(
        NpgsqlConnection connection, List<byte[]> digests, CancellationToken ct)
    {
        var result = new Dictionary<string, (string?, byte[]?)>();

        await using var command = new NpgsqlCommand(SqlBuilder.QueryTextDimBatchSql, connection);
        command.Parameters.Add(new NpgsqlParameter("digests", NpgsqlDbType.Array | NpgsqlDbType.Bytea) { Value = digests });

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var digest = (byte[])reader.GetValue(0);
            var text = reader.IsDBNull(1) ? null : reader.GetString(1);
            result[DimensionContent.ToHexDigest(digest)] = (text, null);
        }

        return result;
    }

    internal static string ComputeSha256(string path)
    {
        using var stream = File.OpenRead(path);
        var hash = SHA256.HashData(stream);
        return Convert.ToHexStringLower(hash);
    }

    private static string QuoteDuckIdentifier(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";
}

/// <summary>
/// The plan-fetch gate: fetch a plan's XML only when its accumulated executions reach <see
/// cref="PlanFetchThreshold.MinExecutions"/> or its accumulated CPU reaches <see cref="PlanFetchThreshold.MinCpuUs"/>.
/// Keys below the threshold wait in the state store's `plan_fetch_pending` table, and are fetched in a later hour once
/// they cross it.
/// </summary>
internal static class PlanFetchGate
{
    /// <summary>
    /// The gate is on unless the operator turned it off: <c>--ignore-state</c> (a re-export fetches everything,
    /// as it always did) or <c>--plan-fetch-all</c>.
    /// </summary>
    public static bool IsEnabled(ExportOptions options) => !options.IgnoreState && !options.PlanFetchAll;

    /// <summary>
    /// Prunes stale pending rows (once per call, in DuckDB), then adds this window's totals to every candidate's
    /// pending row and returns the verdicts. Reads nothing from the store.
    /// </summary>
    public static List<PlanFetchDecision> Apply(
        StateStore stateStore, string sourceStore, DateTime nowUtc, IEnumerable<PlanFetchCandidate> candidates,
        long minExecutions = PlanFetchThreshold.MinExecutions, long minCpuUs = PlanFetchThreshold.MinCpuUs)
    {
        stateStore.PrunePlanFetchPending(nowUtc);
        return stateStore.EvaluatePlanFetchGate(sourceStore, candidates, minExecutions, minCpuUs);
    }
}

/// <summary>
/// Holds the set of dimension keys each export actually wrote to Parquet for this run, keyed
/// by store, so <see cref="ExportRunner"/> can pass them to <see cref="StateStore.RecordExportedKeys"/>
/// only after the whole run (and, when --upload is given, the upload) has succeeded. A static
/// per-store dictionary is safe here because the exporter process runs one export at a time.
/// </summary>
public static class PendingKeyRecorder
{
    public static readonly Dictionary<string, List<(string?, string)>> PendingPlanKeys = new();
    public static readonly Dictionary<string, List<(string?, string?, string?)>> PendingTextKeys = new();

    /// <summary>
    /// The plan-fetch gate's totals for keys not exported this run (held back, or qualified but with no content
    /// yet), written to `plan_fetch_pending` by <see cref="StateStore.RecordExportedKeys"/> after the run succeeded.
    /// </summary>
    public static readonly Dictionary<string, List<PlanFetchDecision>> PendingPlanFetchUpserts = new();

    /// <summary>Returns and clears this store's pending plan-fetch totals.</summary>
    public static List<PlanFetchDecision> TakePlanFetchPending(string store)
    {
        if (PendingPlanFetchUpserts.Remove(store, out var list))
        {
            return list;
        }

        return [];
    }

    // Appended to, not assigned: plan_shape and procedure_plan can both run in the same export and
    // both parse feature digests. Each calls AddPendingFeatureDigests instead of overwriting the
    // other's list, so a combined run records every digest either of them parsed.
    public static readonly Dictionary<string, List<(string PlanDigest, int FeatureVersion)>> PendingFeatureDigests = new();

    public static readonly Dictionary<string, List<(string? ServerName, string? DatabaseName, string? SchemaName, string? ObjectName, string ShapeHash)>> PendingProcedurePlanKeys = new();
    public static readonly Dictionary<string, List<(string PlanDigest, string ShapeHash)>> PendingProcedureDigests = new();

    public static void AddPendingFeatureDigests(string store, IEnumerable<(string PlanDigest, int FeatureVersion)> digests)
    {
        if (!PendingFeatureDigests.TryGetValue(store, out var list))
        {
            list = [];
            PendingFeatureDigests[store] = list;
        }
        list.AddRange(digests);
    }
}
