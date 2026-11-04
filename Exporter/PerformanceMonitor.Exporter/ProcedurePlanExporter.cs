using System.Diagnostics;
using DuckDB.NET.Data;
using Npgsql;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Exports the `procedure_plan` dimension table: module plans from `collect.procedure_stats`, keyed by (server,
/// database, schema, object, shape). One row per distinct shape per object, with an example digest and its parsed
/// statement features written to `plan_features` / `plan_warnings` / `plan_missing_indexes` alongside the ones
/// plan_shape writes (see DimensionTableExporter). Digests already known to map to a shape (StateStore's
/// `procedure_digests`) skip a re-fetch of their content; keys already recorded in `procedure_plan_keys` are dropped
/// before any content fetch used for writing runs. the plan keys and the oversized-plan count come from the
/// shared, run-scoped <see cref="ProcedureStatsWindowCache"/> (<see cref="SqlBuilder.ProcedurePlanKeyDuckDbSql"/>,
/// <see cref="SqlBuilder.ProcedurePlanOversizedCountDuckDbSql"/>) rather than two dedicated store reads (<see
/// cref="SqlBuilder.ProcedurePlanKeySql"/>, <see cref="SqlBuilder.ProcedurePlanOversizedCountSql"/> stay in <see
/// cref="SqlBuilder"/> only as test oracles). pass 1 and pass 2 are merged into one ordered walk. <see
/// cref="SqlBuilder.ProcedurePlanKeyDuckDbSql"/>'s explicit ORDER BY (server, database, schema, object, object_type,
/// digest) means the keys for one object are visited in ascending digest order, so the FIRST digest that resolves to a
/// given (object, shape) is always the smallest digest for that shape -- the same "first in ORDER BY wins" rule Q6
/// already uses elsewhere. The walk fetches unresolved digests in batches of <see cref="DigestBatchSize"/>, computes
/// each one's shape from the content it just fetched, and -- if that digest is the first sighting of a NEW (object,
/// shape) group -- keeps that same content (in <c>groupExampleContent</c>, bounded by the number of distinct groups
/// this run, not the number of digests) to write the row and its parsed features later, once the group survives the
/// bulk `procedure_plan_keys` filter. A digest whose shape was already known from a previous run (StateStore's
/// `procedure_digests`) but which turns out to be a NEW group's example still needs exactly one content fetch to write
/// it -- unavoidable, since that digest's content was never read before, and not a repeat of anything read in this
/// run.
/// </summary>
public static class ProcedurePlanExporter
{
    private const int DigestBatchSize = 250;
    private const string MissingContentMarker = "\u0000missing";

    private sealed record ProcedureKey(
        string? ServerName, string? DatabaseName, string? SchemaName, string? ObjectName, string? ObjectType,
        byte[] ExampleDigest, DateTime FirstSeen, DateTime LastSeen, ProcedurePlanIdentity? Identity);

    private sealed record ShapeKey(
        string? ServerName, string? DatabaseName, string? SchemaName, string? ObjectName, string ShapeHash)
    {
        public DateTime FirstSeen { get; set; }
        public DateTime LastSeen { get; set; }
        public byte[]? ExampleDigest { get; set; }
    }

    public static async Task<IReadOnlyList<ManifestTableEntry>> ExportAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, ProcedureStatsWindowCache windowCache, ExportOptions options, CancellationToken ct,
        DateTime? nowUtc = null)
    {
        // The run clock, read once. Every reuse decision and the recorded last_fetch_utc use this value; tests pass their own.
        var runTimeUtc = nowUtc ?? DateTime.UtcNow;
        const string table = DimensionTables.ProcedurePlan;
        var outputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, table, options.FromUtc, options.ToUtc);
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

            // Step 1: the plan keys and the oversized-plan count both come from the
            // shared procedure_stats raw window, run in DuckDB -- no store-side GROUP BY.
            var rawTempDbPath = await windowCache.GetOrBuildAsync(timings, ct).ConfigureAwait(false);

            var procedureKeys = new List<ProcedureKey>();
            long oversizedPlanRows;
            using (var duckConnection = DuckDbSession.Open(rawTempDbPath))
            {
                using (var keyCommand = duckConnection.CreateCommand())
                {
                    keyCommand.CommandText = SqlBuilder.ProcedurePlanKeyDuckDbSql;
                    using var reader = (DuckDBDataReader)keyCommand.ExecuteReader();
                    while (reader.Read())
                    {
                        var serverName = NullableDuckString(reader, 0);
                        var databaseName = NullableDuckString(reader, 1);
                        var schemaName = NullableDuckString(reader, 2);
                        var objectName = NullableDuckString(reader, 3);
                        // A NULL or empty plan_handle, or a NULL cached_time, means no identity: such a digest is fetched as today.
                        var planHandle = NullableDuckString(reader, 8);
                        var identity = string.IsNullOrWhiteSpace(planHandle) || reader.IsDBNull(9)
                            ? null
                            : new ProcedurePlanIdentity(serverName, databaseName, schemaName, objectName, planHandle, reader.GetDateTime(9));
                        procedureKeys.Add(new ProcedureKey(
                            serverName, databaseName, schemaName, objectName, NullableDuckString(reader, 4),
                            DimensionTableExporter.ReadDuckBlob(reader, 5), reader.GetDateTime(6), reader.GetDateTime(7), identity));
                    }
                }

                using (var oversizedCommand = duckConnection.CreateCommand())
                {
                    oversizedCommand.CommandText = SqlBuilder.ProcedurePlanOversizedCountDuckDbSql;
                    oversizedPlanRows = Convert.ToInt64(oversizedCommand.ExecuteScalar() ?? 0L);
                }
            }

            using var stateStore = StateStore.Open(options.StateDir);

            // Prune procedure_digests rows older than the store's plan retention (14 days) once per run, before
            // filtering. A pruned digest is at worst shaped again.
            if (!options.IgnoreState)
            {
                var prunedCount = stateStore.PruneOldProcedureDigests(DateTime.UtcNow);
                if (prunedCount > 0)
                {
                    Console.Error.WriteLine($"StateStore: pruned {prunedCount} procedure_digests row(s) older than 14 days.");
                }
            }

            // Step 2: look up known digests in procedure_digests, which gives their shape without a fetch.
            var digestHexes = procedureKeys.Select(k => DimensionContent.ToHexDigest(k.ExampleDigest)).ToHashSet(StringComparer.Ordinal);
            HashSet<string> unknownDigestHexesSeed;
            Dictionary<string, string> knownShapeByDigest;
            if (options.IgnoreState)
            {
                unknownDigestHexesSeed = digestHexes;
                knownShapeByDigest = new Dictionary<string, string>(StringComparer.Ordinal);
            }
            else
            {
                (unknownDigestHexesSeed, knownShapeByDigest) = stateStore.FilterUnknownProcedureDigests(options.Store, digestHexes);
            }

            // Procedure plan reuse: decide, per unknown digest, whether its cached plan (plan_handle + cached_time)
            // was fetched recently enough to skip. Off (or --ignore-state) touches nothing.
            var reusePlan = ProcedurePlanReuse.Plan(
                stateStore, options.Store, options.ProcedurePlanReuse, options.IgnoreState,
                procedureKeys
                    .Select(k => (Hex: DimensionContent.ToHexDigest(k.ExampleDigest), k.Identity))
                    .Where(k => unknownDigestHexesSeed.Contains(k.Hex))
                    .Select(k => (k.Hex, k.Identity))
                    .ToList(),
                runTimeUtc);
            var digestsToDrop = reusePlan.DigestsToDrop;
            var shadowSkipDigests = reusePlan.Mode == ProcedurePlanReuseMode.Shadow ? reusePlan.Evaluation!.SkipDigests : null;
            long identitySkipBytes = 0;
            long identityRefetchRows = 0, identityNewRows = 0, identityNullRows = 0; // rows written whose example digest was fetched under that decision
            long identitySkipRows = 0; // shadow: rows written whose example digest `on` would have skipped

            var objectTypeByGroupKeyPrefix = procedureKeys
                .GroupBy(k => (k.ServerName, k.DatabaseName, k.SchemaName, k.ObjectName))
                .ToDictionary(g => g.Key, g => g.First().ObjectType);

            // Step 3/4/5 merged: one ordered walk over procedureKeys (already sorted by
            // digest within object). Shapes are resolved in batches of unresolved digests as
            // the walk needs them; each freshly resolved digest that turns out to be the first
            // sighting of a NEW (object, shape) group keeps its content in groupExampleContent
            // (bounded by distinct-group count, not digest count) for the write step below.
            var shapeByDigest = new Dictionary<string, string>(knownShapeByDigest, StringComparer.Ordinal);
            var recordedProcedureDigests = new List<(string PlanDigest, string ShapeHash)>();
            var shapeKeys = new Dictionary<(string?, string?, string?, string?, string), ShapeKey>();
            var groupExampleContent = new Dictionary<(string?, string?, string?, string?, string), (string Content, IReadOnlyList<string> StatementPlanHashes)>();
            // Content fetched for the batch currently being resolved, keyed by digest hex. Bounded to at
            // most DigestBatchSize entries at a time: each entry is consumed and removed by the ordered
            // walk immediately after the key that needed it is processed, one key per digest (the key
            // query already groups to one row per (object, digest)). Local to this call -- ExportAsync
            // has no shared mutable state across concurrent runs.
            var pendingBatchContent = new Dictionary<string, (string Content, IReadOnlyList<string> StatementPlanHashes)>(StringComparer.Ordinal);
            long keysWithoutContent = 0;

            var idx = 0;
            while (idx < procedureKeys.Count)
            {
                var key = procedureKeys[idx];
                var hex = DimensionContent.ToHexDigest(key.ExampleDigest);

                if (digestsToDrop.Contains(hex))
                {
                    // `on`: this digest's cached plan was fetched recently. No fetch, no shape, and nothing recorded for the
                    // digest, so the next run decides it again.
                    idx++;
                    continue;
                }

                if (!shapeByDigest.ContainsKey(hex))
                {
                    var batchDigests = new List<byte[]>();
                    var batchSeen = new HashSet<string>(StringComparer.Ordinal);
                    for (var j = idx; j < procedureKeys.Count && batchDigests.Count < DigestBatchSize; j++)
                    {
                        var h = DimensionContent.ToHexDigest(procedureKeys[j].ExampleDigest);
                        if (!shapeByDigest.ContainsKey(h) && !digestsToDrop.Contains(h) && batchSeen.Add(h))
                        {
                            batchDigests.Add(procedureKeys[j].ExampleDigest);
                        }
                    }

                    var fetched = await timings.PacedContentFetchAsync(pacer, () => DimensionTableExporter.FetchPlanContentAsync(pgConnection, batchDigests, ct), ct).ConfigureAwait(false);
                    foreach (var digest in batchDigests)
                    {
                        var h = DimensionContent.ToHexDigest(digest);
                        var (xml, gz) = fetched.TryGetValue(h, out var found) ? found : (null, null);
                        if (shadowSkipDigests is not null && shadowSkipDigests.Contains(h) && fetched.TryGetValue(h, out var skippedValue))
                        {
                            identitySkipBytes += ExportTimings.MeasureValue(skippedValue); // shadow: what `on` would not have fetched
                        }

                        var content = timings.Parse(() => DimensionContent.ResolvePlanXml(xml, gz));
                        if (content is null)
                        {
                            shapeByDigest[h] = MissingContentMarker; // no content anywhere for this digest
                            continue;
                        }

                        var (shapeHash, statementPlanHashes) = timings.Parse(() => PlanShapeHasher.Compute(content));
                        shapeByDigest[h] = shapeHash;
                        recordedProcedureDigests.Add((h, shapeHash));

                        // Tentatively hold this digest's content only if it becomes an example below
                        // (checked in the same pass, immediately after this batch resolves, since the
                        // digests just fetched are exactly the ones the upcoming walk positions need).
                        pendingBatchContent[h] = (content, statementPlanHashes);
                    }

                    continue; // re-check the current index now that its digest's shape is resolved
                }

                var shapeHash2 = shapeByDigest[hex];
                if (shapeHash2 == MissingContentMarker)
                {
                    keysWithoutContent++;
                    idx++;
                    continue;
                }

                var groupKey = (key.ServerName, key.DatabaseName, key.SchemaName, key.ObjectName, shapeHash2);
                if (!shapeKeys.TryGetValue(groupKey, out var shapeKeyRow))
                {
                    shapeKeyRow = new ShapeKey(key.ServerName, key.DatabaseName, key.SchemaName, key.ObjectName, shapeHash2)
                    {
                        FirstSeen = key.FirstSeen,
                        LastSeen = key.LastSeen,
                        ExampleDigest = key.ExampleDigest,
                    };
                    shapeKeys[groupKey] = shapeKeyRow;

                    if (pendingBatchContent.TryGetValue(hex, out var pending))
                    {
                        groupExampleContent[groupKey] = pending;
                    }
                }
                else
                {
                    if (key.FirstSeen < shapeKeyRow.FirstSeen) shapeKeyRow.FirstSeen = key.FirstSeen;
                    if (key.LastSeen > shapeKeyRow.LastSeen) shapeKeyRow.LastSeen = key.LastSeen;
                }

                idx++;

                // Once every key that could need this batch's digests has been walked past, the
                // pending content is no longer reachable by any later key (procedureKeys is sorted,
                // and DistinctBy-style de-dup means each digest's keys are contiguous per object) --
                // drop it here to keep memory bounded to groups, not digests.
                if (idx >= procedureKeys.Count || DimensionContent.ToHexDigest(procedureKeys[idx].ExampleDigest) != hex)
                {
                    pendingBatchContent.Remove(hex);
                }
            }

            // Step 6: drop groups already recorded in procedure_plan_keys, one bulk filter call
            // exactly as before.
            var allShapeKeyTuples = shapeKeys.Keys.ToHashSet();
            var survivingShapeKeyTuples = options.IgnoreState
                ? allShapeKeyTuples
                : stateStore.FilterUnknownProcedurePlanKeys(options.Store, allShapeKeyTuples);
            var knownKeysSkipped = allShapeKeyTuples.Count - survivingShapeKeyTuples.Count;

            // Deterministic write order, matching the key ORDER BY.
            var newShapeKeys = survivingShapeKeyTuples
                .Select(k => shapeKeys[k])
                .OrderBy(k => k.ServerName, StringComparer.Ordinal)
                .ThenBy(k => k.DatabaseName, StringComparer.Ordinal)
                .ThenBy(k => k.SchemaName, StringComparer.Ordinal)
                .ThenBy(k => k.ObjectName, StringComparer.Ordinal)
                .ThenBy(k => k.ShapeHash, StringComparer.Ordinal)
                .ToList();

            var columns = new List<ColumnShape>
            {
                new("source_store", "VARCHAR"),
                new("server_name", "VARCHAR"),
                new("database_name", "VARCHAR"),
                new("schema_name", "VARCHAR"),
                new("object_name", "VARCHAR"),
                new("object_type", "VARCHAR"),
                new("shape_hash", "VARCHAR"),
                new("first_seen", "TIMESTAMP"),
                new("last_seen", "TIMESTAMP"),
                new("example_digest", "VARCHAR"),
                new("plan_xml", "VARCHAR"),
                new("plan_xml_bytes", "BIGINT"),
                new("statement_count", "INTEGER"),
                new("statement_plan_hashes", "VARCHAR"),
            };

            long rowCount = 0;

            var featureColumns = DimensionTableExporter.FeatureColumns();
            var warningColumns = DimensionTableExporter.WarningColumns();
            var missingIndexColumns = DimensionTableExporter.MissingIndexColumns();

            var featureVersion = PlanFeatureExtractor.FeatureVersion;

            long featureDigestsParsed = 0;
            long featureRowCount = 0;
            long warningRowCount = 0;
            long missingIndexRowCount = 0;
            var missingContentCount = keysWithoutContent;

            var parsedFeatureDigests = new List<(string PlanDigest, int FeatureVersion)>();
            var writtenKeys = new List<(string? ServerName, string? DatabaseName, string? SchemaName, string? ObjectName, string ShapeHash)>();

            // Features are keyed by plan content (digest), not by (object, shape): two shape rows in this run can
            // share the same example digest (e.g. object B's only shape has the same example as one of object A's
            // shapes --). Parse each digest's features once per run, matching DimensionTableExporter's per-digest (not
            // per-key) extraction.
            var featuresParsedThisRunByDigest = new HashSet<string>(StringComparer.Ordinal);

            var writeStopwatch = new Stopwatch();
            using (var writer = new ChunkedParquetWriter(tempDbPath, table, columns))
            using (var featuresWriter = new ChunkedParquetWriter(featuresTempDbPath, PlanFeatureTables.PlanFeatures, featureColumns))
            using (var warningsWriter = new ChunkedParquetWriter(warningsTempDbPath, PlanFeatureTables.PlanWarnings, warningColumns))
            using (var missingIndexesWriter = new ChunkedParquetWriter(missingIndexesTempDbPath, PlanFeatureTables.PlanMissingIndexes, missingIndexColumns))
            {
                foreach (var shapeKeyRow in newShapeKeys)
                {
                    var groupKey = (shapeKeyRow.ServerName, shapeKeyRow.DatabaseName, shapeKeyRow.SchemaName, shapeKeyRow.ObjectName, shapeKeyRow.ShapeHash);
                    var hex = DimensionContent.ToHexDigest(shapeKeyRow.ExampleDigest!);

                    string content;
                    IReadOnlyList<string> statementPlanHashes;
                    if (groupExampleContent.TryGetValue(groupKey, out var cached))
                    {
                        content = cached.Content;
                        statementPlanHashes = cached.StatementPlanHashes;
                    }
                    else
                    {
                        // The example's shape was already known from a previous run's procedure_digests,
                        // so its content was never read during the walk above. One fetch here, for a
                        // digest whose content this run has not touched before.
                        var resolved = await FetchExampleContentAsync(
                            timings, pacer, digests => DimensionTableExporter.FetchPlanContentAsync(pgConnection, digests, ct),
                            shapeKeyRow.ExampleDigest!, ct).ConfigureAwait(false);
                        if (resolved is null)
                        {
                            missingContentCount++;
                            continue; // no content left for this example: nothing to write (plan_shape's rule)
                        }
                        content = resolved;
                        (_, statementPlanHashes) = timings.Parse(() => PlanShapeHasher.Compute(content));
                    }

                    var objectType = objectTypeByGroupKeyPrefix.GetValueOrDefault((shapeKeyRow.ServerName, shapeKeyRow.DatabaseName, shapeKeyRow.SchemaName, shapeKeyRow.ObjectName));
                    var planXmlBytes = (long)System.Text.Encoding.UTF8.GetByteCount(content);
                    var row = writer.Appender.CreateRow();
                    row.AppendValue(options.Store);
                    AppendNullableString(row, shapeKeyRow.ServerName);
                    AppendNullableString(row, shapeKeyRow.DatabaseName);
                    AppendNullableString(row, shapeKeyRow.SchemaName);
                    AppendNullableString(row, shapeKeyRow.ObjectName);
                    AppendNullableString(row, objectType);
                    row.AppendValue(shapeKeyRow.ShapeHash);
                    row.AppendValue(shapeKeyRow.FirstSeen);
                    row.AppendValue(shapeKeyRow.LastSeen);
                    row.AppendValue(hex);
                    row.AppendValue(content);
                    row.AppendValue(planXmlBytes);
                    row.AppendValue(statementPlanHashes.Count);
                    AppendNullableString(row, statementPlanHashes.Count == 0 ? null : string.Join(",", statementPlanHashes));
                    row.EndRow();
                    writer.RowAppended(planXmlBytes); // module plans run to ~256 KB: chunks close by bytes too
                    rowCount++;
                    if (shadowSkipDigests is not null && shadowSkipDigests.Contains(hex)) { identitySkipRows++; }
                    if (reusePlan.Evaluation is { } rowEvaluation && rowEvaluation.ClassByDigest.TryGetValue(hex, out var rowClass))
                    {
                        if (rowClass == ProcedurePlanReuseClass.IdentityRefetch) identityRefetchRows++;
                        else if (rowClass == ProcedurePlanReuseClass.IdentityNew) identityNewRows++;
                        else if (rowClass == ProcedurePlanReuseClass.IdentityNull) identityNullRows++;
                    }
                    writtenKeys.Add((shapeKeyRow.ServerName, shapeKeyRow.DatabaseName, shapeKeyRow.SchemaName, shapeKeyRow.ObjectName, shapeKeyRow.ShapeHash));

                    if (!featuresParsedThisRunByDigest.Add(hex))
                    {
                        continue; // this digest's features were already parsed for another shape row in this run
                    }

                    var result = timings.Parse(() => PlanFeatureExtractor.Extract(hex, content));
                    foreach (var feature in result.Features)
                    {
                        DimensionTableExporter.AppendFeatureRow(featuresWriter.Appender, options.Store, options.FromUtc, feature);
                        featuresWriter.RowAppended();
                        featureRowCount++;
                    }
                    foreach (var warning in result.Warnings)
                    {
                        DimensionTableExporter.AppendWarningRow(warningsWriter.Appender, options.Store, warning);
                        warningsWriter.RowAppended();
                        warningRowCount++;
                    }
                    foreach (var missingIndex in result.MissingIndexes)
                    {
                        DimensionTableExporter.AppendMissingIndexRow(missingIndexesWriter.Appender, options.Store, missingIndex);
                        missingIndexesWriter.RowAppended();
                        missingIndexRowCount++;
                    }
                    featureDigestsParsed++;
                    parsedFeatureDigests.Add((hex, featureVersion));
                }

                storeStopwatch.Stop();
                writeStopwatch.Start();
                writer.Complete(outputPath);

                var featuresOutputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, PlanFeatureTables.PlanFeatures, options.FromUtc, options.ToUtc, part: "procedure");
                var warningsOutputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, PlanFeatureTables.PlanWarnings, options.FromUtc, options.ToUtc, part: "procedure");
                var missingIndexesOutputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, PlanFeatureTables.PlanMissingIndexes, options.FromUtc, options.ToUtc, part: "procedure");
                Directory.CreateDirectory(Path.GetDirectoryName(featuresOutputPath)!);
                Directory.CreateDirectory(Path.GetDirectoryName(warningsOutputPath)!);
                Directory.CreateDirectory(Path.GetDirectoryName(missingIndexesOutputPath)!);
                featuresWriter.Complete(featuresOutputPath);
                warningsWriter.Complete(warningsOutputPath);
                missingIndexesWriter.Complete(missingIndexesOutputPath);
                writeStopwatch.Stop();

                var byteSize = new FileInfo(outputPath).Length;
                var sha256 = DimensionTableExporter.ComputeSha256(outputPath);

                var timingSnapshot = timings.Snapshot(includeParse: true, includeWindowCache: true);

                ProcedurePlanReuseStats? reuseStats = null;
                if (reusePlan.Evaluation is { } evaluation)
                {
                    var (shapeChanged, shapeSame, prevUnknown) = evaluation.CompareRefetchShapes(
                        digestHex => shapeByDigest.TryGetValue(digestHex, out var shape) && shape != MissingContentMarker ? shape : null);
                    reuseStats = new ProcedurePlanReuseStats(
                        ProcedurePlanReuseModes.Name(reusePlan.Mode),
                        evaluation.Count(ProcedurePlanReuseClass.IdentityNew),
                        evaluation.Count(ProcedurePlanReuseClass.IdentityRefetch),
                        evaluation.Count(ProcedurePlanReuseClass.IdentitySkip),
                        evaluation.Count(ProcedurePlanReuseClass.IdentityNull),
                        reusePlan.Mode == ProcedurePlanReuseMode.Shadow ? identitySkipBytes : null,
                        reusePlan.Mode == ProcedurePlanReuseMode.Shadow ? identitySkipRows : null,
                        identityRefetchRows, identityNewRows, identityNullRows, shapeChanged, shapeSame, prevUnknown);

                    // Recorded with the run's other keys, in one transaction, only after the whole run succeeded. An identity
                    // is recorded only when one of its digests had content.
                    PendingProcedurePlanIdentities.Set(options.Store, evaluation.IdentitiesToRecord(
                        digestHex => shapeByDigest.TryGetValue(digestHex, out var shape) && shape != MissingContentMarker, runTimeUtc));
                }
                var entry = new ManifestTableEntry
                {
                    Table = table,
                    OutputFile = OutputPaths.RelativeParquetPath(options.Store, table, options.FromUtc, options.ToUtc),
                    RowCount = rowCount,
                    ByteSize = byteSize,
                    Sha256 = sha256,
                    Columns = [.. columns.Select(c => new ManifestColumn { Name = c.Name, DuckDbType = c.DuckDbType })],
                    StoreQueryElapsedMs = storeStopwatch.Elapsed.TotalMilliseconds,
                    WriteElapsedMs = writeStopwatch.Elapsed.TotalMilliseconds,
                    MissingContentCount = missingContentCount,
                    KnownKeysSkipped = knownKeysSkipped,
                    OversizedPlanRows = oversizedPlanRows,
                    ProcedurePlanReuse = reuseStats,
                    Timings = timingSnapshot,
                };

                var featuresEntry = DimensionTableExporter.BuildCompanionEntry(
                    options, PlanFeatureTables.PlanFeatures, featuresOutputPath, featureRowCount, featureColumns,
                    storeStopwatch, writeStopwatch, featureDigestsParsed, 0, part: "procedure", timings: timingSnapshot);
                var warningsEntry = DimensionTableExporter.BuildCompanionEntry(
                    options, PlanFeatureTables.PlanWarnings, warningsOutputPath, warningRowCount, warningColumns,
                    storeStopwatch, writeStopwatch, null, null, part: "procedure", timings: timingSnapshot);
                var missingIndexesEntry = DimensionTableExporter.BuildCompanionEntry(
                    options, PlanFeatureTables.PlanMissingIndexes, missingIndexesOutputPath, missingIndexRowCount, missingIndexColumns,
                    storeStopwatch, writeStopwatch, null, null, part: "procedure", timings: timingSnapshot);

                PendingKeyRecorder.PendingProcedurePlanKeys[options.Store] = writtenKeys;
                PendingKeyRecorder.PendingProcedureDigests[options.Store] = recordedProcedureDigests;
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
    /// Fetches and resolves one example digest's plan content: one store call, paced like every other content fetch
    /// (the pacer sleeps after it) and counted in <paramref name="timings"/>. Null when the store has no content.
    /// <paramref name="fetch"/> is the store call, injectable so a test can count pacer runs without a store.
    /// </summary>
    internal static async Task<string?> FetchExampleContentAsync(
        ExportTimings timings, ContentFetchPacer pacer,
        Func<List<byte[]>, Task<Dictionary<string, (string?, byte[]?)>>> fetch, byte[] digest, CancellationToken ct)
    {
        var fetched = await timings.PacedContentFetchAsync(pacer, () => fetch([digest]), ct).ConfigureAwait(false);
        var (xml, gz) = fetched.TryGetValue(DimensionContent.ToHexDigest(digest), out var found) ? found : (null, null);
        return timings.Parse(() => DimensionContent.ResolvePlanXml(xml, gz));
    }

    private static string? NullableDuckString(DuckDBDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static void AppendNullableString(IDuckDBAppenderRow row, string? value)
    {
        if (value is null) row.AppendNullValue();
        else row.AppendValue(value);
    }

}
