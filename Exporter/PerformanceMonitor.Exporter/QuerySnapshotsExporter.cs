using System.Data.Common;
using System.Security.Cryptography;
using System.Text;
using DuckDB.NET.Data;
using Npgsql;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Exports `collect.query_snapshots`: a streaming read that never selects `live_query_plan`, drops
/// `query_plan`/`live_query_plan` from the fact row, and dedupes each row's `query_plan` into `plan_shape` (origin
/// `qsnap`, part suffix `qsnap`) and `plan_features`, the same as the other plan origins. The fact row's digest is
/// `example_digest` = SHA-256(UTF8(query_plan.Replace("\0", string.Empty))) -- Darling's own
/// <c>PayloadDimensions.Digest</c> recipe (SHA-256 of the UTF-8 bytes of the payload after
/// <c>PgCollectorRowWriter.StripEmbeddedNuls</c>), so it is directly comparable with a real `query_plan_dim.digest`
/// (the same recipe on both sides). NULL when `query_plan` is NULL or empty. Memory: at most one plan's text is held at a
/// time (a `query_plan` row is ~58 KB on a large store); the fact rows and any new plan_shape/plan_features rows
/// stream through <see cref="ChunkedParquetWriter"/>/<see cref="OptionalTableExporter"/>'s byte-bounded chunking.
/// </summary>
public static class QuerySnapshotsExporter
{
    /// <summary>The plan_shape/plan_features origin recorded for a query_snapshots-derived plan.</summary>
    public const string Origin = "qsnap";

    /// <summary>The Parquet file-name part suffix for this origin's plan_shape/plan_features/plan_warnings/plan_missing_indexes output.</summary>
    public const string Part = "qsnap";

    /// <summary>
    /// Darling's own content digest (`PayloadDimensions.Digest` + `PgCollectorRowWriter.StripEmbeddedNuls`):
    /// SHA-256 over the UTF-8 bytes of the payload with embedded NULs stripped, and NO other normalization.
    /// This is intentionally the exact two-line recipe so a query_snapshots plan's digest is directly
    /// comparable to a real `query_plan_dim.digest` written by the collector.
    /// </summary>
    public static byte[] DarlingDigest(string payload) =>
        SHA256.HashData(Encoding.UTF8.GetBytes(payload.Replace("\0", string.Empty)));

    /// <summary>
    /// Builds the query_snapshots SELECT: every optional-registry column of the fact table EXCEPT `query_plan` and
    /// `live_query_plan`, plus `query_plan` appended LAST so the reader can read it once per row without re-querying,
    /// tagged for pg_stat_statements. `live_query_plan` is NEVER in this SELECT list.
    /// </summary>
    public static string BuildSelect(IReadOnlyList<string> discoveredColumns)
    {
        var factColumns = discoveredColumns
            .Where(c => c is not "query_plan" and not "live_query_plan")
            .ToList();

        if (factColumns.Count == 0)
        {
            throw new ArgumentException("query_snapshots has no exportable columns.");
        }

        var quotedFact = factColumns.Select(SqlBuilder.QuoteIdentifier);
        var columnList = string.Join(", ", quotedFact.Append(SqlBuilder.QuoteIdentifier("query_plan")));

        return "/* darling-export:opt.query_snapshots */ SELECT " + columnList +
               " FROM collect.query_snapshots WHERE collection_time >= @from AND collection_time < @to";
    }

    /// <summary>
    /// Runs the streaming export: the fact rows (minus both plan columns, plus `query_plan_digest`)
    /// go through <see cref="OptionalTableExporter"/>'s byte-bounded writer; any new plan key's XML
    /// is parsed for features and written into its own `plan_shape`/`plan_features`/`plan_warnings`/
    /// `plan_missing_indexes` parts under origin/part <see cref="Origin"/>/<see cref="Part"/>.
    /// </summary>
    public static async Task<QuerySnapshotsExportResult> ExportAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder,
        ExportOptions options,
        IReadOnlyList<string> discoveredColumns,
        CancellationToken ct)
    {
        var sql = BuildSelect(discoveredColumns);
        var factColumns = discoveredColumns.Where(c => c is not "query_plan" and not "live_query_plan").ToList();

        await using var connection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter("from", NpgsqlTypes.NpgsqlDbType.Timestamp)
        {
            Value = DateTime.SpecifyKind(options.FromUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter("to", NpgsqlTypes.NpgsqlDbType.Timestamp)
        {
            Value = DateTime.SpecifyKind(options.ToUtc, DateTimeKind.Unspecified),
        });

        await using var reader = await command.ExecuteReaderAsync(System.Data.CommandBehavior.Default, ct).ConfigureAwait(false);
        var planColumnOrdinal = factColumns.Count; // last projected column is query_plan

        var factColumnShapes = new List<ColumnShape> { new("source_store", "VARCHAR") };
        for (var i = 0; i < factColumns.Count; i++)
        {
            var pgType = reader.GetDataTypeName(i);
            factColumnShapes.Add(new ColumnShape(factColumns[i], DuckDbTypeMapper.Map(factColumns[i], pgType)));
        }
        factColumnShapes.Add(new ColumnShape("query_plan_digest", "VARCHAR"));

        var tableDir = Path.Combine(options.OutDir, options.Store, "query_snapshots");
        Directory.CreateDirectory(tableDir);
        var factTempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-qsnap-fact-{Guid.NewGuid():N}.duckdb");
        var planTempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-qsnap-plan-{Guid.NewGuid():N}.duckdb");
        var featuresTempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-qsnap-feat-{Guid.NewGuid():N}.duckdb");
        var warningsTempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-qsnap-warn-{Guid.NewGuid():N}.duckdb");
        var missingIndexesTempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-qsnap-mi-{Guid.NewGuid():N}.duckdb");

        var planColumns = new List<ColumnShape>
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
        var featureColumns = DimensionTableExporter.FeatureColumns();
        var warningColumns = DimensionTableExporter.WarningColumns();
        var missingIndexColumns = DimensionTableExporter.MissingIndexColumns();

        long factRowCount = 0;
        long unparsedPlanRows = 0;
        long planRowCount = 0;
        long featureRowCount = 0;
        long warningRowCount = 0;
        long missingIndexRowCount = 0;

        // Only ONE plan text is held at a time: the reader loop below fetches the row (including its
        // query_plan text), computes/uses its digest, decides whether the key is new, appends the fact
        // row (dropping the plan text), and -- only if the key is new -- writes the plan_shape/features
        // rows right away, before moving to the next row.
        var seenKeysThisRun = new HashSet<(string?, string)>();

        using var stateStore = StateStore.Open(options.StateDir);
        var newPlanKeys = new List<(string? ServerName, string QueryPlanHash)>();
        var newFeatureDigests = new List<(string PlanDigest, int FeatureVersion)>();

        try
        {
            using var factWriter = new ChunkedParquetWriter(factTempDbPath, "query_snapshots", factColumnShapes);
            using var planWriter = new ChunkedParquetWriter(planTempDbPath, DimensionTables.PlanShape, planColumns);
            using var featuresWriter = new ChunkedParquetWriter(featuresTempDbPath, PlanFeatureTables.PlanFeatures, featureColumns);
            using var warningsWriter = new ChunkedParquetWriter(warningsTempDbPath, PlanFeatureTables.PlanWarnings, warningColumns);
            using var missingIndexesWriter = new ChunkedParquetWriter(missingIndexesTempDbPath, PlanFeatureTables.PlanMissingIndexes, missingIndexColumns);

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                string? serverName = null;
                string? queryPlanHash = null;
                string? queryPlanText = reader.IsDBNull(planColumnOrdinal) ? null : reader.GetString(planColumnOrdinal);

                var row = factWriter.Appender.CreateRow();
                row.AppendValue(options.Store);
                long rowBytes = 0;
                for (var i = 0; i < factColumns.Count; i++)
                {
                    if (string.Equals(factColumns[i], "server_name", StringComparison.Ordinal) && !reader.IsDBNull(i))
                    {
                        serverName = reader.GetString(i);
                    }
                    if (string.Equals(factColumns[i], "query_hash", StringComparison.Ordinal) && !reader.IsDBNull(i))
                    {
                        // query_snapshots carries query_hash, not query_plan_hash the plan key comes from the
                        // query_plan XML itself -- follow the plan's own QueryPlanHash statement attribute via
                        // PlanShapeHasher-style extraction).
                    }

                    rowBytes += AppendFactColumn(row, reader, i, factColumnShapes[i + 1].DuckDbType);
                }

                string? digestHex = null;
                if (!string.IsNullOrEmpty(queryPlanText))
                {
                    var digest = DarlingDigest(queryPlanText);
                    digestHex = DimensionContent.ToHexDigest(digest);
                    queryPlanHash = ExtractQueryPlanHash(queryPlanText);
                    if (queryPlanHash is null)
                    {
                        unparsedPlanRows++;
                    }
                }

                if (digestHex is null)
                {
                    row.AppendNullValue();
                }
                else
                {
                    row.AppendValue(digestHex);
                    rowBytes += Encoding.UTF8.GetByteCount(digestHex);
                }

                row.EndRow();
                factWriter.RowAppended(rowBytes);
                factRowCount++;

                if (queryPlanHash is null || queryPlanText is null)
                {
                    continue;
                }

                var key = (serverName, queryPlanHash);
                if (!seenKeysThisRun.Add(key))
                {
                    continue; // already handled earlier in this same run
                }

                var known = options.IgnoreState
                    ? new HashSet<(string?, string)>()
                    : stateStore.FilterUnknownPlanKeys(options.Store, [key]);
                if (!options.IgnoreState && known.Count == 0)
                {
                    continue; // StateStore already has this (server_name, query_plan_hash)
                }

                var planXmlBytes = (long)Encoding.UTF8.GetByteCount(queryPlanText);
                var planRow = planWriter.Appender.CreateRow();
                planRow.AppendValue(options.Store);
                AppendNullable(planRow, serverName);
                planRow.AppendValue(queryPlanHash);
                planRow.AppendValue(reader.GetDateTime(FindOrdinal(factColumns, "collection_time")));
                planRow.AppendValue(reader.GetDateTime(FindOrdinal(factColumns, "collection_time")));
                planRow.AppendValue(digestHex!);
                planRow.AppendValue(queryPlanText);
                planRow.AppendValue(planXmlBytes);
                planRow.AppendValue(Origin);
                planRow.EndRow();
                planWriter.RowAppended(planXmlBytes);
                planRowCount++;
                newPlanKeys.Add((serverName, queryPlanHash));

                var featureVersion = PlanFeatureExtractor.FeatureVersion;
                var alreadyParsed = !options.IgnoreState &&
                    stateStore.FilterUnknownFeatureDigests(options.Store, [digestHex!], featureVersion).Count == 0;
                if (!alreadyParsed)
                {
                    var result = PlanFeatureExtractor.Extract(digestHex!, queryPlanText);
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

                    newFeatureDigests.Add((digestHex!, featureVersion));
                }
            }

            var factOutputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, "query_snapshots", options.FromUtc, options.ToUtc);
            var planOutputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, DimensionTables.PlanShape, options.FromUtc, options.ToUtc, Part);
            var featuresOutputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, PlanFeatureTables.PlanFeatures, options.FromUtc, options.ToUtc, Part);
            var warningsOutputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, PlanFeatureTables.PlanWarnings, options.FromUtc, options.ToUtc, Part);
            var missingIndexesOutputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, PlanFeatureTables.PlanMissingIndexes, options.FromUtc, options.ToUtc, Part);

            foreach (var path in new[] { factOutputPath, planOutputPath, featuresOutputPath, warningsOutputPath, missingIndexesOutputPath })
            {
                Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            }

            factWriter.Complete(factOutputPath);
            planWriter.Complete(planOutputPath);
            featuresWriter.Complete(featuresOutputPath);
            warningsWriter.Complete(warningsOutputPath);
            missingIndexesWriter.Complete(missingIndexesOutputPath);

            if (!options.IgnoreState)
            {
                stateStore.RecordExportedKeys(options.Store, newPlanKeys, [], newFeatureDigests);
            }

            return new QuerySnapshotsExportResult(
                FactRowCount: factRowCount,
                FactOutputPath: factOutputPath,
                PlanRowCount: planRowCount,
                PlanOutputPath: planOutputPath,
                FeatureRowCount: featureRowCount,
                FeaturesOutputPath: featuresOutputPath,
                WarningRowCount: warningRowCount,
                WarningsOutputPath: warningsOutputPath,
                MissingIndexRowCount: missingIndexRowCount,
                MissingIndexesOutputPath: missingIndexesOutputPath,
                UnparsedPlanRows: unparsedPlanRows);
        }
        finally
        {
            DuckDbSession.DeleteTempDatabase(factTempDbPath);
            DuckDbSession.DeleteTempDatabase(planTempDbPath);
            DuckDbSession.DeleteTempDatabase(featuresTempDbPath);
            DuckDbSession.DeleteTempDatabase(warningsTempDbPath);
            DuckDbSession.DeleteTempDatabase(missingIndexesTempDbPath);
        }
    }

    private static int FindOrdinal(List<string> columns, string name) => columns.IndexOf(name);

    /// <summary>
    /// Pulls `query_plan_hash` from the first statement's own `QueryPlanHash` attribute. Reuses <see
    /// cref="PlanShapeHasher"/>'s uppercasing convention. Returns null (never throws) on any XML the forward-only
    /// reader cannot walk -- that row counts toward `unparsed_plan_rows` on the optional manifest.
    /// </summary>
    private static string? ExtractQueryPlanHash(string xml)
    {
        try
        {
            var settings = new System.Xml.XmlReaderSettings
            {
                DtdProcessing = System.Xml.DtdProcessing.Prohibit,
                XmlResolver = null,
            };
            using var stringReader = new StringReader(xml);
            using var xmlReader = System.Xml.XmlReader.Create(stringReader, settings);
            while (xmlReader.Read())
            {
                if (xmlReader.NodeType != System.Xml.XmlNodeType.Element)
                {
                    continue;
                }

                var value = xmlReader.GetAttribute("QueryPlanHash");
                if (!string.IsNullOrEmpty(value))
                {
                    return value.Length >= 2 && value[0] == '0' && (value[1] is 'x' or 'X')
                        ? "0x" + value[2..].ToUpperInvariant()
                        : value.ToUpperInvariant();
                }
            }
        }
        catch
        {
            // Malformed XML: no hash extracted, no exception propagated NULL-hash rule).
        }

        return null;
    }

    private static long AppendFactColumn(IDuckDBAppenderRow row, DbDataReader reader, int ordinal, string duckDbType)
    {
        if (reader.IsDBNull(ordinal))
        {
            row.AppendNullValue();
            return 0;
        }

        switch (duckDbType)
        {
            case "BOOLEAN":
                row.AppendValue(reader.GetBoolean(ordinal));
                return 8;
            case "SMALLINT":
                row.AppendValue(Convert.ToInt16(reader.GetValue(ordinal)));
                return 8;
            case "INTEGER":
                row.AppendValue(Convert.ToInt32(reader.GetValue(ordinal)));
                return 8;
            case "BIGINT":
                row.AppendValue(Convert.ToInt64(reader.GetValue(ordinal)));
                return 8;
            case "REAL":
                row.AppendValue(Convert.ToSingle(reader.GetValue(ordinal)));
                return 8;
            case "DOUBLE":
                row.AppendValue(Convert.ToDouble(reader.GetValue(ordinal)));
                return 8;
            case "VARCHAR":
            {
                var value = reader.GetValue(ordinal)?.ToString();
                row.AppendValue(value);
                return value is null ? 0 : Encoding.UTF8.GetByteCount(value);
            }
            case "TIMESTAMP":
            case "TIMESTAMPTZ":
                row.AppendValue(reader.GetDateTime(ordinal));
                return 8;
            case var decimalType when decimalType.StartsWith("DECIMAL(", StringComparison.Ordinal):
                row.AppendValue(reader.GetDecimal(ordinal));
                return 8;
            default:
                throw new NotSupportedException($"No appender case for DuckDB type '{duckDbType}' (query_snapshots).");
        }
    }

    private static void AppendNullable(IDuckDBAppenderRow row, string? value)
    {
        if (value is null) row.AppendNullValue(); else row.AppendValue(value);
    }
}

/// <summary>The result of one query_snapshots + derived plan_shape/plan_features export.</summary>
public sealed record QuerySnapshotsExportResult(
    long FactRowCount,
    string FactOutputPath,
    long PlanRowCount,
    string PlanOutputPath,
    long FeatureRowCount,
    string FeaturesOutputPath,
    long WarningRowCount,
    string WarningsOutputPath,
    long MissingIndexRowCount,
    string MissingIndexesOutputPath,
    long UnparsedPlanRows);
