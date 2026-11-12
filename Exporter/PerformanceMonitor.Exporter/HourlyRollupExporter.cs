using System.Data.Common;
using System.Security.Cryptography;
using DuckDB.NET.Data;

namespace PerformanceMonitor.Exporter;

/// <summary>One Parquet part written for an hourly-rollup table's export.</summary>
public sealed record HourlyRollupPart(string RelativePath, long RowCount, long ByteSize, string Sha256);

/// <summary>
/// The result of one <see cref="HourlyRollupExporter.ExportAsync"/> call: the single rolled-up part written, and the
/// raw row count read (never written out).
/// </summary>
public sealed class HourlyRollupExportResult
{
    public required long RawRowCount { get; init; }
    public required long RolledRowCount { get; init; }
    public required HourlyRollupPart Part { get; init; }
    public required double StoreQueryElapsedMs { get; init; }
    public required double WriteElapsedMs { get; init; }
}

/// <summary>
/// Exports one hourly-rollup optional table: the window's single raw read is appended into an in-memory DuckDB table
/// named <c>raw</c> (the same appender loop <see cref="OptionalTableExporter"/> uses), then <see
/// cref="RollupSql.Build"/> turns that into <c>rolled</c>, which is written out as exactly ONE Parquet part named
/// after the spec's <c>Name</c> (e.g. <c>pg_statement_stats_hourly</c>), ordered by <c>server_name, hour</c>. No raw
/// part is ever written for an Hourly-grain table.
/// </summary>
public static class HourlyRollupExporter
{
    /// <summary>Same Parquet settings as <see cref="OptionalTableExporter"/>'s parts.</summary>
    private const string PartParquetOptions = "FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000, ROW_GROUP_SIZE_BYTES '64MB'";

    /// <summary>
    /// How far before the window the read of a spec with Counter rules starts, so that each key's last sample before
    /// the window is in the read: the longest collector cycle (15 minutes) plus slack. Those rows feed only the
    /// counters' `_delta_hour_bridged` and are never counted (see <see cref="RollupSql.Build"/>).
    /// </summary>
    public static readonly TimeSpan CounterBridgeLookback = TimeSpan.FromMinutes(20);

    /// <summary>
    /// Reads <paramref name="reader"/>'s rows into a DuckDB table named <c>raw</c>, checks that every key and
    /// rule column named by <paramref name="spec"/> is present in the discovered shape, builds
    /// <c>rolled</c> via <see cref="RollupSql.Build"/>, and writes it as one sorted Parquet part under
    /// <paramref name="outDir"/>/<paramref name="store"/>/<paramref name="spec"/>.Name/. Rows before
    /// <paramref name="fromUtc"/> are bridge samples for the counters (see <see cref="CounterBridgeLookback"/>): they
    /// are read and appended, but only the counters' `_delta_hour_bridged` sees them.
    /// </summary>
    public static async Task<HourlyRollupExportResult> ExportAsync(
        RollupSpec spec,
        string store,
        string outDir,
        DateTime fromUtc,
        DateTime toUtc,
        Func<CancellationToken, Task<DbDataReader>> readerFactory,
        CancellationToken ct)
    {
        var storeStopwatch = System.Diagnostics.Stopwatch.StartNew();
        await using var reader = await readerFactory(ct).ConfigureAwait(false);

        // No `snapshot_time` column here -- an Hourly table is never a Snapshot kind.
        var columns = OptionalTableExporter.BuildColumns(reader, isSnapshot: false);
        var discoveredNames = columns.Select(c => c.Name).ToHashSet(StringComparer.Ordinal);

        var requiredColumns = new List<string> { "server_name", spec.TimeColumn };
        requiredColumns.AddRange(spec.Key);
        requiredColumns.AddRange(spec.Rules.SelectMany(RuleSourceColumns));
        if (spec.HasSampleInterval)
        {
            requiredColumns.Add("sample_interval_seconds");
        }

        var missing = requiredColumns.Distinct(StringComparer.Ordinal)
            .Where(c => !discoveredNames.Contains(c))
            .ToList();
        if (missing.Count > 0)
        {
            throw new InvalidOperationException(
                $"'{spec.Name}' is missing key/rule column(s) required by its rollup spec: {string.Join(", ", missing)}.");
        }

        var spillRoot = Path.Combine(Path.GetTempPath(), $"darling-export-hourly-{Guid.NewGuid():N}");
        Directory.CreateDirectory(spillRoot);
        try
        {
            using var connection = DuckDbSession.OpenInMemory(spillRoot);
            var columnDefs = string.Join(", ", columns.Select(c => $"{QuoteIdentifier(c.Name)} {c.DuckDbType}"));
            Execute(connection, $"CREATE TABLE raw ({columnDefs})");

            using (var appender = connection.CreateAppender("raw"))
            {
                var readerColumnCount = reader.FieldCount;
                long rawRowCount = 0;
                while (reader.Read())
                {
                    var row = appender.CreateRow();
                    row.AppendValue(store);
                    for (var i = 0; i < readerColumnCount; i++)
                    {
                        OptionalTableExporter.AppendColumnValue(row, reader, i, columns[i + 1].DuckDbType);
                    }

                    row.EndRow();
                    rawRowCount++;
                }

                appender.Close();
                storeStopwatch.Stop();

                var writeStopwatch = System.Diagnostics.Stopwatch.StartNew();
                var rollupSql = RollupSql.Build(spec, DateTime.SpecifyKind(fromUtc, DateTimeKind.Unspecified));
                Execute(connection, $"CREATE TABLE rolled AS {rollupSql}");
                // RollupSql emits source_store as a NULL placeholder; the store this export ran for fills it in.
                Execute(connection, $"UPDATE rolled SET source_store = '{store.Replace("'", "''")}'");

                var relativePath = OutputPaths.RelativeParquetPath(store, spec.Name, fromUtc, toUtc);
                var absolutePath = Path.Combine(outDir, store, relativePath.Replace('/', Path.DirectorySeparatorChar));
                Directory.CreateDirectory(Path.GetDirectoryName(absolutePath)!);

                Execute(connection,
                    $"COPY (SELECT * FROM rolled ORDER BY server_name, hour) TO '{absolutePath.Replace("'", "''")}' ({PartParquetOptions})");

                var rolledRowCount = Convert.ToInt64(
                    ExecuteScalar(connection, "SELECT count(*) FROM rolled"), System.Globalization.CultureInfo.InvariantCulture);

                var byteSize = new FileInfo(absolutePath).Length;
                var sha256 = ComputeSha256(absolutePath);
                writeStopwatch.Stop();

                return new HourlyRollupExportResult
                {
                    RawRowCount = rawRowCount,
                    RolledRowCount = rolledRowCount,
                    Part = new HourlyRollupPart(relativePath, rolledRowCount, byteSize, sha256),
                    StoreQueryElapsedMs = storeStopwatch.Elapsed.TotalMilliseconds,
                    WriteElapsedMs = writeStopwatch.Elapsed.TotalMilliseconds,
                };
            }
        }
        finally
        {
            try
            {
                if (Directory.Exists(spillRoot))
                {
                    Directory.Delete(spillRoot, recursive: true);
                }
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }

    /// <summary>The source column name(s) a rule needs present in the raw read, stripping any `"col=alias"` pin
    /// and, for <see cref="RollupRuleKind.ArgMaxBy"/>, treating every entry (order column included) as a source.</summary>
    private static IEnumerable<string> RuleSourceColumns(RollupRule rule) =>
        rule.Columns.Select(c =>
        {
            var i = c.IndexOf('=');
            return i < 0 ? c : c[..i];
        });

    private static string QuoteIdentifier(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    private static void Execute(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static object? ExecuteScalar(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar();
    }

    private static string ComputeSha256(string path)
    {
        using var stream = File.OpenRead(path);
        var hash = SHA256.HashData(stream);
        return Convert.ToHexStringLower(hash);
    }
}
