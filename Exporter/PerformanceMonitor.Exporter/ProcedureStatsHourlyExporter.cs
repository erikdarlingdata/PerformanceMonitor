using System.Diagnostics;
using System.Security.Cryptography;
using DuckDB.NET.Data;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Exports the `procedure_stats_hourly` window table from the shared run-scoped
/// procedure_stats raw window (<see cref="ProcedureStatsWindowCache"/>) rather than a
/// dedicated store read: it collapses the store-side hourly rollup (<see
/// cref="SqlBuilder.ProcedureStatsHourlySql"/>) into <see
/// cref="SqlBuilder.ProcedureStatsHourlyDuckDbSql"/>, run in DuckDB over
/// `procedure_stats_raw`. COPYs the result to Parquet with today's ordering (source_store,
/// then <see cref="SqlBuilder.TimeColumn"/>). Mirrors <see cref="QueryStatsHourlyExporter"/>'s
/// shape.
/// </summary>
public static class ProcedureStatsHourlyExporter
{
    public static async Task<ManifestTableEntry> ExportAsync(
        ProcedureStatsWindowCache windowCache, ExportOptions options, CancellationToken ct)
    {
        const string table = HourlyTables.ProcedureStatsHourly;
        var outputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, table, options.FromUtc, options.ToUtc);
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);

        var storeStopwatch = Stopwatch.StartNew();
        var rawTempDbPath = await windowCache.GetOrBuildAsync(ct).ConfigureAwait(false);
        storeStopwatch.Stop();

        long rowCount;
        var columns = new List<ManifestColumn>();
        var writeStopwatch = Stopwatch.StartNew();
        using (var duckConnection = DuckDbSession.Open(rawTempDbPath))
        {
            using (var rollupCommand = duckConnection.CreateCommand())
            {
                rollupCommand.CommandText = $"CREATE TABLE procedure_stats_hourly_rollup AS {SqlBuilder.ProcedureStatsHourlyDuckDbSql}";
                rollupCommand.ExecuteNonQuery();
            }

            using (var addSourceStoreCommand = duckConnection.CreateCommand())
            {
                addSourceStoreCommand.CommandText =
                    $"ALTER TABLE procedure_stats_hourly_rollup ADD COLUMN source_store VARCHAR DEFAULT '{options.Store.Replace("'", "''")}'";
                addSourceStoreCommand.ExecuteNonQuery();
            }

            using (var countCommand = duckConnection.CreateCommand())
            {
                countCommand.CommandText = "SELECT count(*) FROM procedure_stats_hourly_rollup";
                rowCount = (long)(countCommand.ExecuteScalar() ?? 0L);
            }

            using var copyCommand = duckConnection.CreateCommand();
            // Same output shape/ordering as today's TableExporter path: source_store first,
            // then ORDER BY (server_name, hour).
            copyCommand.CommandText =
                "COPY (SELECT source_store, * EXCLUDE (source_store) FROM procedure_stats_hourly_rollup " +
                "ORDER BY server_name, hour) " +
                $"TO '{outputPath.Replace("'", "''")}' ({DuckDbSession.ParquetCopyOptions})";
            copyCommand.ExecuteNonQuery();

            // The manifest's column list is read back from the written file, so it cannot drift from it.
            using var describeCommand = duckConnection.CreateCommand();
            describeCommand.CommandText =
                $"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('{outputPath.Replace("'", "''")}'))";
            using var describeReader = describeCommand.ExecuteReader();
            while (describeReader.Read())
            {
                columns.Add(new ManifestColumn { Name = describeReader.GetString(0), DuckDbType = describeReader.GetString(1) });
            }
        }
        writeStopwatch.Stop();

        var byteSize = new FileInfo(outputPath).Length;
        var sha256 = ComputeSha256(outputPath);

        return new ManifestTableEntry
        {
            Table = table,
            OutputFile = OutputPaths.RelativeParquetPath(options.Store, table, options.FromUtc, options.ToUtc),
            RowCount = rowCount,
            ByteSize = byteSize,
            Sha256 = sha256,
            Columns = columns,
            StoreQueryElapsedMs = storeStopwatch.Elapsed.TotalMilliseconds,
            WriteElapsedMs = writeStopwatch.Elapsed.TotalMilliseconds,
            MissingContentCount = 0,
        };
    }

    private static string ComputeSha256(string path)
    {
        using var stream = File.OpenRead(path);
        var hash = SHA256.HashData(stream);
        return Convert.ToHexStringLower(hash);
    }
}
