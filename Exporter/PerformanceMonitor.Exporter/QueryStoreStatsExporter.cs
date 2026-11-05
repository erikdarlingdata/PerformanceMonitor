using System.Diagnostics;
using System.Security.Cryptography;
using DuckDB.NET.Data;
using Npgsql;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Exports the `query_store_stats` window table: dedups the raw window in DuckDB (see
/// <see cref="QueryStoreWindow"/>), drops server_id and the always-null query_text /
/// query_plan_text columns are already excluded upstream, and COPYs the result to Parquet
/// ordered by (server_name, interval_start_time_utc). Dedups from the shared run-scoped raw
/// window (<see cref="QueryStoreRawWindowCache"/>) rather than reading the store itself, so
/// this export never causes a second raw read when plan_shape/query_text already triggered
/// the cache's build.
/// </summary>
public static class QueryStoreStatsExporter
{
    public static async Task<ManifestTableEntry> ExportAsync(
        QueryStoreRawWindowCache rawWindowCache, ExportOptions options, CancellationToken ct)
    {
        var outputPath = OutputPaths.ParquetFilePath(
            options.OutDir, options.Store, QueryStoreTables.QueryStoreStats, options.FromUtc, options.ToUtc);
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);

        var storeStopwatch = Stopwatch.StartNew();
        var rawTempDbPath = await rawWindowCache.GetOrBuildAsync(ct).ConfigureAwait(false);
        storeStopwatch.Stop();

        // Set when the window build ran with --query-store-source wide|compare; null in raw mode.
        var sourceReport = rawWindowCache.SourceReport;

        long rowCount;
        var columns = new List<ManifestColumn>();
        var writeStopwatch = Stopwatch.StartNew();
        using (var duckConnection = DuckDbSession.Open(rawTempDbPath))
        {
            using (var dedupCommand = duckConnection.CreateCommand())
            {
                dedupCommand.CommandText =
                    $"CREATE TABLE query_store_dedup AS {SqlBuilder.BuildQueryStoreDedupSql("query_store_raw")}";
                dedupCommand.ExecuteNonQuery();
            }

            // The wide table does not count snapshots (it keeps only the latest), so rows that came from it carry NULL.
            // The dedup's own count would be 1 for every row, which is a fact about this read, not about the interval.
            if (sourceReport is { Source: "wide" })
            {
                using var nullSnapshotCount = duckConnection.CreateCommand();
                nullSnapshotCount.CommandText = "UPDATE query_store_dedup SET snapshot_count = NULL";
                nullSnapshotCount.ExecuteNonQuery();
            }

            using (var addSourceStoreCommand = duckConnection.CreateCommand())
            {
                addSourceStoreCommand.CommandText =
                    $"ALTER TABLE query_store_dedup ADD COLUMN source_store VARCHAR DEFAULT '{options.Store.Replace("'", "''")}'";
                addSourceStoreCommand.ExecuteNonQuery();
            }

            using (var countCommand = duckConnection.CreateCommand())
            {
                countCommand.CommandText = "SELECT count(*) FROM query_store_dedup";
                rowCount = (long)(countCommand.ExecuteScalar() ?? 0L);
            }

            using var copyCommand = duckConnection.CreateCommand();
            // server_id never leaves this table (local to one store); collection_id was
            // never selected into the raw table at all. last_collection_time/
            // snapshot_count come from the dedup step itself. source_store goes first, as in
            // every other exported table.
            copyCommand.CommandText =
                "COPY (SELECT source_store, * EXCLUDE (server_id, source_store) FROM query_store_dedup " +
                "ORDER BY server_name, interval_start_time_utc) " +
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
            Table = QueryStoreTables.QueryStoreStats,
            OutputFile = OutputPaths.RelativeParquetPath(options.Store, QueryStoreTables.QueryStoreStats, options.FromUtc, options.ToUtc),
            RowCount = rowCount,
            ByteSize = byteSize,
            Sha256 = sha256,
            Columns = columns,
            // "Store time for the read the output came from": the wide read when the rows are wide, else the raw read.
            // Absent a report (raw mode) this is the cache wait, as before.
            StoreQueryElapsedMs = (sourceReport?.Source == "wide" ? sourceReport.WideReadMs : sourceReport?.RawReadMs)
                ?? storeStopwatch.Elapsed.TotalMilliseconds,
            QueryStoreSource = sourceReport?.ToStats(),
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
