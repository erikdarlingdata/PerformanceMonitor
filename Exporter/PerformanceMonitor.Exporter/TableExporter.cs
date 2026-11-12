using System.Data;
using System.Diagnostics;
using System.Security.Cryptography;
using DuckDB.NET.Data;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Exporter;

/// <summary>One column's shape as discovered from the Npgsql reader, before it lands in DuckDB.</summary>
public sealed record ColumnShape(string Name, string DuckDbType);

/// <summary>
/// Exports one table for one window: runs the store query, streams rows into an on-disk
/// DuckDB temp file via the appender, then COPYs that table out to a Parquet file ordered
/// by (server_name, time column). The on-disk temp file (never in-memory) is deliberate:
/// raw tables can be tens of millions of rows for a wide window.
/// </summary>
public sealed class TableExporter
{
    /// <summary>
    /// Runs the store query for <paramref name="table"/>, writes it to Parquet, and returns
    /// the manifest entry for it. Throws on any failure; the caller must not write a manifest
    /// for a run where any table failed.
    /// </summary>
    public static async Task<ManifestTableEntry> ExportAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder,
        ExportOptions options,
        string table,
        IReadOnlyList<string>? rawColumns,
        CancellationToken ct)
    {
        var sql = SqlBuilder.BuildSelect(table, rawColumns);
        var timeColumn = SqlBuilder.TimeColumn(table);
        var outputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, table, options.FromUtc, options.ToUtc);
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);

        var tempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-{Guid.NewGuid():N}.duckdb");

        try
        {
            var storeStopwatch = Stopwatch.StartNew();

            // The chunk guard is now a run-level pre-flight in ExportRunner.RunChunkGuardPreflightAsync, run before
            // this or any other core table's read.
            await using var pgConnection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);

            await using var command = new NpgsqlCommand(sql, pgConnection);
            // Store timestamps are naive UTC `timestamp`; see SqlBuilder.WindowParameters.
            command.Parameters.AddRange(SqlBuilder.WindowParameters(options.FromUtc, options.ToUtc));

            await using var reader = await command.ExecuteReaderAsync(RawWindowAppender.Behavior, ct).ConfigureAwait(false);

            var columns = new List<ColumnShape>(reader.FieldCount + 1)
            {
                new("source_store", "VARCHAR"),
            };
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var name = reader.GetName(i);
                var pgType = reader.GetDataTypeName(i);
                columns.Add(new ColumnShape(name, DuckDbTypeMapper.Map(name, pgType)));
            }

            long rowCount;
            using (var duckConnection = DuckDbSession.Open(tempDbPath))
            {
                CreateDuckDbTable(duckConnection, table, columns);
                rowCount = await RawWindowAppender.AppendAllAsync(
                    reader, duckConnection, table, [.. columns.Skip(1).Select(c => c.DuckDbType)], ct, options.Store).ConfigureAwait(false);
            }

            storeStopwatch.Stop();

            var writeStopwatch = Stopwatch.StartNew();
            using (var duckConnection = DuckDbSession.Open(tempDbPath))
            {
                CopyToParquet(duckConnection, table, timeColumn, outputPath);
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
                Columns = [.. columns.Select(c => new ManifestColumn { Name = c.Name, DuckDbType = c.DuckDbType })],
                StoreQueryElapsedMs = storeStopwatch.Elapsed.TotalMilliseconds,
                WriteElapsedMs = writeStopwatch.Elapsed.TotalMilliseconds,
                MissingContentCount = 0,
            };
        }
        finally
        {
            DuckDbSession.DeleteTempDatabase(tempDbPath);
        }
    }

    private static void CreateDuckDbTable(DuckDBConnection connection, string table, List<ColumnShape> columns)
    {
        var columnDefs = string.Join(", ", columns.Select(c => $"{QuoteDuckIdentifier(c.Name)} {c.DuckDbType}"));
        using var command = connection.CreateCommand();
        command.CommandText = $"CREATE TABLE {QuoteDuckIdentifier(table)} ({columnDefs})";
        command.ExecuteNonQuery();
    }

    private static void CopyToParquet(DuckDBConnection connection, string table, string timeColumn, string outputPath)
    {
        using var command = connection.CreateCommand();
        // Ordered by (server_name, time column) so downstream row-group pruning by server
        // and time is effective; row group size is fixed by the exporter.
        command.CommandText =
            $"COPY (SELECT * FROM {QuoteDuckIdentifier(table)} ORDER BY server_name, {QuoteDuckIdentifier(timeColumn)}) " +
            $"TO '{outputPath.Replace("'", "''")}' ({DuckDbSession.ParquetCopyOptions})";
        command.ExecuteNonQuery();
    }

    private static string ComputeSha256(string path)
    {
        using var stream = File.OpenRead(path);
        var hash = SHA256.HashData(stream);
        return Convert.ToHexStringLower(hash);
    }

    private static string QuoteDuckIdentifier(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";
}
