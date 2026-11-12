using System.Data.Common;
using System.Diagnostics;
using System.Security.Cryptography;
using DuckDB.NET.Data;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Exports a Daily-grain, Window-kind optional table: the last complete collection cycle per server before 00:00Z on
/// the run's own window ("the daily snapshot"). This is a sibling of <see cref="OptionalTableExporter"/> (the
/// Event-grain / Snapshot-kind path used by `servers`), not a replacement for it: <see cref="OptionalTableRunner"/>
/// dispatches Daily+Window tables here. The read range is NOT the caller's window: it is <c>[from - DailyLookbackHours
/// h, from)</c>, where <c>from</c> is the window start (00:00Z). <paramref name="readerFactory"/> is called with that
/// range already applied to the SQL by the caller (<c>SqlBuilder.BuildOptionalSelect</c>'s existing
/// <c>@from</c>/<c>@to</c> placeholders), so this type never builds SQL itself -- it only shapes the appended rows in
/// DuckDB and decides whether to run at all.
/// </summary>
public static class DailySnapshotExporter
{
    private const string PartParquetOptions = "FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000, ROW_GROUP_SIZE_BYTES '64MB'";

    /// <summary>
    /// Exports <paramref name="table"/>'s daily snapshot for the UTC day of <paramref name="options"/>.FromUtc: the
    /// read range ends at that day's 00:00 and the rows are stamped with it, whichever window of the day runs it. The
    /// 00:00 window always runs it. A later window of the day runs it only when <paramref name="dayAlreadyExported"/>
    /// is false (no run has produced the day's part yet); otherwise it returns a skip without ever calling
    /// <paramref name="readerFactory"/>.
    /// </summary>
    public static async Task<OptionalTableExportResult> ExportAsync(
        OptionalTable table,
        ExportOptions options,
        string store,
        string outDir,
        Func<DateTime, DateTime, CancellationToken, Task<DbDataReader>> readerFactory,
        CancellationToken ct,
        bool dayAlreadyExported = false)
    {
        var day = options.FromUtc.Date;
        if (!OptionalTableExporter.IsDailySnapshotWindow(options.FromUtc) && dayAlreadyExported)
        {
            return new OptionalTableExportResult
            {
                Table = table.Name,
                RowCount = 0,
                Parts = [],
                StoreQueryElapsedMs = 0,
                WriteElapsedMs = 0,
                SkippedReason = $"skipped: the daily snapshot for {day:yyyy-MM-dd} is already exported",
            };
        }

        var lookbackHours = table.DailyLookbackHours
            ?? throw new InvalidOperationException($"Daily-grain table '{table.Name}' has no DailyLookbackHours.");

        var readTo = day;
        var readFrom = readTo.AddHours(-lookbackHours);

        var tableDir = Path.Combine(outDir, store, table.Name);
        Directory.CreateDirectory(tableDir);

        var storeStopwatch = Stopwatch.StartNew();
        await using var reader = await readerFactory(readFrom, readTo, ct).ConfigureAwait(false);

        var columns = OptionalTableExporter.BuildColumns(reader, isSnapshot: false);
        var readerColumnCount = reader.FieldCount;

        using var rawWriter = DuckDbSession.OpenInMemory(Path.Combine(Path.GetTempPath(), $"darling-export-daily-{Guid.NewGuid():N}"));
        var columnDefs = string.Join(", ", columns.Select(c => $"{QuoteIdentifier(c.Name)} {c.DuckDbType}"));
        Execute(rawWriter, $"CREATE TABLE raw ({columnDefs})");

        long rowCount = 0;
        using (var appender = rawWriter.CreateAppender("raw"))
        {
            while (reader.Read())
            {
                var row = appender.CreateRow();
                row.AppendValue(store);
                for (var i = 0; i < readerColumnCount; i++)
                {
                    OptionalTableExporter.AppendColumnValue(row, reader, i, columns[i + 1].DuckDbType);
                }

                row.EndRow();
                rowCount++;
            }
        }

        storeStopwatch.Stop();
        var writeStopwatch = Stopwatch.StartNew();

        // The last complete cycle per server: every row whose collection_time equals that server's max collection_time
        // in the read range, stamped with the day's 00:00 as snapshot_time.
        Execute(rawWriter,
            "CREATE TABLE snap AS " +
            "SELECT r.*, CAST(? AS TIMESTAMP) AS snapshot_time " +
            "FROM raw r " +
            "JOIN (SELECT server_name, max(collection_time) AS ct FROM raw GROUP BY server_name) m " +
            "  ON r.server_name = m.server_name AND r.collection_time = m.ct",
            DateTime.SpecifyKind(day, DateTimeKind.Unspecified));

        var relativePath = OutputPaths.RelativeParquetPath(store, table.Name, readFrom, readTo, part: "p00000");
        var absolutePath = Path.Combine(outDir, store, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(absolutePath)!);

        Execute(rawWriter,
            $"COPY (SELECT * FROM snap ORDER BY server_name, collection_time) TO '{absolutePath.Replace("'", "''")}' ({PartParquetOptions})");

        writeStopwatch.Stop();

        var snapRowCount = (long)(ExecuteScalar(rawWriter, "SELECT count(*) FROM snap") ?? 0L);
        var byteSize = new FileInfo(absolutePath).Length;
        var sha256 = ComputeSha256(absolutePath);

        return new OptionalTableExportResult
        {
            Table = table.Name,
            RowCount = snapRowCount,
            Parts = [new OptionalTablePart(relativePath, snapRowCount, byteSize, sha256)],
            StoreQueryElapsedMs = storeStopwatch.Elapsed.TotalMilliseconds,
            WriteElapsedMs = writeStopwatch.Elapsed.TotalMilliseconds,
            ReadFrom = readFrom,
            ReadTo = readTo,
        };
    }

    private static string QuoteIdentifier(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    private static void Execute(DuckDBConnection connection, string sql, params object?[] parameters)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var parameter in parameters)
        {
            var p = command.CreateParameter();
            p.Value = parameter;
            command.Parameters.Add(p);
        }

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
