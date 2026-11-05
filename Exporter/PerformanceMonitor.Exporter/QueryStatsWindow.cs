using System.Data;
using DuckDB.NET.Data;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// loads one export window's `collect.query_stats` rows into an on-disk DuckDB temp file,
/// ONE store read shared by `query_stats_hourly`, `plan_shape` and `query_text` (origin
/// query_stats), replacing the three separate store-side passes that used to read
/// `collect.query_stats` three times per window (<see cref="SqlBuilder.QueryStatsHourlySql"/>,
/// <see cref="SqlBuilder.PlanShapeKeySql"/>, <see cref="SqlBuilder.QueryTextKeySql"/> --
/// all three stay in <see cref="SqlBuilder"/> only as test oracles). Mirrors <see
/// cref="QueryStoreWindow"/> shape: a plain projection, streamed into DuckDB via the
/// appender, with every GROUP BY/min/max derived from the cached table afterward.
/// </summary>
public static class QueryStatsWindow
{
    /// <summary>Column names of the raw `collect.query_stats` select, in the same order <see cref="SqlBuilder.QueryStatsWindowSql"/> lists them.</summary>
    public static readonly string[] RawColumns =
    [
        "server_name", "database_name", "query_hash", "query_plan_hash", "collection_time",
        "sample_interval_seconds",
        "delta_execution_count", "delta_worker_time", "delta_elapsed_time",
        "delta_logical_reads", "delta_logical_writes", "delta_physical_reads",
        "delta_rows", "delta_spills",
        "query_plan_digest", "query_text_digest",
    ];

    private static readonly Dictionary<string, string> RawColumnTypes = new()
    {
        ["server_name"] = "VARCHAR",
        ["database_name"] = "VARCHAR",
        ["query_hash"] = "VARCHAR",
        ["query_plan_hash"] = "VARCHAR",
        ["collection_time"] = "TIMESTAMP",
        ["sample_interval_seconds"] = "INTEGER",
        ["delta_execution_count"] = "BIGINT",
        ["delta_worker_time"] = "BIGINT",
        ["delta_elapsed_time"] = "BIGINT",
        ["delta_logical_reads"] = "BIGINT",
        ["delta_logical_writes"] = "BIGINT",
        ["delta_physical_reads"] = "BIGINT",
        ["delta_rows"] = "BIGINT",
        ["delta_spills"] = "BIGINT",
        ["query_plan_digest"] = "BLOB",
        ["query_text_digest"] = "BLOB",
    };

    internal static string DuckDbTypeFor(string column) => RawColumnTypes[column];

    /// <summary>
    /// Opens a fresh on-disk DuckDB file and streams the raw window from the store into a
    /// `query_stats_raw` table. Runs <see cref="QueryStoreWindow.AssertCCollationAsync"/>
    /// first, on the same connection, before a single row is read: the DuckDB-side hourly
    /// rollup and key queries group and min/max text-bearing columns, and this is the one
    /// place every consumer of the window (query_stats_hourly, plan_shape, query_text) passes
    /// through, so checking here means every consumer is covered without repeating the check.
    /// Caller owns and must delete the returned temp file path.
    /// </summary>
    public static async Task<string> BuildRawWindowFileAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, ExportOptions options, CancellationToken ct)
    {
        var tempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-qsw-{Guid.NewGuid():N}.duckdb");

        await using var pgConnection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);

        await QueryStoreWindow.AssertCCollationAsync(pgConnection, ct).ConfigureAwait(false);

        // The chunk guard is now a run-level pre-flight in ExportRunner.RunChunkGuardPreflightAsync, run before this
        // or any other core table's read.
        await using var command = new NpgsqlCommand(SqlBuilder.QueryStatsWindowSql, pgConnection);
        command.Parameters.Add(new NpgsqlParameter("from", NpgsqlDbType.Timestamp)
        {
            Value = DateTime.SpecifyKind(options.FromUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter("to", NpgsqlDbType.Timestamp)
        {
            Value = DateTime.SpecifyKind(options.ToUtc, DateTimeKind.Unspecified),
        });

        await using var reader = await command.ExecuteReaderAsync(RawWindowAppender.Behavior, ct).ConfigureAwait(false);

        using var duckConnection = DuckDbSession.Open(tempDbPath);

        var columnDefs = string.Join(", ", RawColumns.Select(c => $"\"{c}\" {DuckDbTypeFor(c)}"));
        using (var createCommand = duckConnection.CreateCommand())
        {
            createCommand.CommandText = $"CREATE TABLE query_stats_raw ({columnDefs})";
            createCommand.ExecuteNonQuery();
        }

        await RawWindowAppender.AppendAllAsync(reader, duckConnection, "query_stats_raw",
            RawColumns.Select(DuckDbTypeFor).ToArray(), ct).ConfigureAwait(false);

        return tempDbPath;
    }

    public static void TryDeleteTempFile(string tempDbPath) => DuckDbSession.DeleteTempDatabase(tempDbPath);
}
