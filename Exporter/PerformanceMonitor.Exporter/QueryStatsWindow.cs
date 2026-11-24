using System.Data;
using DuckDB.NET.Data;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// loads one export window's `collect.query_stats` rows into an on-disk DuckDB temp file,
/// ONE store read shared by `query_stats_hourly`, `plan_shape` and `query_text` (origin
/// query_stats), replacing the three separate store-side passes that used to read
/// `collect.query_stats` three times per window. Mirrors <see
/// cref="QueryStoreWindow"/> shape: a plain projection, streamed into DuckDB via the
/// appender, with every GROUP BY/min/max derived from the cached table afterward.
/// </summary>
public static class QueryStatsWindow
{
    /// <summary>Column names of the raw `collect.query_stats` select, in the same order <see cref="SqlBuilder.QueryStatsWindowSql"/> lists them.</summary>
    public static readonly string[] RawColumns =
    [
        "server_name", "database_name", "query_hash", "query_plan_hash", "host_object_name", "collection_time",
        "sample_interval_seconds",
        "delta_execution_count", "delta_worker_time", "delta_elapsed_time",
        "delta_logical_reads", "delta_logical_writes", "delta_physical_reads",
        "delta_rows", "delta_spills",
        "query_plan_digest", "query_text_digest",
        // DMV attributes, carried into query_stats_hourly as the value of the hour's last counted sample.
        "creation_time", "last_execution_time", "plan_generation_num", "sql_handle", "plan_handle",
        "statement_start_offset", "statement_end_offset", "execution_count", "total_worker_time",
        "total_elapsed_time", "total_logical_reads", "total_logical_writes", "total_physical_reads",
        "total_clr_time", "total_rows", "total_spills", "min_worker_time", "max_worker_time", "min_elapsed_time",
        "max_elapsed_time", "min_physical_reads", "max_physical_reads", "min_rows", "max_rows", "min_dop", "max_dop",
        "min_grant_kb", "max_grant_kb", "min_used_grant_kb", "max_used_grant_kb", "min_ideal_grant_kb",
        "max_ideal_grant_kb", "min_reserved_threads", "max_reserved_threads", "min_used_threads", "max_used_threads",
        "min_spills", "max_spills", "query_plan_xml_bytes",
    ];

    private static readonly Dictionary<string, string> RawColumnTypes = new()
    {
        ["server_name"] = "VARCHAR",
        ["database_name"] = "VARCHAR",
        ["query_hash"] = "VARCHAR",
        ["query_plan_hash"] = "VARCHAR",
        ["host_object_name"] = "VARCHAR",
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
        ["creation_time"] = "TIMESTAMP",
        ["last_execution_time"] = "TIMESTAMP",
        ["plan_generation_num"] = "BIGINT",
        ["sql_handle"] = "VARCHAR",
        ["plan_handle"] = "VARCHAR",
        ["statement_start_offset"] = "INTEGER",
        ["statement_end_offset"] = "INTEGER",
        ["execution_count"] = "BIGINT",
        ["total_worker_time"] = "BIGINT",
        ["total_elapsed_time"] = "BIGINT",
        ["total_logical_reads"] = "BIGINT",
        ["total_logical_writes"] = "BIGINT",
        ["total_physical_reads"] = "BIGINT",
        ["total_clr_time"] = "BIGINT",
        ["total_rows"] = "BIGINT",
        ["total_spills"] = "BIGINT",
        ["min_worker_time"] = "BIGINT",
        ["max_worker_time"] = "BIGINT",
        ["min_elapsed_time"] = "BIGINT",
        ["max_elapsed_time"] = "BIGINT",
        ["min_physical_reads"] = "BIGINT",
        ["max_physical_reads"] = "BIGINT",
        ["min_rows"] = "BIGINT",
        ["max_rows"] = "BIGINT",
        ["min_dop"] = "BIGINT",
        ["max_dop"] = "BIGINT",
        ["min_grant_kb"] = "BIGINT",
        ["max_grant_kb"] = "BIGINT",
        ["min_used_grant_kb"] = "BIGINT",
        ["max_used_grant_kb"] = "BIGINT",
        ["min_ideal_grant_kb"] = "BIGINT",
        ["max_ideal_grant_kb"] = "BIGINT",
        ["min_reserved_threads"] = "BIGINT",
        ["max_reserved_threads"] = "BIGINT",
        ["min_used_threads"] = "BIGINT",
        ["max_used_threads"] = "BIGINT",
        ["min_spills"] = "BIGINT",
        ["max_spills"] = "BIGINT",
        ["query_plan_xml_bytes"] = "BIGINT",
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
