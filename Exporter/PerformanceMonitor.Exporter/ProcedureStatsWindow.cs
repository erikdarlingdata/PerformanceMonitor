using System.Data;
using DuckDB.NET.Data;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// loads one export window's `collect.procedure_stats` rows into an on-disk DuckDB temp file, ONE store read
/// shared by `procedure_stats_hourly`, the procedure plan keys and the oversized-plan count, replacing the three
/// separate store-side passes that used to read `collect.procedure_stats` three times per window (<see
/// cref="SqlBuilder.ProcedureStatsHourlySql"/>, <see cref="SqlBuilder.ProcedurePlanKeySql"/>, <see
/// cref="SqlBuilder.ProcedurePlanOversizedCountSql"/> -- all three stay in <see cref="SqlBuilder"/> only as test
/// oracles). Mirrors <see cref="QueryStatsWindow"/>: a plain projection, streamed into DuckDB via the
/// appender, with every GROUP BY/min/max derived from the cached table afterward. The projection never carries
/// `query_plan_xml`/`query_plan_xml_bytes` content itself: only needs their nullness, and `IS NULL`
/// on a TOASTed column checks the null bitmap without detoasting, so the two booleans read no TOAST.
/// </summary>
public static class ProcedureStatsWindow
{
    /// <summary>Column names of the raw `collect.procedure_stats` select, in the same order <see cref="SqlBuilder.ProcedureStatsWindowSql"/> lists them.</summary>
    public static readonly string[] RawColumns =
    [
        "server_name", "database_name", "schema_name", "object_name", "object_type", "collection_time",
        "sample_interval_seconds",
        "delta_execution_count", "delta_worker_time", "delta_elapsed_time",
        "delta_logical_reads", "delta_logical_writes", "delta_physical_reads", "delta_spills",
        "query_plan_digest", "plan_handle", "cached_time",
        "plan_xml_is_null", "has_plan_xml_bytes",
    ];

    private static readonly Dictionary<string, string> RawColumnTypes = new()
    {
        ["server_name"] = "VARCHAR",
        ["database_name"] = "VARCHAR",
        ["schema_name"] = "VARCHAR",
        ["object_name"] = "VARCHAR",
        ["object_type"] = "VARCHAR",
        ["collection_time"] = "TIMESTAMP",
        ["sample_interval_seconds"] = "INTEGER",
        ["delta_execution_count"] = "BIGINT",
        ["delta_worker_time"] = "BIGINT",
        ["delta_elapsed_time"] = "BIGINT",
        ["delta_logical_reads"] = "BIGINT",
        ["delta_logical_writes"] = "BIGINT",
        ["delta_physical_reads"] = "BIGINT",
        ["delta_spills"] = "BIGINT",
        ["query_plan_digest"] = "BLOB",
        ["plan_handle"] = "VARCHAR",
        ["cached_time"] = "TIMESTAMP",
        ["plan_xml_is_null"] = "BOOLEAN",
        ["has_plan_xml_bytes"] = "BOOLEAN",
    };

    internal static string DuckDbTypeFor(string column) => RawColumnTypes[column];

    /// <summary>
    /// Opens a fresh on-disk DuckDB file and streams the raw window from the store into a
    /// `procedure_stats_raw` table. Unlike <see cref="QueryStoreWindow"/>/<see
    /// cref="QueryStatsWindow"/>, this window runs no <see
    /// cref="QueryStoreWindow.AssertCCollationAsync"/> check: every downstream DuckDB query
    /// over `procedure_stats_raw` (the hourly rollup, the procedure plan keys, the
    /// oversized-plan count) either aggregates numeric/timestamp columns or GROUP BYs text
    /// columns by plain equality -- no `min()`/`max()`/sort over text, which is the only case
    /// where PostgreSQL's collation-dependent text order could disagree with DuckDB's byte
    /// order. `query_plan_digest` is a BLOB key, not text, so the C-collation trap the Query
    /// Store queries guard against does not apply here. Caller owns and must delete the
    /// returned temp file path.
    /// </summary>
    public static async Task<string> BuildRawWindowFileAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, ExportOptions options, CancellationToken ct)
    {
        var tempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-psw-{Guid.NewGuid():N}.duckdb");

        // The chunk guard is now a run-level pre-flight in ExportRunner.RunChunkGuardPreflightAsync, run before this
        // or any other core table's read.
        await using var pgConnection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);

        await using var command = new NpgsqlCommand(SqlBuilder.ProcedureStatsWindowSql, pgConnection);
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
            createCommand.CommandText = $"CREATE TABLE procedure_stats_raw ({columnDefs})";
            createCommand.ExecuteNonQuery();
        }

        await RawWindowAppender.AppendAllAsync(reader, duckConnection, "procedure_stats_raw",
            RawColumns.Select(DuckDbTypeFor).ToArray(), ct).ConfigureAwait(false);

        return tempDbPath;
    }

    public static void TryDeleteTempFile(string tempDbPath) => DuckDbSession.DeleteTempDatabase(tempDbPath);
}
