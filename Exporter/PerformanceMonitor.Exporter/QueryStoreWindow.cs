using System.Data;
using DuckDB.NET.Data;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Loads one export window's raw `query_store_stats` rows into an on-disk DuckDB temp file
/// and dedups them there (never in Postgres: the store is CPU-bound at peaks and must not run
/// a sort). Shared by the `query_store_stats` table export and the Query Store side of
/// `plan_shape` / `query_text`, all of which need the same deduped rows: the window table
/// writes them out almost as-is, and the two dimension tables derive their keys from them.
/// </summary>
public static class QueryStoreWindow
{
    /// <summary>Column names of the raw `collect.query_store_stats` select, in the same order `QueryStoreStatsRawSql` lists them.</summary>
    public static readonly string[] RawColumns =
    [
        "server_id", "server_name", "database_name", "query_id", "plan_id", "execution_type_desc",
        "first_execution_time", "last_execution_time", "module_name", "query_hash", "execution_count",
        "avg_duration_us", "min_duration_us", "max_duration_us", "avg_cpu_time_us", "min_cpu_time_us", "max_cpu_time_us",
        "avg_logical_io_reads", "min_logical_io_reads", "max_logical_io_reads",
        "avg_logical_io_writes", "min_logical_io_writes", "max_logical_io_writes",
        "avg_physical_io_reads", "min_physical_io_reads", "max_physical_io_reads",
        "avg_clr_time_us", "min_clr_time_us", "max_clr_time_us", "min_dop", "max_dop",
        "avg_query_max_used_memory", "min_query_max_used_memory", "max_query_max_used_memory",
        "avg_rowcount", "min_rowcount", "max_rowcount",
        "avg_num_physical_io_reads", "min_num_physical_io_reads", "max_num_physical_io_reads",
        "avg_log_bytes_used", "min_log_bytes_used", "max_log_bytes_used",
        "avg_tempdb_space_used", "min_tempdb_space_used", "max_tempdb_space_used",
        "plan_type", "plan_forcing_type", "is_forced_plan", "force_failure_count", "last_force_failure_reason",
        "compatibility_level", "query_plan_hash", "replica_role", "runtime_stats_interval_id",
        "interval_start_time_utc", "collection_time",
    ];

    private static readonly Dictionary<string, string> RawColumnTypes = new()
    {
        ["server_id"] = "INTEGER",
        ["server_name"] = "VARCHAR",
        ["database_name"] = "VARCHAR",
        ["query_id"] = "BIGINT",
        ["plan_id"] = "BIGINT",
        ["execution_type_desc"] = "VARCHAR",
        ["first_execution_time"] = "TIMESTAMP",
        ["last_execution_time"] = "TIMESTAMP",
        ["module_name"] = "VARCHAR",
        ["query_hash"] = "VARCHAR",
        ["execution_count"] = "BIGINT",
        ["plan_type"] = "VARCHAR",
        ["plan_forcing_type"] = "VARCHAR",
        ["is_forced_plan"] = "BOOLEAN",
        ["force_failure_count"] = "BIGINT",
        ["last_force_failure_reason"] = "VARCHAR",
        ["compatibility_level"] = "INTEGER",
        ["query_plan_hash"] = "VARCHAR",
        ["replica_role"] = "VARCHAR",
        ["runtime_stats_interval_id"] = "BIGINT",
        ["interval_start_time_utc"] = "TIMESTAMP",
        ["collection_time"] = "TIMESTAMP",
    };

    internal static string DuckDbTypeFor(string column) =>
        RawColumnTypes.TryGetValue(column, out var t) ? t : "BIGINT"; // every other column above is an int8 counter

    /// <summary>
    /// Opens a fresh on-disk DuckDB file and streams the raw window from the store into a
    /// `query_store_raw` table -- undeduped. Runs <see cref="AssertCCollationAsync"/> first, on
    /// the same connection, before a single row is read: the DuckDB-side Query Store key
    /// queries (<see cref="SqlBuilder.QueryStorePlanShapeKeyDuckDbSql"/>, <see
    /// cref="SqlBuilder.QueryStoreTextKeyDuckDbSql"/>) both depend on the store's collation
    /// being C, and this is the one place every consumer of the raw window (query_store_stats,
    /// plan_shape, query_text) passes through, so checking here means every consumer is
    /// covered without repeating the check. Caller owns and must delete the returned temp file
    /// path.
    /// </summary>
    public static Task<string> BuildRawWindowFileAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, ExportOptions options, CancellationToken ct) =>
        BuildWindowFileAsync(connectionStringBuilder, options, null, () => DateTime.UtcNow, ct);

    /// <summary>
    /// <see cref="BuildRawWindowFileAsync"/> with the Query Store source choice (<see cref="ExportOptions.QueryStoreSource"/>).
    /// <c>raw</c> is the read above and nothing else: no extra store query, <paramref name="report"/> untouched. In
    /// <c>wide</c> mode the gate (<see cref="QueryStoreWideGate"/>) and the wide read run first, and any refusal or
    /// validation failure falls back to the raw read, with the reason in <paramref name="report"/>. <c>compare</c>
    /// reads both, compares them in DuckDB and keeps the raw rows in `query_store_raw`. Either way `query_store_raw`
    /// holds the rows every consumer works from; when they came from the wide table <see
    /// cref="QueryStoreSourceReport.Source"/> is `wide`. <paramref name="utcNow"/> is the clock the gate's 8-day rule uses.
    /// </summary>
    public static async Task<string> BuildWindowFileAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, ExportOptions options,
        QueryStoreSourceReport? report, Func<DateTime> utcNow, CancellationToken ct)
    {
        var tempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-qs-{Guid.NewGuid():N}.duckdb");
        try
        {
            await using var pgConnection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);

            await AssertCCollationAsync(pgConnection, ct).ConfigureAwait(false);

            // The chunk guard is now a run-level pre-flight in ExportRunner.RunChunkGuardPreflightAsync (checked to
            // `to + 2h`, the same late-snapshot margin the SQL below applies), run before this or any other core
            // table's read.
            using var duckConnection = DuckDbSession.Open(tempDbPath);

            var mode = report?.Mode ?? QueryStoreSourceMode.Raw;
            if (mode == QueryStoreSourceMode.Raw)
            {
                await LoadRawAsync(pgConnection, duckConnection, "query_store_raw", options, ct).ConfigureAwait(false);
                return tempDbPath;
            }

            var wideLoaded = await TryLoadWideAsync(pgConnection, duckConnection, options, report!, utcNow(), ct).ConfigureAwait(false);
            if (wideLoaded && mode == QueryStoreSourceMode.Wide)
            {
                using var rename = duckConnection.CreateCommand();
                rename.CommandText = "ALTER TABLE query_store_wide RENAME TO query_store_raw";
                rename.ExecuteNonQuery();
                report!.Source = "wide";
                return tempDbPath;
            }

            var rawStart = System.Diagnostics.Stopwatch.GetTimestamp();
            await LoadRawAsync(pgConnection, duckConnection, "query_store_raw", options, ct).ConfigureAwait(false);
            report!.RawReadMs = System.Diagnostics.Stopwatch.GetElapsedTime(rawStart).TotalMilliseconds;
            report.Source = "raw";

            if (wideLoaded)
            {
                QueryStoreCompare.Run(duckConnection, report, Console.Out);
            }

            return tempDbPath;
        }
        catch
        {
            TryDeleteTempFile(tempDbPath);
            throw;
        }
    }

    /// <summary>Streams the raw read (<see cref="SqlBuilder.QueryStoreStatsRawSql"/>) into a new DuckDB table.</summary>
    private static async Task LoadRawAsync(
        NpgsqlConnection pgConnection, DuckDBConnection duckConnection, string tableName, ExportOptions options, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(SqlBuilder.QueryStoreStatsRawSql, pgConnection);
        command.Parameters.Add(new NpgsqlParameter("from", NpgsqlDbType.Timestamp)
        {
            Value = DateTime.SpecifyKind(options.FromUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter("to", NpgsqlDbType.Timestamp)
        {
            Value = DateTime.SpecifyKind(options.ToUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter("to_plus_2h", NpgsqlDbType.Timestamp)
        {
            // Late snapshots of an interval that started just before --to can land up to 66
            // minutes after the interval start; widening by 2 hours past --to on
            // collection_time gives comfortable margin without scanning the whole store.
            Value = DateTime.SpecifyKind(options.QueryStoreCollectionToUtc, DateTimeKind.Unspecified),
        });

        await using var reader = await command.ExecuteReaderAsync(RawWindowAppender.Behavior, ct).ConfigureAwait(false);
        CreateWindowTable(duckConnection, tableName);
        await AppendAllAsync(reader, duckConnection, tableName, ct).ConfigureAwait(false);
    }

    private static void CreateWindowTable(DuckDBConnection duckConnection, string tableName)
    {
        var columnDefs = string.Join(", ", RawColumns.Select(c => $"\"{c}\" {DuckDbTypeFor(c)}"));
        using var createCommand = duckConnection.CreateCommand();
        createCommand.CommandText = $"CREATE TABLE {tableName} ({columnDefs})";
        createCommand.ExecuteNonQuery();
    }

    private static Task AppendAllAsync(NpgsqlDataReader reader, DuckDBConnection duckConnection, string tableName, CancellationToken ct) =>
        RawWindowAppender.AppendAllAsync(reader, duckConnection, tableName, RawColumns.Select(DuckDbTypeFor).ToArray(), ct);

    /// <summary>
    /// The wide side of <c>wide</c>/<c>compare</c>: gate, read, validate. On success `query_store_wide` holds the rows
    /// (server_name filled from the raw table's own names) and this returns true. On any refusal or failure it records the reason in
    /// <paramref name="report"/>, leaves no wide table behind and returns false, and the caller reads raw.
    /// </summary>
    private static async Task<bool> TryLoadWideAsync(
        NpgsqlConnection pgConnection, DuckDBConnection duckConnection, ExportOptions options,
        QueryStoreSourceReport report, DateTime nowUtc, CancellationToken ct)
    {
        var early = QueryStoreWideGate.PreCheck(options.FromUtc, options.ToUtc, nowUtc);
        if (early is not null)
        {
            RecordFallback(report, early.Reason!, early.ServerId);
            return false;
        }

        IReadOnlySet<int> checkedServers = new HashSet<int>();
        try
        {
            var tablesExist = await ReadScalarBoolAsync(pgConnection, SqlBuilder.QueryStoreWideTablesExistSql, ct).ConfigureAwait(false);
            var coverage = new List<WideCoverageRow>();
            var pending = new List<int>();
            if (tablesExist)
            {
                await using (var cmd = new NpgsqlCommand(SqlBuilder.QueryStoreWideCoverageSql, pgConnection))
                await using (var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false))
                {
                    while (await r.ReadAsync(ct).ConfigureAwait(false))
                    {
                        coverage.Add(new WideCoverageRow(r.GetInt32(0), r.GetDateTime(1), r.GetDateTime(2)));
                    }
                }

                await using (var cmd = new NpgsqlCommand(SqlBuilder.QueryStoreWidePendingSql, pgConnection))
                await using (var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false))
                {
                    while (await r.ReadAsync(ct).ConfigureAwait(false))
                    {
                        pending.Add(r.GetInt32(0));
                    }
                }
            }

            var decision = QueryStoreWideGate.Evaluate(options.FromUtc, options.ToUtc, nowUtc, tablesExist, coverage, pending);
            checkedServers = QueryStoreWideGate.ServersToCheck(coverage, options.FromUtc);
            if (!decision.UseWide)
            {
                RecordFallback(report, decision.Reason!, decision.ServerId, decision.Detail);
                return false;
            }
        }
        catch (NpgsqlException ex)
        {
            Console.Error.WriteLine($"Query Store wide gate failed ({ex.GetType().Name}: {ex.Message}); reading raw.");
            RecordFallback(report, QueryStoreFallbackReasons.GateError, null);
            return false;
        }

        var wideStart = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            await LoadWideStageAsync(pgConnection, duckConnection, options, ct).ConfigureAwait(false);
        }
        catch (NpgsqlException ex)
        {
            Console.Error.WriteLine($"Query Store wide read failed ({ex.GetType().Name}: {ex.Message}); reading raw.");
            DropTable(duckConnection, "query_store_wide_stage");
            RecordFallback(report, QueryStoreFallbackReasons.WideReadError, null);
            return false;
        }
        report.WideReadMs = System.Diagnostics.Stopwatch.GetElapsedTime(wideStart).TotalMilliseconds;

        string? validation;
        int? validationServerId = null;
        try
        {
            (validation, validationServerId) = ValidateWide(duckConnection, options, checkedServers);
            if (validation is null)
            {
                var names = await ReadServerNamesAsync(pgConnection, ServerIdsOfStage(duckConnection), options, ct).ConfigureAwait(false);
                validation = NameWide(duckConnection, names);
            }
        }
        catch (NpgsqlException ex)
        {
            Console.Error.WriteLine($"Query Store wide server-name read failed ({ex.GetType().Name}: {ex.Message}); reading raw.");
            DropTable(duckConnection, "query_store_wide_stage");
            DropTable(duckConnection, "query_store_wide");
            DropTable(duckConnection, "query_store_wide_names");
            RecordFallback(report, QueryStoreFallbackReasons.WideReadError, null);
            return false;
        }

        if (validation is not null)
        {
            DropTable(duckConnection, "query_store_wide_names");
            DropTable(duckConnection, "query_store_wide_stage");
            DropTable(duckConnection, "query_store_wide");
            RecordFallback(report, validation, validationServerId);
            return false;
        }

        return true;
    }

    private static void RecordFallback(QueryStoreSourceReport report, string reason, int? serverId, string? detail = null)
    {
        report.FallbackReason = reason;
        report.FallbackServerId = serverId;
        report.FallbackDetail = detail;
    }

    private static async Task<bool> ReadScalarBoolAsync(NpgsqlConnection pgConnection, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, pgConnection);
        var value = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return value is bool b && b;
    }

    /// <summary>
    /// Runs <see cref="SqlBuilder.QueryStoreStatsWideSql"/> in a read-only transaction with `enable_indexscan = off`
    /// (so the planner picks a bitmap heap scan) and streams it into `query_store_wide_stage`.
    /// </summary>
    private static async Task LoadWideStageAsync(
        NpgsqlConnection pgConnection, DuckDBConnection duckConnection, ExportOptions options, CancellationToken ct)
    {
        await using var transaction = await pgConnection.BeginTransactionAsync(ct).ConfigureAwait(false);
        await using (var setup = new NpgsqlCommand(SqlBuilder.QueryStoreWideReadSetupSql, pgConnection, transaction))
        {
            await setup.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using var command = new NpgsqlCommand(SqlBuilder.QueryStoreStatsWideSql, pgConnection, transaction);
        command.Parameters.Add(new NpgsqlParameter("from", NpgsqlDbType.Timestamp)
        {
            Value = DateTime.SpecifyKind(options.FromUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter("to", NpgsqlDbType.Timestamp)
        {
            Value = DateTime.SpecifyKind(options.ToUtc, DateTimeKind.Unspecified),
        });

        try
        {
            await using var reader = await command.ExecuteReaderAsync(RawWindowAppender.Behavior, ct).ConfigureAwait(false);
            CreateWindowTable(duckConnection, "query_store_wide_stage");
            await AppendAllAsync(reader, duckConnection, "query_store_wide_stage", ct).ConfigureAwait(false);
        }
        finally
        {
            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Validates `query_store_wide_stage` in DuckDB. Returns the fallback reason and server id (when one applies), or
    /// (null, null) when the rows hold:
    /// <list type="bullet">
    /// <item>`interval_misaligned`: every `interval_start_time_utc` must be in `[from, to)` and no `first_execution_time` may precede it.</item>
    /// <item>`no_coverage`: every server_id in the rows must be one the gate checked. The gate cannot see a server that has no
    /// coverage row (the registry is not read), so a wide row from such a server refuses the window here.</item>
    /// </list>
    /// </summary>
    private static (string? Reason, int? ServerId) ValidateWide(
        DuckDBConnection duckConnection, ExportOptions options, IReadOnlySet<int> checkedServers)
    {
        var from = options.FromUtc.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
        var to = options.ToUtc.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);

        using (var misaligned = duckConnection.CreateCommand())
        {
            misaligned.CommandText =
                "SELECT count(*) FROM query_store_wide_stage " +
                $"WHERE interval_start_time_utc IS NULL OR interval_start_time_utc < TIMESTAMP '{from}' OR interval_start_time_utc >= TIMESTAMP '{to}' " +
                "OR first_execution_time < interval_start_time_utc";
            if (Convert.ToInt64(misaligned.ExecuteScalar()) > 0)
            {
                return (QueryStoreFallbackReasons.IntervalMisaligned, null);
            }
        }

        foreach (var id in ServerIdsOfStage(duckConnection))
        {
            if (!checkedServers.Contains(id))
            {
                return (QueryStoreFallbackReasons.NoCoverage, id);
            }
        }

        return (null, null);
    }

    private static int[] ServerIdsOfStage(DuckDBConnection duckConnection)
    {
        var ids = new List<int>();
        using var cmd = duckConnection.CreateCommand();
        cmd.CommandText = "SELECT DISTINCT server_id FROM query_store_wide_stage ORDER BY server_id";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            ids.Add(reader.GetInt32(0));
        }

        return [.. ids];
    }

    /// <summary>
    /// `server_name` for each wide server_id, from the raw table `collect.query_store_stats` (the value the raw read
    /// returns): one index probe per id on `(server_id, collection_time)`. A server with no raw row in the window comes
    /// back NULL. Read-only transaction, no `enable_indexscan = off`: this statement wants the index. The registry
    /// (`config.config_monitored_servers`) is never read; the export login is denied it.
    /// </summary>
    private static async Task<Dictionary<int, string?>> ReadServerNamesAsync(
        NpgsqlConnection pgConnection, int[] serverIds, ExportOptions options, CancellationToken ct)
    {
        var names = new Dictionary<int, string?>();
        if (serverIds.Length == 0)
        {
            return names;
        }

        await using var transaction = await pgConnection.BeginTransactionAsync(ct).ConfigureAwait(false);
        try
        {
            await using (var setup = new NpgsqlCommand(SqlBuilder.QueryStoreWideNamesSetupSql, pgConnection, transaction))
            {
                await setup.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            await using var command = new NpgsqlCommand(SqlBuilder.QueryStoreWideNamesSql, pgConnection, transaction);
            command.Parameters.Add(new NpgsqlParameter("ids", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = serverIds });
            command.Parameters.Add(new NpgsqlParameter("from", NpgsqlDbType.Timestamp)
            {
                Value = DateTime.SpecifyKind(options.FromUtc, DateTimeKind.Unspecified),
            });
            command.Parameters.Add(new NpgsqlParameter("to_plus_2h", NpgsqlDbType.Timestamp)
            {
                Value = DateTime.SpecifyKind(options.QueryStoreCollectionToUtc, DateTimeKind.Unspecified),
            });

            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                names[reader.GetInt32(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);
            }
        }
        finally
        {
            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
        }

        return names;
    }

    /// <summary>
    /// Builds `query_store_wide` from `query_store_wide_stage` with `server_name` from <paramref name="names"/>. Returns
    /// `server_unmapped` when any server_id has no name (NULL), else null.
    /// </summary>
    private static string? NameWide(DuckDBConnection duckConnection, IReadOnlyDictionary<int, string?> names)
    {
        var ids = ServerIdsOfStage(duckConnection);
        if (ids.Any(id => !names.TryGetValue(id, out var name) || name is null))
        {
            return QueryStoreFallbackReasons.ServerUnmapped;
        }

        using (var create = duckConnection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE query_store_wide_names (server_id INTEGER, server_name VARCHAR)";
            create.ExecuteNonQuery();
        }

        using (var appender = duckConnection.CreateAppender("query_store_wide_names"))
        {
            foreach (var id in ids)
            {
                var row = appender.CreateRow();
                row.AppendValue(id);
                row.AppendValue(names[id]);
                row.EndRow();
            }
        }

        using (var build = duckConnection.CreateCommand())
        {
            build.CommandText =
                "CREATE TABLE query_store_wide AS SELECT s.* REPLACE (n.server_name AS server_name) " +
                "FROM query_store_wide_stage s JOIN query_store_wide_names n ON n.server_id = s.server_id";
            build.ExecuteNonQuery();
        }

        DropTable(duckConnection, "query_store_wide_stage");
        DropTable(duckConnection, "query_store_wide_names");
        return null;
    }

    private static void DropTable(DuckDBConnection duckConnection, string tableName)
    {
        using var cmd = duckConnection.CreateCommand();
        cmd.CommandText = $"DROP TABLE IF EXISTS {tableName}";
        cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// Builds the raw window file (<see cref="BuildRawWindowFileAsync"/>) and adds
    /// `query_store_dedup` on top of it: one row per (server_name, database_name, query_id,
    /// plan_id, runtime_stats_interval_id, execution_type_desc, replica_role), the greatest
    /// collection_time snapshot, renamed to last_collection_time, plus snapshot_count.
    /// Caller owns and must delete the returned temp file path. Kept for callers that only
    /// need the deduped grain and never touch the raw-window cache (currently none in
    /// production code -- see <see cref="QueryStoreRawWindowCache"/> -- but retained as the
    /// single place "raw then dedup" is composed, and exercised directly by tests).
    /// </summary>
    public static async Task<string> BuildDedupedTempFileAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder, ExportOptions options, CancellationToken ct)
    {
        var tempDbPath = await BuildRawWindowFileAsync(connectionStringBuilder, options, ct).ConfigureAwait(false);

        using var duckConnection = DuckDbSession.Open(tempDbPath);
        using var dedupCommand = duckConnection.CreateCommand();
        dedupCommand.CommandText =
            $"CREATE TABLE query_store_dedup AS {SqlBuilder.BuildQueryStoreDedupSql("query_store_raw")}";
        dedupCommand.ExecuteNonQuery();

        return tempDbPath;
    }

    public static void TryDeleteTempFile(string tempDbPath)
    {
        DuckDbSession.DeleteTempDatabase(tempDbPath);
    }

    /// <summary>
    /// fails loudly if the store's collation is not C/POSIX, or if `database_name` on `collect.query_store_stats`
    /// carries a column-level collation override. Both production stores are libc 'C' (decided fact; /// override): under that collation PostgreSQL text order equals DuckDB byte order, which is the precondition the
    /// DuckDB-side Query Store key queries (<see cref="SqlBuilder.QueryStorePlanShapeKeyDuckDbSql"/>, <see
    /// cref="SqlBuilder.QueryStoreTextKeyDuckDbSql"/>) depend on. Never falls back silently: a non-C store must fail
    /// plan_shape/query_text loudly, not silently mis-order examples.
    /// </summary>
    public static async Task AssertCCollationAsync(Npgsql.NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new Npgsql.NpgsqlCommand(SqlBuilder.DatabaseCollationCheckSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            throw new InvalidOperationException(
                "Query Store key derivation: could not read the store's collation (pg_database had no row for the current database).");
        }

        var datCollate = reader.IsDBNull(0) ? null : reader.GetString(0);
        var columnCollation = reader.IsDBNull(1) ? null : reader.GetString(1);

        if (!string.Equals(datCollate, "C", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Query Store key derivation requires the store's collation to be 'C' (byte order), so PostgreSQL " +
                $"text order matches DuckDB byte order. Found datcollate = '{datCollate}'. Refusing to compute " +
                "plan_shape/query_text Query Store keys under a different collation: fix the store's collation, " +
                "or escalate for a collation-aware key query.");
        }

        if (columnCollation is not null)
        {
            throw new InvalidOperationException(
                $"Query Store key derivation requires collect.query_store_stats.database_name to have no column-level " +
                $"collation override. Found collation '{columnCollation}'. Refusing to compute plan_shape/query_text " +
                "Query Store keys: fix the column's collation, or escalate for a collation-aware key query.");
        }
    }
}
